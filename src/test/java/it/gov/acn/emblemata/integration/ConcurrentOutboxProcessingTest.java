package it.gov.acn.emblemata.integration;

import it.gov.acn.emblemata.KafkaTestConfiguration;
import it.gov.acn.emblemata.PostgresTestContext;
import it.gov.acn.emblemata.TestUtil;
import it.gov.acn.emblemata.config.KafkaOutboxSchedulerConfiguration;
import it.gov.acn.emblemata.integration.kafka.KafkaClient;
import it.gov.acn.emblemata.integration.kafka.KafkaOutboxProcessor;
import it.gov.acn.emblemata.integration.kafka.KafkaOutboxStatistics;
import it.gov.acn.emblemata.listener.KafkaApplicationEventListener;
import it.gov.acn.emblemata.locking.KafkaOutboxLockManager;
import it.gov.acn.emblemata.model.event.ConstituencyCreatedEvent;
import it.gov.acn.emblemata.repository.ConstituencyRepository;
import it.gov.acn.emblemata.repository.KafkaOutboxRepository;
import it.gov.acn.emblemata.scheduling.KafkaOutboxScheduler;
import it.gov.acn.emblemata.service.KafkaOutboxService;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest(properties = {
        "spring.kafka.enabled=true",
        "spring.kafka.initial-attempt=true",
        "spring.kafka.outbox.scheduler.enabled=true",
        "spring.kafka.admin.fail-fast: true"
})
@ExtendWith(MockitoExtension.class)
@Import(KafkaTestConfiguration.class) // used for kafka integration with testcontainers
public class ConcurrentOutboxProcessingTest extends PostgresTestContext {

  @SpyBean
  private KafkaOutboxService kafkaOutboxService;

  @SpyBean
  private KafkaClient kafkaClient;
  @SpyBean
  private KafkaOutboxProcessor kafkaOutboxProcessor;

  @SpyBean
  private KafkaOutboxRepository kafkaOutboxRepository;

  @Autowired
  private KafkaApplicationEventListener kafkaApplicationEventListener;

  @Autowired
  private KafkaOutboxSchedulerConfiguration kafkaOutboxSchedulerConfiguration;

  @Autowired
  private KafkaOutboxStatistics kafkaOutboxStatistics;

  @Autowired
  private ConstituencyRepository constituencyRepository;
  @Autowired
  private KafkaOutboxLockManager kafkaOutboxLockManager;

  @Autowired
  private KafkaOutboxScheduler kafkaOutboxScheduler;



  @Test
  @Tag("clean")
  void when_concurrent_processOutbox_invocations_then_only_one_enters_critical_section()
          throws InterruptedException {

    ConstituencyCreatedEvent event1 = ConstituencyCreatedEvent.builder()
            .payload(TestUtil.createTelecom())
            .build();
    ConstituencyCreatedEvent event2 = ConstituencyCreatedEvent.builder()
            .payload(TestUtil.createTelecom())
            .build();

    AtomicInteger criticalSectionEntrances = new AtomicInteger();
    int workMs = 1000;

    // here we simulate the actual work by means of a Thread.sleep
    Mockito.doAnswer(invocation -> {
      criticalSectionEntrances.getAndIncrement();
      Thread.sleep(workMs);
      return null;
    }).when(kafkaOutboxProcessor).processOutbox(Mockito.any());

    // here we arbitrarily invoke 2 processOutbox in parallel

    CompletableFuture<String> cf1 = CompletableFuture.supplyAsync(() -> {
      kafkaApplicationEventListener.on(event1);
      return null;
    });
    CompletableFuture<String> cf2 = CompletableFuture.supplyAsync(() -> {
      kafkaApplicationEventListener.on(event2);
      return null;
    });

    Thread.sleep(workMs);

    Assertions.assertEquals(1, criticalSectionEntrances.get() );

    // wait for all the thread to complete e release the lock
    CompletableFuture.allOf(cf1, cf2).join();
  }

  @Test
  @Tag("clean")
  void when_concurrent_schedulers_then_only_one_enters_critical_section()
          throws InterruptedException {


    AtomicInteger criticalSectionEntrances = new AtomicInteger();
    int workMs = 1000;

    // here we simulate the actual work by means of a Thread.sleep, both for findOutstandingEvents and processOutbox
    Mockito.doAnswer(invocation -> {
      criticalSectionEntrances.getAndIncrement();
      Thread.sleep(workMs);
      return new ArrayList<>();
    }).when(kafkaOutboxRepository).findOutstandingEvents(Mockito.anyInt(), Mockito.any());

    // here we arbitrarily invoke 2 scheduler  in parallel

    CompletableFuture<String> cf1 = CompletableFuture.supplyAsync(() -> {
      kafkaOutboxScheduler.processKafkaOutbox();
      return null;
    });
    CompletableFuture<String> cf2 = CompletableFuture.supplyAsync(() -> {
      kafkaOutboxScheduler.processKafkaOutbox();
      return null;
    });

    Thread.sleep(workMs/2);

    Assertions.assertEquals(1, criticalSectionEntrances.get() );

    // wait for all the thread to complete e release the lock
    CompletableFuture.allOf(cf1, cf2).join();
  }

  @Test
  @Tag("clean")
  void when_concurrent_schedulers_and_processOutbox_then_only_one_enters_critical_section()
          throws InterruptedException {

    ConstituencyCreatedEvent event1 = ConstituencyCreatedEvent.builder()
            .payload(TestUtil.createTelecom())
            .build();

    AtomicInteger criticalSectionEntrances = new AtomicInteger();
    int workMs = 1000;

    // here we simulate the actual work by means of a Thread.sleep
    Mockito.doAnswer(invocation -> {
      criticalSectionEntrances.getAndIncrement();
      Thread.sleep(workMs);
      return new ArrayList<>();
    }).when(kafkaOutboxRepository).findOutstandingEvents(Mockito.anyInt(), Mockito.any());

    Mockito.doAnswer(invocation -> {
      criticalSectionEntrances.getAndIncrement();
      Thread.sleep(workMs);
      return null;
    }).when(kafkaOutboxProcessor).processOutbox(Mockito.any());

    // here we arbitrarily invoke 2 scheduler  in parallel
    CompletableFuture<String> cf1 = CompletableFuture.supplyAsync(() -> {
      kafkaApplicationEventListener.on(event1);
      return null;
    });
    CompletableFuture<String> cf2 = CompletableFuture.supplyAsync(() -> {
      kafkaOutboxScheduler.processKafkaOutbox();
      return null;
    });

    Thread.sleep(workMs/2);

    Assertions.assertEquals(1, criticalSectionEntrances.get() );

    // wait for all the thread to complete e release the lock
    CompletableFuture.allOf(cf1, cf2).join();
  }

  @BeforeEach
  @Transactional
  void clean(TestInfo info) {
    if(!info.getTags().contains("clean")) {
      return;
    }
    this.kafkaOutboxRepository.deleteAll();
    this.constituencyRepository.deleteAll();
    this.kafkaOutboxLockManager.releaseAllLocks();
  }

}
