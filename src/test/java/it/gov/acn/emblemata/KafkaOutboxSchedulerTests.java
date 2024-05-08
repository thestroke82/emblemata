package it.gov.acn.emblemata;

import it.gov.acn.emblemata.config.KafkaOutboxSchedulerConfiguration;
import it.gov.acn.emblemata.integration.kafka.KafkaClient;
import it.gov.acn.emblemata.integration.kafka.KafkaOutboxStatistics;
import it.gov.acn.emblemata.model.Constituency;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.repository.KafkaOutboxRepository;
import it.gov.acn.emblemata.service.ConstituencyService;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.flywaydb.core.Flyway;
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

@SpringBootTest(properties = {
    "spring.kafka.initial-attempt=true",
    "spring.kafka.outbox.scheduler.enabled=true",
    "spring.kafka.outbox.scheduler.max-attempts=3",
    "spring.kafka.outbox.scheduler.delay-ms=5000",
    "spring.kafka.outbox.scheduler.backoff-base=2",
    "spring.kafka.outbox.statistics.log-interval-minutes=1"
})
@ExtendWith(MockitoExtension.class)
@TestInstance(Lifecycle.PER_CLASS)
public class KafkaOutboxSchedulerTests {

  @Autowired
  private EntityManagerFactory entityManagerFactory;

  @SpyBean
  private KafkaOutboxSchedulerConfiguration kafkaOutboxSchedulerConfiguration;


  @Autowired
  private ConstituencyService constituencyService;
  @Autowired
  private KafkaOutboxRepository kafkaOutboxRepository;

  @SpyBean
  private KafkaClient kafkaClient;
  @Autowired
  private Flyway flyway;

  @Autowired
  private KafkaOutboxStatistics  kafkaOutboxStatistics;



  @BeforeAll
  void flywayMigration(){
    this.flyway.migrate();
  }


  @Test
  @Tag("clean")
  void save_constituency_best_effort_attempt_success() throws InterruptedException {

    Constituency enel = TestUtil.createEnel();

    // I want the actual Kafka call to always succeed, since we are testing the scheduler
    Mockito.doReturn(CompletableFuture.completedFuture(
        null
    )).when(kafkaClient).send(Mockito.any());

    // base case: a new constituency is saved and an initial succesful attempt is made
    this.constituencyService.saveConstituency(enel);

    // wait for the scheduler to process the outbox and verify that's not called a second time
    Thread.sleep(200);

    Mockito.verify(kafkaClient, Mockito.times(1)).send(Mockito.any());

    KafkaOutbox kafkaOutbox = this.kafkaOutboxRepository.findAll().iterator().next();
    Assertions.assertNotNull(
        kafkaOutbox.getCompletionDate());
    Assertions.assertEquals(1, kafkaOutbox.getTotalAttempts());

  }

  @Test
  @Tag("clean")
  void save_constituency_best_effort_attempt_failure_then_scheduler_success() throws InterruptedException {

    Constituency enel = TestUtil.createEnel();

    // I want the actual Kafka call to fail the first time
    Mockito.doThrow(new RuntimeException("Test exception")).when(kafkaClient).send(Mockito.any());

    // base case: a new constituency is saved and an initial unsuccesful attempt is made
    this.constituencyService.saveConstituency(enel);

    // wait for the scheduler to process the outbox and verify that's not been called a second time
    Thread.sleep(200);

    Mockito.verify(kafkaClient, Mockito.times(1)).send(Mockito.any());

    KafkaOutbox kafkaOutbox = this.kafkaOutboxRepository.findAll().iterator().next();
    Assertions.assertNull(
        kafkaOutbox.getCompletionDate());
    Assertions.assertEquals(1, kafkaOutbox.getTotalAttempts());
    Assertions.assertNotNull(kafkaOutbox.getLastError());

    Mockito.doReturn(CompletableFuture.completedFuture(null))
        .when(kafkaClient).send(Mockito.any());

    // I have to wait the backoff period
    Thread.sleep(500+this.kafkaOutboxSchedulerConfiguration.getDelayMs()
        +this.calculateBackoff(kafkaOutbox).toMillis());

    Mockito.verify(kafkaClient, Mockito.times(2)).send(Mockito.any());

    kafkaOutbox = this.kafkaOutboxRepository.findAll().iterator().next();
    Assertions.assertNotNull(
        kafkaOutbox.getCompletionDate());
    Assertions.assertEquals(2, kafkaOutbox.getTotalAttempts());
    Assertions.assertNotNull(kafkaOutbox.getLastError());
  }

  @Test
  @Tag("clean")
  void save_constituency_success_third_attempt() throws InterruptedException {
    Constituency enel = TestUtil.createEnel();


    // the kafka send will fail twice
    Mockito.doThrow(new RuntimeException("Test exception 1")).when(kafkaClient).send(Mockito.any());
    // disable first best effort attempt
    Mockito.when(kafkaOutboxSchedulerConfiguration.isInitialAttempt()).thenReturn(false);

    this.constituencyService.saveConstituency(enel);

    // wait for the scheduler to fail the first time
    Thread.sleep(this.kafkaOutboxSchedulerConfiguration.getDelayMs()+100);

    Mockito.verify(kafkaClient, Mockito.times(1)).send(Mockito.any());
    KafkaOutbox kafkaOutbox = this.kafkaOutboxRepository.findAll().iterator().next();
    Assertions.assertNull(
        kafkaOutbox.getCompletionDate());
    Assertions.assertEquals(1, kafkaOutbox.getTotalAttempts());
    Assertions.assertEquals("Test exception 1", kafkaOutbox.getLastError());

    Mockito.doThrow(new RuntimeException("Test exception 2")).when(kafkaClient).send(Mockito.any());

    // wait for the scheduler to fail the second time
    Thread.sleep(this.kafkaOutboxSchedulerConfiguration.getDelayMs()+this.calculateBackoff(kafkaOutbox).toMillis());
    kafkaOutbox = this.kafkaOutboxRepository.findAll().iterator().next();
    Assertions.assertNull(
        kafkaOutbox.getCompletionDate());
    Assertions.assertEquals(2, kafkaOutbox.getTotalAttempts());
    Assertions.assertEquals("Test exception 2", kafkaOutbox.getLastError());


    // next time it will succeed
    Mockito.doReturn(CompletableFuture.completedFuture(null))
        .when(kafkaClient).send(Mockito.any());

    // wait for the scheduler to succeed the third time
    Thread.sleep(this.kafkaOutboxSchedulerConfiguration.getDelayMs()+this.calculateBackoff(kafkaOutbox).toMillis());
    kafkaOutbox = this.kafkaOutboxRepository.findAll().iterator().next();
    Assertions.assertNotNull(
        kafkaOutbox.getCompletionDate());
    Assertions.assertEquals(3, kafkaOutbox.getTotalAttempts());
    Assertions.assertEquals("Test exception 2", kafkaOutbox.getLastError());

    Assertions.assertEquals(1, this.kafkaOutboxStatistics.getSucceeded());
    Assertions.assertEquals(1, this.kafkaOutboxStatistics.getQueued());
  }

  private Duration calculateBackoff(KafkaOutbox kafkaOutbox) {
    return Duration.ofMinutes((long)
        Math.pow(this.kafkaOutboxSchedulerConfiguration.getBackoffBase(), kafkaOutbox.getTotalAttempts()));
  }


  @BeforeEach
  void clean(TestInfo info) {
    if(!info.getTags().contains("clean")) {
      return;
    }
    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();
    em.createNativeQuery("truncate table constituency").executeUpdate();
    em.createNativeQuery("truncate table kafka_outbox").executeUpdate();
    em.getTransaction().commit();
  }

}
