package it.gov.acn.emblemata.integration;

import it.gov.acn.emblemata.KafkaTestConfiguration;
import it.gov.acn.emblemata.PostgresTestContext;
import it.gov.acn.emblemata.TestUtil;
import it.gov.acn.emblemata.config.KafkaOutboxSchedulerConfiguration;
import it.gov.acn.emblemata.integration.kafka.KafkaClient;
import it.gov.acn.emblemata.integration.kafka.KafkaOutboxStatistics;
import it.gov.acn.emblemata.locking.KafkaOutboxLockManager;
import it.gov.acn.emblemata.model.Constituency;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.repository.ConstituencyRepository;
import it.gov.acn.emblemata.repository.KafkaOutboxRepository;
import it.gov.acn.emblemata.service.ConstituencyService;
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
import org.springframework.context.annotation.Import;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@SpringBootTest(properties = {
        "spring.kafka.enabled=true",
        "spring.kafka.initial-attempt=true",
        "spring.kafka.outbox.scheduler.enabled=true",
        "spring.kafka.outbox.scheduler.max-attempts=3",
        "spring.kafka.outbox.scheduler.delay-ms=5000",
        "spring.kafka.outbox.scheduler.backoff-base=2",
        "spring.kafka.outbox.statistics.log-interval-minutes=1"
})
@ExtendWith(MockitoExtension.class)
@Import(KafkaTestConfiguration.class) // used for kafka integration with testcontainers
public class KafkaOutboxSchedulerTests extends PostgresTestContext {


  @SpyBean
  private KafkaOutboxSchedulerConfiguration kafkaOutboxSchedulerConfiguration;


  @Autowired
  private ConstituencyService constituencyService;
  @Autowired
  private KafkaOutboxRepository kafkaOutboxRepository;

  @SpyBean
  private KafkaClient kafkaClient;

  @Autowired
  private KafkaOutboxStatistics  kafkaOutboxStatistics;
  @Autowired
  private ConstituencyRepository constituencyRepository;
  @Autowired
  private KafkaOutboxLockManager kafkaOutboxLockManager;

  @Test
  @Tag("clean")
  void when_saveConstituency_bestEffort_attempt_fails_then_scheduler_succeeds() throws InterruptedException {
    // This test takes approximately 2 minutes to run

    // Given: Create a new constituency
    Constituency enel = TestUtil.createEnel();

    // Mock the Kafka client to throw an exception when sending a message
    Mockito.doThrow(new RuntimeException("Test exception")).when(kafkaClient).send(Mockito.any());

    // When: Save the constituency, which triggers a Kafka message send attempt that fails
    this.constituencyService.saveConstituency(enel);

    // Then: Verify that the Kafka client's send method was called once
    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() ->
            Mockito.verify(kafkaClient, Mockito.times(1)).send(Mockito.any())
        );

    // Check the state of the Kafka outbox after the failed send attempt
    KafkaOutbox kafkaOutbox = this.kafkaOutboxRepository.findAll().iterator().next();
    Assertions.assertNull(
            kafkaOutbox.getCompletionDate());
    Assertions.assertEquals(1, kafkaOutbox.getTotalAttempts());
    Assertions.assertNotNull(kafkaOutbox.getLastError());

    // Mock the Kafka client to succeed when sending a message
    Mockito.doReturn(CompletableFuture.completedFuture(null))
            .when(kafkaClient).send(Mockito.any());

    // Wait for the scheduler to retry the message send
    Awaitility
        .await()
        .atMost(Duration.ofMillis(
            500+this.kafkaOutboxSchedulerConfiguration.getDelayMs()+this.calculateBackoff(kafkaOutbox).toMillis()
            )
        )
        .untilAsserted(() ->
            Mockito.verify(kafkaClient, Mockito.times(2)).send(Mockito.any())
        );


    // Check the state of the Kafka outbox after the successful send attempt
    kafkaOutbox = this.kafkaOutboxRepository.findAll().iterator().next();
    Assertions.assertNotNull(
            kafkaOutbox.getCompletionDate());
    Assertions.assertEquals(2, kafkaOutbox.getTotalAttempts());
    Assertions.assertNotNull(kafkaOutbox.getLastError());
  }

  @Test
  @Tag("clean")
  void given_initialAttempt_disabled_when_saveConstituency_then_scheduler_succeeds_on_third_attempt() throws InterruptedException {
    // This test takes approximately 6 minutes to run

    // Given: Create a new constituency
    Constituency enel = TestUtil.createEnel();

    // Mock the Kafka client to throw an exception when sending a message
    Mockito.doThrow(new RuntimeException("Test exception 1")).when(kafkaClient).send(Mockito.any());

    // Disable the initial attempt configuration
    Mockito.when(kafkaOutboxSchedulerConfiguration.isInitialAttempt()).thenReturn(false);

    // When
    this.constituencyService.saveConstituency(enel);

    // Then: Wait for the scheduler to fail the first time
    Awaitility
        .await()
        .atMost(Duration.ofMillis(this.kafkaOutboxSchedulerConfiguration.getDelayMs()+100))
        .untilAsserted(() ->
            Mockito.verify(kafkaClient, Mockito.times(1)).send(Mockito.any())
        );

    // Check the state of the Kafka outbox after the first failed send attempt
    KafkaOutbox kafkaOutbox = this.kafkaOutboxRepository.findAll().iterator().next();
    Assertions.assertNull(
            kafkaOutbox.getCompletionDate());
    Assertions.assertEquals(1, kafkaOutbox.getTotalAttempts());
    Assertions.assertEquals("Test exception 1", kafkaOutbox.getLastError());

    // Mock the Kafka client to throw an exception when sending a message the second time
    Mockito.doThrow(new RuntimeException("Test exception 2")).when(kafkaClient).send(Mockito.any());

    // Wait for the scheduler to fail the second time
    Awaitility
        .await()
        .atMost(Duration.ofMillis(this.kafkaOutboxSchedulerConfiguration.getDelayMs()+this.calculateBackoff(kafkaOutbox).toMillis()))
        .untilAsserted(() ->
            Mockito.verify(kafkaClient, Mockito.times(2)).send(Mockito.any())
        );

    // Check the state of the Kafka outbox after the second failed send attempt
    kafkaOutbox = this.kafkaOutboxRepository.findAll().iterator().next();
    Assertions.assertNull(
            kafkaOutbox.getCompletionDate());
    Assertions.assertEquals(2, kafkaOutbox.getTotalAttempts());
    Assertions.assertEquals("Test exception 2", kafkaOutbox.getLastError());


    // Mock the Kafka client to succeed this time
    Mockito.doReturn(CompletableFuture.completedFuture(null))
            .when(kafkaClient).send(Mockito.any());

    // Wait for the scheduler to succeed the third time
    Awaitility
        .await()
        .atMost(Duration.ofMillis(this.kafkaOutboxSchedulerConfiguration.getDelayMs()+this.calculateBackoff(kafkaOutbox).toMillis()))
        .untilAsserted(() ->
            Mockito.verify(kafkaClient, Mockito.times(3)).send(Mockito.any())
        );

    // Check the state of the Kafka outbox after the successful send attempt
    kafkaOutbox = this.kafkaOutboxRepository.findAll().iterator().next();
    Assertions.assertNotNull(
            kafkaOutbox.getCompletionDate());
    Assertions.assertEquals(3, kafkaOutbox.getTotalAttempts());
    Assertions.assertEquals("Test exception 2", kafkaOutbox.getLastError());

    // Verify the statistics of the Kafka outbox
    Assertions.assertEquals(1, this.kafkaOutboxStatistics.getSucceeded());
    Assertions.assertEquals(1, this.kafkaOutboxStatistics.getQueued());
  }

  private Duration calculateBackoff(KafkaOutbox kafkaOutbox) {
    return Duration.ofMinutes((long)
            Math.pow(this.kafkaOutboxSchedulerConfiguration.getBackoffBase(), kafkaOutbox.getTotalAttempts()));
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
    this.kafkaOutboxStatistics.reset();
  }

}
