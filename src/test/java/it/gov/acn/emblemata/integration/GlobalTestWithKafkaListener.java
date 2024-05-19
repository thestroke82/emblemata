package it.gov.acn.emblemata.integration;

import it.gov.acn.emblemata.KafkaTestConfiguration;
import it.gov.acn.emblemata.PostgresTestContext;
import it.gov.acn.emblemata.TestUtil;
import it.gov.acn.emblemata.config.KafkaConfiguration;
import it.gov.acn.emblemata.config.KafkaOutboxSchedulerConfiguration;
import it.gov.acn.emblemata.integration.kafka.KafkaOutboxProcessor;
import it.gov.acn.emblemata.model.Constituency;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.model.event.ConstituencyCreatedEvent;
import it.gov.acn.emblemata.repository.KafkaOutboxRepository;
import it.gov.acn.emblemata.service.ConstituencyService;
import it.gov.acn.emblemata.util.Commons;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.testcontainers.containers.KafkaContainer;

@SpringBootTest(properties = {
    "spring.kafka.enabled=true",
    "spring.kafka.initial-attempt=true",
    "spring.kafka.outbox.scheduler.enabled=true",
    "spring.kafka.outbox.scheduler.max-attempts=3",
    "spring.kafka.outbox.scheduler.delay-ms=5000",
    "spring.kafka.outbox.scheduler.backoff-base=1",
    "spring.kafka.outbox.statistics.log-interval-minutes=1"
})
@ExtendWith(MockitoExtension.class)
@Import(KafkaTestConfiguration.class) // used for kafka integration with testcontainers
public class GlobalTestWithKafkaListener extends PostgresTestContext {

  @Autowired
  private ConstituencyService constituencyService;

  @Autowired
  private KafkaContainer kafkaContainer;

  @Autowired
  private KafkaOutboxSchedulerConfiguration kafkaOutboxSchedulerConfiguration;

  @Autowired
  private KafkaOutboxRepository kafkaOutboxRepository;

  @SpyBean
  private KafkaOutboxProcessor  kafkaOutboxProcessor;

  private List<ConstituencyCreatedEvent> receivedKafkaMessages = new ArrayList<>();

  @BeforeEach
  public void beforeEach() {
    receivedKafkaMessages.clear();
  }

  @Test
  public void constituency_events_are_correctly_published_and_consumed() {

    // Given
    Constituency enel = TestUtil.createEnel();

    // When
    this.constituencyService.saveConstituency(enel);

    // Then
    Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
        Assertions.assertEquals(1, receivedKafkaMessages.size())
    );

    ConstituencyCreatedEvent event = receivedKafkaMessages.get(0);
    Assertions.assertEquals(enel.getId(), event.getPayload().getId());

    System.out.println(Commons.gson.toJson(event));
  }

  // simulate a real scenario in which the kafka broker will go down and up again. In the meantime some events are produced
  // and i want to assert that they are correctly delivered when the broker is up again. The test will make use of
  // the kafka container of testcontainers to simulate the broker going down and up again
    @Test
    public void kafka_broker_down_and_up_again() {
        // Given
        Constituency enel = TestUtil.createEnel();
        Constituency fastweb = TestUtil.createFastweb();

        System.out.println("Kafka broker is about to go down: "+this.kafkaContainer.getBootstrapServers());


        // broker goes down
        this.kafkaContainer.stop();


        // When
        this.constituencyService.saveConstituency(enel);
        this.constituencyService.saveConstituency(fastweb);
        // assert that the exceptions are caught  and the events get recorded in the outbox
        List<KafkaOutbox> allOutboxItems = StreamSupport.stream(this.kafkaOutboxRepository.findAll().spliterator(), false).toList();
        Assertions.assertEquals(2, allOutboxItems.stream().filter(k->k.getLastError()!=null).count());



        // Then
        // the initial attempt is done, the broker is down, so the events are not in kafka yet
        // now we wait for a scheduled retry to happen and fail, in other words we will wait some more time before
        // bringing the broker up again

        Mockito.reset(this.kafkaOutboxProcessor);
        Awaitility.await().atMost(this.kafkaOutboxSchedulerConfiguration.getBackoffBase()*2, TimeUnit.MINUTES).untilAsserted(() ->
                Mockito.verify(this.kafkaOutboxProcessor, Mockito.times(2)).processOutbox(Mockito.any())
        );

        // broker goes up again
        this.kafkaContainer.start();
        System.out.println("Kafka broker is up again: "+this.kafkaContainer.getBootstrapServers());

        // wait and verify that everything is delivered and received correctly
        Awaitility.await().atMost(this.kafkaOutboxSchedulerConfiguration.getBackoffBase()*2, TimeUnit.MINUTES)
                .pollDelay(5, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        Assertions.assertEquals(2,
                                StreamSupport.stream(this.kafkaOutboxRepository.findAll().spliterator(), false).filter(k-> k.getCompletionDate()!=null).count())
                );

        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                Assertions.assertEquals(2, receivedKafkaMessages.size())
        );

        ConstituencyCreatedEvent eventEnel = receivedKafkaMessages.get(0);
        Assertions.assertEquals(enel.getId(), eventEnel.getPayload().getId());

        ConstituencyCreatedEvent eventEni = receivedKafkaMessages.get(1);
        Assertions.assertEquals(fastweb.getId(), eventEni.getPayload().getId());
    }



  @KafkaListener(topics = "#{kafkaConfiguration.getTopicConstituency()}", containerFactory = "constituencyKafkaListenerContainerFactory")
  public void listen(ConstituencyCreatedEvent event) {
    this.receivedKafkaMessages.add(event);
  }

}
