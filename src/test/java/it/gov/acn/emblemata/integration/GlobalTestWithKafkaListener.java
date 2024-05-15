package it.gov.acn.emblemata.integration;

import it.gov.acn.emblemata.KafkaTestConfiguration;
import it.gov.acn.emblemata.PostgresTestContext;
import it.gov.acn.emblemata.TestUtil;
import it.gov.acn.emblemata.config.KafkaConfiguration;
import it.gov.acn.emblemata.model.Constituency;
import it.gov.acn.emblemata.model.event.ConstituencyCreatedEvent;
import it.gov.acn.emblemata.service.ConstituencyService;
import it.gov.acn.emblemata.util.Commons;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;

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
public class GlobalTestWithKafkaListener extends PostgresTestContext {

  @Autowired
  private ConstituencyService constituencyService;

  @Autowired
  private KafkaConfiguration kafkaConfiguration;

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

  @KafkaListener(topics = "#{kafkaConfiguration.getTopicConstituency()}", containerFactory = "constituencyKafkaListenerContainerFactory")
  public void listen(ConstituencyCreatedEvent event) {
    this.receivedKafkaMessages.add(event);
  }

}
