package it.gov.acn.emblemata.integration;

import it.gov.acn.autoconfigure.outbox.config.OutboxProperties;
import it.gov.acn.emblemata.KafkaTestConfiguration;
import it.gov.acn.emblemata.PostgresTestContext;
import it.gov.acn.emblemata.TestUtil;
import it.gov.acn.emblemata.model.Constituency;
import it.gov.acn.emblemata.model.event.ConstituencyCreatedEvent;
import it.gov.acn.emblemata.service.ConstituencyService;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.testcontainers.shaded.org.awaitility.Awaitility;

@SpringBootTest(properties = {
    "spring.kafka.enabled=true",
    "spring.kafka.initial-attempt=false",
    "acn.outbox.scheduler.enabled=true",
    "acn.outbox.scheduler.max-attempts=3",
    "acn.outbox.scheduler.fixed-delay=5000",
    "acn.outbox.scheduler.backoff-base=1",
})
@ExtendWith(MockitoExtension.class)
@Import(KafkaTestConfiguration.class) // used for kafka integration with testcontainers
public class GlobalTestKafkaListener extends PostgresTestContext {
  private final List<ConstituencyCreatedEvent> receivedKafkaMessages = new ArrayList<>();

  @Autowired
  private OutboxProperties outboxProperties;
  @Autowired
  private ConstituencyService  constituencyService;


  @BeforeEach
  public void beforeEach() {
    receivedKafkaMessages.clear();
  }

  @Test
  public void contextLoads() {
  }

  @Test
  public void given_normal_circumstances_when_saveConstituency_then_should_receive_kafka_message() {
    // given
    Constituency constituency = TestUtil.createEnel();

    // when
    this.constituencyService.saveConstituency(constituency);
    // then
    Awaitility.await()
        .atMost(Duration.ofMillis(outboxProperties.getFixedDelay()+1000))
        .until(() -> receivedKafkaMessages.size() == 1);

    ConstituencyCreatedEvent event = receivedKafkaMessages.get(0);
    Assertions.assertEquals(constituency.getId(), event.getPayload().getId());

  }

  @KafkaListener(topics = "#{kafkaConfiguration.getTopicConstituency()}",
      containerFactory = "constituencyKafkaListenerContainerFactory")
  public void listen(ConstituencyCreatedEvent event) {
    this.receivedKafkaMessages.add(event);
  }

}
