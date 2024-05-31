package it.gov.acn.emblemata.integration;

import it.gov.acn.autoconfigure.outbox.config.DefaultConfiguration;
import it.gov.acn.autoconfigure.outbox.config.OutboxProperties;
import it.gov.acn.emblemata.KafkaTestConfiguration;
import it.gov.acn.emblemata.PostgresTestContext;
import it.gov.acn.emblemata.TestUtil;
import it.gov.acn.emblemata.model.Constituency;
import it.gov.acn.emblemata.model.event.ConstituencyCreatedEvent;
import it.gov.acn.emblemata.repository.ConstituencyRepository;
import it.gov.acn.emblemata.service.ConstituencyService;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
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
  private JdbcTemplate jdbcTemplate;

  @Autowired
  private OutboxProperties outboxProperties;
  @Autowired
  private ConstituencyService  constituencyService;

  @SpyBean
  private ConstituencyRepository constituencyRepository;

  @SpyBean
  private KafkaTemplate<?, ?> kafkaTemplate;


  @BeforeEach
  public void beforeEach() {
    receivedKafkaMessages.clear();
    constituencyRepository.deleteAll();
    jdbcTemplate.execute("TRUNCATE TABLE "+ DefaultConfiguration.TABLE_NAME);
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

  @Test
  public void given_kafka_template_exception_on_first_attempt_when_saveConstituency_then_succeeds_second_attempt() {
    // given
    Mockito.doThrow(new RuntimeException("Kafka template test exception"))
        .when(kafkaTemplate).send(Mockito.anyString(), Mockito.any());
    Constituency constituency = TestUtil.createEnel();

    // when
    this.constituencyService.saveConstituency(constituency);

    // then
    Awaitility.await()
        .atMost(Duration.ofMillis(outboxProperties.getFixedDelay()))
        .untilAsserted(()->
            Mockito.verify(kafkaTemplate, Mockito.times(1))
                .send(Mockito.anyString(),Mockito.any(), Mockito.any())
        );

    Assertions.assertEquals(0, receivedKafkaMessages.size());

    Mockito.reset(kafkaTemplate);

    Awaitility.await()
        .atMost(
            Duration.ofMillis(outboxProperties.getFixedDelay())
            .plus(Duration.ofMinutes(this.calculateBackoff(1,1)))
            .plus(Duration.ofMillis(outboxProperties.getFixedDelay()))
        )
        .until(() -> receivedKafkaMessages.size() == 1);

    ConstituencyCreatedEvent event = receivedKafkaMessages.get(0);
    Assertions.assertEquals(constituency.getId(), event.getPayload().getId());
  }

  @KafkaListener(topics = "#{kafkaConfiguration.getTopicConstituency()}",
      containerFactory = "constituencyKafkaListenerContainerFactory")
  public void listen(ConstituencyCreatedEvent event) {
    this.receivedKafkaMessages.add(event);
  }

  private int calculateBackoff(int attempts, int backoffBase) {
    return (int) Math.pow(backoffBase, attempts);
  }

}
