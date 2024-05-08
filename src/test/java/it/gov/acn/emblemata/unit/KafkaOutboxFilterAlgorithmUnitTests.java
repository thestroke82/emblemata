package it.gov.acn.emblemata.unit;

import it.gov.acn.emblemata.config.KafkaOutboxSchedulerConfiguration;
import it.gov.acn.emblemata.integration.kafka.KafkaOutboxProcessor;
import it.gov.acn.emblemata.integration.kafka.KafkaOutboxStatistics;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.repository.KafkaOutboxRepository;
import it.gov.acn.emblemata.scheduling.KafkaOutboxScheduler;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class KafkaOutboxFilterAlgorithmUnitTests {

  @Mock
  private KafkaOutboxSchedulerConfiguration configuration;
  @Mock
  private KafkaOutboxRepository kafkaOutboxRepository;
  @Mock
  private KafkaOutboxProcessor kafkaOutboxProcessor;
  @Mock
  private KafkaOutboxStatistics kafkaOutboxStatistics;

  private final KafkaOutboxScheduler outboxScheduler = new KafkaOutboxScheduler(configuration, kafkaOutboxRepository, kafkaOutboxProcessor, kafkaOutboxStatistics);

  @Test
  public void given_some_outboxes_when_filterOutboxItems_test_algorithm_correctness() {
    List<KafkaOutbox> outboxList = new ArrayList<>();
    Instant now = Instant.now();
    for(int i=0; i<10; i++){
      outboxList.add(KafkaOutbox.builder()
          .id(UUID.randomUUID())
          .event("Nando"+i)
          .lastAttemptDate(now)
          .totalAttempts(1)
          .build()
      );
    }
    int backOffBase = 2;

    // this one should be retrieved
    KafkaOutbox outbox = outboxList.get(1);
    outbox.setLastAttemptDate(now.minus(backOffBase, ChronoUnit.MINUTES));
    outbox.setTotalAttempts(1);

    // this one should be retrieved
    outbox = outboxList.get(7);
    outbox.setLastAttemptDate(now.minus(backOffBase*backOffBase, ChronoUnit.MINUTES));
    outbox.setTotalAttempts(2);

    List<KafkaOutbox> filteredOutboxList = outboxScheduler.filterOutboxItems(outboxList, 3, backOffBase);

    Assertions.assertEquals(2, filteredOutboxList.size());
    Assertions.assertEquals("Nando1", filteredOutboxList.get(0).getEvent());
    Assertions.assertEquals("Nando7", filteredOutboxList.get(1).getEvent());
  }

  @Test
  public void when_filterOutboxItems_fucking_break_it() {
    List<KafkaOutbox> outboxList = new ArrayList<>();
    Instant now = Instant.now();
    for(int i=0; i<10; i++){
      outboxList.add(KafkaOutbox.builder()
          .id(UUID.randomUUID())
          .event("Nando"+i)
          .lastAttemptDate(now)
          .totalAttempts(1)
          .build()
      );
    }

    List<KafkaOutbox> filteredOutboxList = outboxScheduler.filterOutboxItems(outboxList, 3, 0);
    Assertions.assertEquals(10, filteredOutboxList.size());

    filteredOutboxList = outboxScheduler.filterOutboxItems(outboxList, 3, -1);
    Assertions.assertEquals(10, filteredOutboxList.size());

    filteredOutboxList = outboxScheduler.filterOutboxItems(outboxList, -1, 1);
    Assertions.assertEquals(0, filteredOutboxList.size());

    filteredOutboxList = outboxScheduler.filterOutboxItems(null, -1, -1);
    Assertions.assertNull(filteredOutboxList);

    filteredOutboxList = outboxScheduler.filterOutboxItems(List.of(), 3, 5);
    Assertions.assertEquals(0, filteredOutboxList.size());
  }

}
