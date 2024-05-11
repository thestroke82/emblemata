package it.gov.acn.emblemata.unit;

import it.gov.acn.emblemata.config.KafkaOutboxSchedulerConfiguration;
import it.gov.acn.emblemata.integration.kafka.KafkaOutboxProcessor;
import it.gov.acn.emblemata.integration.kafka.KafkaOutboxStatistics;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.repository.KafkaOutboxRepository;
import it.gov.acn.emblemata.scheduling.ExponentialBackoffStrategy;
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

public class ExponentialBackoffStrategyTest {


  @Test
  public void given_some_outboxes_when_strategyExecute_test_algorithm_correctness() {
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

    ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy(backOffBase);
    List<KafkaOutbox> filteredOutboxList = strategy.execute(outboxList);

    Assertions.assertEquals(2, filteredOutboxList.size());
    Assertions.assertEquals("Nando1", filteredOutboxList.get(0).getEvent());
    Assertions.assertEquals("Nando7", filteredOutboxList.get(1).getEvent());
  }

  @Test
  public void fucking_break_it() {
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

    ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy(0);
    List<KafkaOutbox> filteredOutboxList = strategy.execute(outboxList);
    Assertions.assertEquals(10, filteredOutboxList.size());

    strategy = new ExponentialBackoffStrategy(-1);
    filteredOutboxList = strategy.execute(outboxList);
    Assertions.assertEquals(10, filteredOutboxList.size());

    strategy = new ExponentialBackoffStrategy(1);
    filteredOutboxList = strategy.execute(outboxList);
    Assertions.assertEquals(0, filteredOutboxList.size());

    filteredOutboxList = strategy.execute(null);
    Assertions.assertEquals(null, filteredOutboxList);
  }

}
