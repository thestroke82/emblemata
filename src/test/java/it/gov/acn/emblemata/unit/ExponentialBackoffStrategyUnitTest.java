package it.gov.acn.emblemata.unit;

import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.scheduling.ExponentialBackoffStrategy;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExponentialBackoffStrategyUnitTest {

  @Test
  public void backoff_strategy_returns_all_items_when_no_attempts_made() {
    // Create a list of KafkaOutbox items with no attempts made
    List<KafkaOutbox> outboxList = new ArrayList<>();
    for(int i=0; i<10; i++){
      outboxList.add(KafkaOutbox.builder()
          .id(UUID.randomUUID())
          .event("Event"+i)
          .totalAttempts(0)
          .build()
      );
    }

    // Apply the backoff strategy
    ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy(2);
    List<KafkaOutbox> filteredOutboxList = strategy.execute(outboxList);

    // Assert that all items are returned
    Assertions.assertEquals(10, filteredOutboxList.size());
  }

  @Test
  public void backoff_strategy_returns_no_items_when_all_attempts_in_progress() {
    // Create a list of KafkaOutbox items with all attempts in progress
    List<KafkaOutbox> outboxList = new ArrayList<>();
    Instant now = Instant.now();
    for(int i=0; i<10; i++){
      outboxList.add(KafkaOutbox.builder()
          .id(UUID.randomUUID())
          .event("Event"+i)
          .lastAttemptDate(now)
          .totalAttempts(1)
          .build()
      );
    }

    // Apply the backoff strategy
    ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy(2);
    List<KafkaOutbox> filteredOutboxList = strategy.execute(outboxList);

    // Assert that no items are returned
    Assertions.assertEquals(0, filteredOutboxList.size());
  }

  @Test
  public void backoff_strategy_returns_items_when_backoff_period_expired() {
    // Create a list of KafkaOutbox items with backoff period expired
    List<KafkaOutbox> outboxList = new ArrayList<>();
    Instant now = Instant.now();
    for(int i=0; i<10; i++){
      outboxList.add(KafkaOutbox.builder()
          .id(UUID.randomUUID())
          .event("Event"+i)
          .lastAttemptDate(now.minus(3, ChronoUnit.MINUTES))
          .totalAttempts(1)
          .build()
      );
    }

    // Apply the backoff strategy
    ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy(2);
    List<KafkaOutbox> filteredOutboxList = strategy.execute(outboxList);

    // Assert that all items are returned
    Assertions.assertEquals(10, filteredOutboxList.size());
  }

  @Test
  public void backoff_strategy_returns_empty_list_when_input_is_null() {
    // Apply the backoff strategy with null input
    ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy(2);
    List<KafkaOutbox> filteredOutboxList = strategy.execute(null);

    // Assert that the result is null
    Assertions.assertEquals(null, filteredOutboxList);
  }

  @Test
  public void given_some_outboxes_test_algorithm_correctness() {
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
    Assertions.assertNull(filteredOutboxList);
  }

}
