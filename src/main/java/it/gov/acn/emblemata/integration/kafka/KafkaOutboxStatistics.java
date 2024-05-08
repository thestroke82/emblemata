package it.gov.acn.emblemata.integration.kafka;

import it.gov.acn.emblemata.config.KafkaOutboxSchedulerConfiguration;
import it.gov.acn.emblemata.model.KafkaOutbox;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import lombok.Getter;
import lombok.ToString;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class KafkaOutboxStatistics {
  private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy hh:mm:ss")
      .withZone(ZoneId.systemDefault());

  @Autowired
  private KafkaOutboxSchedulerConfiguration kafkaOutboxSchedulerConfiguration;

  private Instant observationStart;
  @Getter
  private long queued;
  @Getter
  private long succeeded;
  private List<KafkaOutbox> dlq;

  private KafkaOutboxStatistics() {
    this.reset();
  }

  public synchronized void reset() {
    this.queued = 0;
    this.succeeded = 0;
    this.dlq = new ArrayList<>();
    this.observationStart = Instant.now();
  }

  public synchronized void incrementQueued() {
    this.queued++;
  }

  public synchronized void registerSuccessfulAttempt(KafkaOutbox outbox) {
    this.succeeded++;
  }
  public synchronized void registerUnsuccessfulAttempt(KafkaOutbox outbox) {
    if(outbox!=null
        && outbox.getTotalAttempts() >= this.kafkaOutboxSchedulerConfiguration.getMaxAttempts()
        && dlq.stream().noneMatch(o -> o.getId().equals(outbox.getId()))) {
      this.dlq.add(outbox);
    }
  }

  @Override
  public String toString() {
    return "KafkaOutboxStatistics {" +
        "observationStart: " + this.dateTimeFormatter.format(this.observationStart) +
        ", queued: " + queued +
        ", succeeded: " + succeeded +
        ", dlq: " + dlq.size() +
        '}';
  }
}
