package it.gov.acn.emblemata.service;

import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.model.event.BaseEvent;
import it.gov.acn.emblemata.repository.KafkaOutboxRepository;
import it.gov.acn.emblemata.util.Commons;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
@RequiredArgsConstructor
public class KafkaOutboxService {
  private final Logger logger = LoggerFactory.getLogger(KafkaOutboxService.class);

  private final KafkaOutboxRepository eventOutboxRepository;

  public KafkaOutbox saveOutbox(BaseEvent<?> event) {
    String eventJson = Commons.gson.toJson(event);
    String eventType = event.getClass().getName();
    logger.info("Registering event "+eventType+": {}", eventJson);
    KafkaOutbox eventOutbox = KafkaOutbox.builder()
        .eventClass(eventType)
        .event(eventJson)
        .publishDate(Instant.now())
        .build();
    return this.eventOutboxRepository.save(eventOutbox);
  }

  public void succesfulAttempt(UUID kafkaOutboxId) {
    Optional<KafkaOutbox> savedOutbox = this.eventOutboxRepository.findById(kafkaOutboxId);
    if(savedOutbox.isEmpty()) {
      logger.error("Outbox with id {} not found", kafkaOutboxId);
      return;
    }
    Instant now = Instant.now();
    savedOutbox.get().setLastAttemptDate(now);
    savedOutbox.get().setTotalAttempts(savedOutbox.get().getTotalAttempts() + 1);
    savedOutbox.get().setCompletionDate(now);
    this.eventOutboxRepository.save(savedOutbox.get());
  }

  public void unsuccesfulAttempt(UUID kafkaOutboxId, String errorMessage) {
    Optional<KafkaOutbox> savedOutbox = this.eventOutboxRepository.findById(kafkaOutboxId);
    if(savedOutbox.isEmpty()) {
      logger.error("Outbox with id {} not found", kafkaOutboxId);
      return;
    }
    Instant now = Instant.now();
    savedOutbox.get().setLastAttemptDate(now);
    savedOutbox.get().setTotalAttempts(savedOutbox.get().getTotalAttempts() + 1);
    savedOutbox.get().setLastError(errorMessage);
    this.eventOutboxRepository.save(savedOutbox.get());
  }
}
