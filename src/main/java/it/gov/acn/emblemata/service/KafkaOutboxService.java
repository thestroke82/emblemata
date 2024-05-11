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
    logger.trace("Registering event "+eventType+": {}", eventJson);
    KafkaOutbox eventOutbox = KafkaOutbox.builder()
        .eventClass(eventType)
        .event(eventJson)
        .publishDate(Instant.now())
        .build();
    return this.eventOutboxRepository.save(eventOutbox);
  }

  public Optional<KafkaOutbox> succesfulAttempt(UUID kafkaOutboxId) {
    Optional<KafkaOutbox> savedOutboxOptional = this.eventOutboxRepository.findById(kafkaOutboxId);
    if(savedOutboxOptional.isEmpty()) {
      logger.error("Outbox with id {} not found", kafkaOutboxId);
      return savedOutboxOptional;
    }
    KafkaOutbox savedOutbox = savedOutboxOptional.get();
    Instant now = Instant.now();
    savedOutbox.setLastAttemptDate(now);
    savedOutbox.setTotalAttempts(savedOutbox.getTotalAttempts() + 1);
    savedOutbox.setCompletionDate(now);
    return Optional.of(this.eventOutboxRepository.save(savedOutbox));
  }

  public Optional<KafkaOutbox> unsuccesfulAttempt(UUID kafkaOutboxId, String errorMessage) {
    Optional<KafkaOutbox> savedOutboxOptional = this.eventOutboxRepository.findById(kafkaOutboxId);
    if(savedOutboxOptional.isEmpty()) {
      logger.error("Outbox with id {} not found", kafkaOutboxId);
      return savedOutboxOptional;
    }
    KafkaOutbox savedOutbox = savedOutboxOptional.get();
    Instant now = Instant.now();
    savedOutbox.setLastAttemptDate(now);
    savedOutbox.setTotalAttempts(savedOutbox.getTotalAttempts() + 1);
    savedOutbox.setLastError(errorMessage);
    return Optional.of(this.eventOutboxRepository.save(savedOutbox));
  }
}
