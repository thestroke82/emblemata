package it.gov.acn.emblemata.listener;

import it.gov.acn.emblemata.config.KafkaConfiguration;
import it.gov.acn.emblemata.integration.IntegrationManager;
import it.gov.acn.emblemata.integration.kafka.KafkaOutboxProcessor;
import it.gov.acn.emblemata.integration.kafka.KafkaOutboxStatistics;
import it.gov.acn.emblemata.locking.KafkaOutboxLockManager;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.model.event.BaseEvent;
import it.gov.acn.emblemata.service.KafkaOutboxService;
import lombok.RequiredArgsConstructor;
import net.javacrumbs.shedlock.core.SimpleLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.Optional;

@Component
@RequiredArgsConstructor
public class KafkaApplicationEventListener {

  private final Logger logger = LoggerFactory.getLogger(KafkaApplicationEventListener.class);
  private final KafkaOutboxService kafkaOutboxService;
  private final KafkaOutboxProcessor kafkaOutboxHandler;
  private final KafkaOutboxLockManager kafkaOutboxLockManager;
  private final IntegrationManager integrationManager;
  private final KafkaOutboxStatistics kafkaOutboxStatistics;

  @Autowired(required = false)
  private KafkaConfiguration kafkaConfig;


  @TransactionalEventListener(phase = TransactionPhase.BEFORE_COMMIT)
  public void on(BaseEvent<?> event){
    KafkaOutbox outbox = this.kafkaOutboxService.saveOutbox(event);
    this.kafkaOutboxStatistics.incrementQueued();
    if(this.integrationManager.isKafkaEnabled() && this.kafkaConfig.isInitialAttempt()){
      // best effort initial attempt. See configuration "initial-attempt"
      // if the lock is already acquired by some other thread, skip processing
      Optional<SimpleLock> lock = Optional.empty();
      try {
        lock = this.kafkaOutboxLockManager.lock();
        if(lock.isEmpty()){
          return;
        }
        this.kafkaOutboxHandler.processOutbox(outbox);
      } catch (Exception e) {
        logger.error("Error processing outbox: " + e.getMessage());
      } finally {
        lock.ifPresent(this.kafkaOutboxLockManager::release);
      }
    }
  }



}
