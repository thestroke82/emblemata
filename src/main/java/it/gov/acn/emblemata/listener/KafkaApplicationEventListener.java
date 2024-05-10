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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
@RequiredArgsConstructor
public class KafkaApplicationEventListener {
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
      // if lock already acquired, skip processing
      var lock = this.kafkaOutboxLockManager.lock();
      if(lock.isEmpty()){
        return;
      }
      this.kafkaOutboxHandler.processOutbox(outbox);
      this.kafkaOutboxLockManager.release(lock.orElse(null));
    }
  }



}
