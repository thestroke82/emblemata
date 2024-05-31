package it.gov.acn.emblemata.listener;

import it.gov.acn.emblemata.config.KafkaConfiguration;
import it.gov.acn.emblemata.integration.IntegrationManager;
import it.gov.acn.emblemata.model.event.BaseEvent;
import it.gov.acn.emblemata.outbox.OutboxHandler;
import it.gov.acn.outbox.core.recorder.OutboxEventRecorder;
import it.gov.acn.outbox.model.LockingProvider;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
@RequiredArgsConstructor
public class KafkaApplicationEventListener {

  private final Logger logger = LoggerFactory.getLogger(KafkaApplicationEventListener.class);
  private final IntegrationManager integrationManager;
  private final LockingProvider outboxLockingProvider;
  private final OutboxHandler outboxHandler;
  // that's provided by acn-outbox-starter
  private final OutboxEventRecorder outboxEventRecorder;

  @Autowired(required = false)
  private KafkaConfiguration kafkaConfig;


  @TransactionalEventListener(phase = TransactionPhase.BEFORE_COMMIT)
  public void on(BaseEvent<?> event){
    this.outboxEventRecorder.recordEvent(event, event.getClass().getName());
    if(this.integrationManager.isKafkaEnabled() && this.kafkaConfig.isInitialAttempt()){
      // best effort initial attempt. See configuration "initial-attempt"
      // if the lock is already acquired by some other thread, skip processing
      Optional<Object> lock = Optional.empty();
      try {
        lock = this.outboxLockingProvider.lock();
        if(lock.isEmpty()){
          return;
        }
        this.outboxHandler.handle(event);
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        lock.ifPresent(this.outboxLockingProvider::release);
      }
    }
  }



}
