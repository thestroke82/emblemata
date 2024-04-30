package it.gov.acn.emblemata.listener;

import it.gov.acn.emblemata.config.KafkaConfiguration;
import it.gov.acn.emblemata.integration.IntegrationManager;
import it.gov.acn.emblemata.integration.kafka.KafkaOutboxProcessor;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.model.event.BaseEvent;
import it.gov.acn.emblemata.service.KafkaOutboxService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
@RequiredArgsConstructor
public class KafkaApplicationEventListener {
  private final KafkaOutboxService kafkaOutboxService;
  private final KafkaOutboxProcessor kafkaOutboxHandler;
  private final KafkaConfiguration kafkaConfig;
  private final IntegrationManager integrationManager;

  @TransactionalEventListener(phase = TransactionPhase.BEFORE_COMMIT)
  public void on(BaseEvent<?> event){
    KafkaOutbox outbox = this.kafkaOutboxService.saveOutbox(event);

    if(this.integrationManager.isKafkaEnabled() && this.kafkaConfig.isInitialAttempt()){
      // send the event to the outbox handler to be sent to the kafka topic
      this.kafkaOutboxHandler.processOutbox(outbox);
    }
  }



}
