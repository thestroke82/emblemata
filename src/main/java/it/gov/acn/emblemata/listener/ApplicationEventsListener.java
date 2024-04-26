package it.gov.acn.emblemata.listener;

import it.gov.acn.emblemata.integration.kafka.KafkaEventHandler;
import it.gov.acn.emblemata.model.event.BaseEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
@RequiredArgsConstructor
public class ApplicationEventsListener {
  private final KafkaEventHandler kafkaEventHandler;

  @TransactionalEventListener(phase = TransactionPhase.BEFORE_COMMIT)
  public void on(BaseEvent<?> event){
    this.kafkaEventHandler.handleEvent(event);
  }

}
