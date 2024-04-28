package it.gov.acn.emblemata.integration.kafka;

import it.gov.acn.emblemata.integration.IntegrationManager;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.model.event.BaseEvent;
import it.gov.acn.emblemata.service.KafkaOutboxService;
import it.gov.acn.emblemata.util.Commons;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaOutboxProcessor {
  private final Logger logger = LoggerFactory.getLogger(KafkaOutboxService.class);

  private final KafkaClient kafkaClient;
  private final KafkaOutboxService kafkaOutboxService;


  public void processOutbox(KafkaOutbox outbox){

    String errorMessage = null;
    try {
      BaseEvent<?> event = (BaseEvent<?>) Commons.gson.fromJson(outbox.getEvent(),  Class.forName(outbox.getEventClass()));
      this.kafkaClient.send(event).get();
    } catch (Exception e) {
      logger.error("Error processing outbox {}",outbox.getId());
      logger.error("Exception: ", e);
      errorMessage = e.getMessage();
    }

    if(errorMessage!= null){
      this.kafkaOutboxService.unsuccesfulAttempt(outbox.getId(), errorMessage);
    }else{
      this.kafkaOutboxService.succesfulAttempt(outbox.getId());
    }
  }
}
