package it.gov.acn.emblemata.integration.kafka;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import it.gov.acn.emblemata.integration.IntegrationManager;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.model.event.BaseEvent;
import it.gov.acn.emblemata.model.event.ConstituencyCreatedEvent;
import it.gov.acn.emblemata.service.KafkaOutboxService;
import it.gov.acn.emblemata.util.Commons;
import it.gov.acn.emblemata.util.InstantGsonTypeAdapter;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
public class KafkaEventHandler {
  private final Logger logger = LoggerFactory.getLogger(KafkaOutboxService.class);

  private final KafkaClient kafkaClient;
  private final KafkaOutboxService kafkaOutboxService;
  private final IntegrationManager integrationManager;


  @Transactional
  public void handleEvent(BaseEvent<?> event){
    KafkaOutbox outbox = this.kafkaOutboxService.registerEvent(event);
    if(!this.integrationManager.isKafkaEnabled()){
      return;
    }
     // first attempt is immediate
    this.processOutbox(outbox);
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  private void processOutbox(KafkaOutbox outbox){
    try{
      BaseEvent<?> event = (BaseEvent<?>) Commons.gson.fromJson(outbox.getEvent(),  Class.forName(outbox.getEventClass()));
      this.sendEventToKafka(event);
    }catch(Exception e){
      logger.error("[KafkaEventHandler.processOutbox] Error sending event to Kafka", e);
      this.kafkaOutboxService.unsuccesfulAttempt(outbox.getId(), e.getMessage());
      return;
    }
    this.kafkaOutboxService.succesfulAttempt(outbox.getId());
  }

  private void sendEventToKafka(BaseEvent<?> event) throws Exception {
    if(event instanceof ConstituencyCreatedEvent constituencyCreatedEvent){
      this.kafkaClient.sendConstituencyEvent(constituencyCreatedEvent).get();
    }else{
      throw new Exception("[KafkaEventHandler.sendEvent] Unsupported event type: "+event.getClass().getName());
    }
  }

}
