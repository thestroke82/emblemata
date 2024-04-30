package it.gov.acn.emblemata.integration.kafka;

import it.gov.acn.emblemata.config.KafkaConfiguration;
import it.gov.acn.emblemata.integration.IntegrationManager;
import it.gov.acn.emblemata.model.event.BaseEvent;
import java.util.concurrent.CompletableFuture;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Component
@NoArgsConstructor
public class KafkaClient {
  private final Logger logger = LoggerFactory.getLogger(KafkaClient.class);
  @Autowired(required = false)
  private KafkaConfiguration config;

  @Autowired
  private IntegrationManager integrationManager;

  @Autowired
  private KafkaTemplate<String, BaseEvent<?>> kafkaTemplate;


  public CompletableFuture<SendResult<String, BaseEvent<?>>> send(BaseEvent<?> event){
    if(!this.integrationManager.isKafkaEnabled()){
      this.gracefullyFallBack(" Send event to Kafka ");
      return null;
    }
    return this.kafkaTemplate.send(this.config.getTopicConstituency(), event.getEventId(), event);
  }


  private void gracefullyFallBack(String ctx){
    logger.debug("Kafka is disabled. "+ctx+" just doing nothing...");
  }
}
