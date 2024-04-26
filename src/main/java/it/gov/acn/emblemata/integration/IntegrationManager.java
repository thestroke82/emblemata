package it.gov.acn.emblemata.integration;

import it.gov.acn.emblemata.integration.kafka.KafkaConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class IntegrationManager {
  @Autowired(required = false)
  private KafkaConfig kafkaConfig;

  public boolean isKafkaEnabled(){
    return this.kafkaConfig!=null;
  }
}
