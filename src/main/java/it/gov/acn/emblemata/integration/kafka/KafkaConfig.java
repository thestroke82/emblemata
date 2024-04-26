package it.gov.acn.emblemata.integration.kafka;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
@ConditionalOnProperty( name = "spring.kafka.enabled", havingValue = "true" )
public class KafkaConfig {
  @Value("${spring.kafka.topic_constituency}")
  private String topicConstituency;
}
