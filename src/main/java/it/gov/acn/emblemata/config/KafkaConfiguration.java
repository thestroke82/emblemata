package it.gov.acn.emblemata.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
@ConditionalOnProperty( name = "spring.kafka.enabled", havingValue = "true" )
public class KafkaConfiguration {
  @Value("${spring.kafka.topic-constituency}")
  private String topicConstituency;
  @Value("${spring.kafka.initial-attempt}")
  private boolean initialAttempt;
}
