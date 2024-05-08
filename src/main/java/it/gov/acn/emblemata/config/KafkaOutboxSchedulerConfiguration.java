package it.gov.acn.emblemata.config;

import lombok.Getter;
import org.slf4j.event.Level;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
public class KafkaOutboxSchedulerConfiguration {
  @Value("${spring.kafka.initial-attempt:false}")
  private boolean initialAttempt;
  @Value("${spring.kafka.outbox.scheduler.enabled:false}")
  private boolean enabled;
  @Value("${spring.kafka.outbox.scheduler.delayms:20000}")
  private long delayMs;
  @Value("${spring.kafka.outbox.scheduler.max-attempts:3}")
  private int maxAttempts;
  @Value("${spring.kafka.outbox.scheduler.backoff-base:5}")
  private int backoffBase;

  @Value("${spring.kafka.outbox.scheduler.statistics.log:false}")
  private boolean logStatistics;

  @Value("${spring.kafka.outbox.scheduler.statistics.log-level:INFO}")
  private Level statisticsLogLevel;


}
