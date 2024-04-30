package it.gov.acn.emblemata.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
public class KafkaOutboxSchedulerConfiguration {

  @Value("${spring.kafka.outbox.scheduler.enabled:false}")
  private boolean enabled;
  @Value("${spring.kafka.outbox.scheduler.delayms:20000}")
  private long delayMs;
  @Value("${spring.kafka.outbox.scheduler.batch-size:20}")
  private int batchSize;
  @Value("${spring.kafka.outbox.scheduler.retention-days:7}")
  private int retentionDays;
}
