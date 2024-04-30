package it.gov.acn.emblemata.scheduling;


import it.gov.acn.emblemata.config.KafkaOutboxSchedulerConfiguration;
import it.gov.acn.emblemata.integration.kafka.KafkaOutboxProcessor;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.repository.KafkaOutboxRepositoryPaged;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ScheduledFuture;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
public class KafkaOutboxScheduler {
    private final Logger logger = LoggerFactory.getLogger(KafkaOutboxScheduler.class);

    private final KafkaOutboxSchedulerConfiguration configuration;
    private final KafkaOutboxRepositoryPaged kafkaOutboxRepositoryPaged;
    private final KafkaOutboxProcessor kafkaOutboxProcessor;

    @Bean
    public ScheduledFuture<?> myScheduledTask(TaskScheduler scheduler) {
        if(!this.configuration.isEnabled()){
            return null;
        }
        Runnable task = this::processKafkaOutbox;
        Duration delay = Duration.ofMillis(configuration.getDelayMs());
        return scheduler.scheduleWithFixedDelay(task, delay);
    }


    @Transactional
    public void processKafkaOutbox() {
        Instant retentionStart = Instant.now().minus(Duration.ofDays(this.configuration.getRetentionDays()));
        Pageable pageable = PageRequest.of(0, this.configuration.getBatchSize(), Sort.by("publishDate").ascending());
        Page<KafkaOutbox> batch = this.kafkaOutboxRepositoryPaged.findOutstandingEvents(retentionStart,  null, pageable);
        if(batch.isEmpty()){
            logger.info("No events to process");
            return;
        }
        batch.getContent().forEach(this.kafkaOutboxProcessor::processOutbox);
    }

}
