package it.gov.acn.emblemata.scheduling.adapter.in;


import it.gov.acn.emblemata.config.KafkaOutboxSchedulerConfiguration;
import it.gov.acn.emblemata.scheduling.domain.port.in.ProcessKafkaOutboxUseCase;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;

@Component
@RequiredArgsConstructor
public class KafkaOutboxScheduler {
    private final Logger logger = LoggerFactory.getLogger(KafkaOutboxScheduler.class);

    private final ProcessKafkaOutboxUseCase processKafkaOutboxUseCase;

    private final KafkaOutboxSchedulerConfiguration configuration;


    @Bean
    public ScheduledFuture<?> myScheduledTask(TaskScheduler scheduler) {
        if(!this.configuration.isEnabled()){
            logger.info("Kafka outbox scheduler is disabled");
            return null;
        }
        logger.info("Kafka outbox scheduler is enabled");
        Runnable task = this.processKafkaOutboxUseCase::processKafkaOutbox;
        Duration delay = Duration.ofMillis(configuration.getDelayMs());
        return scheduler.scheduleWithFixedDelay(task, delay);
    }


}
