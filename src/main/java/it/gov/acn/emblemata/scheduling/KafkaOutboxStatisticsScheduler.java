package it.gov.acn.emblemata.scheduling;

import it.gov.acn.emblemata.config.KafkaOutboxSchedulerConfiguration;
import it.gov.acn.emblemata.integration.IntegrationManager;
import it.gov.acn.emblemata.integration.kafka.KafkaOutboxStatistics;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "spring.kafka.outbox.scheduler.statistics.enabled", havingValue = "true")
public class KafkaOutboxStatisticsScheduler {
    private final Logger logger = LoggerFactory.getLogger(KafkaOutboxStatisticsScheduler.class);
    private final IntegrationManager integrationManager;
    private final KafkaOutboxStatistics kafkaOutboxStatistics;
    private final KafkaOutboxSchedulerConfiguration configuration;

    @Scheduled(fixedDelayString = "#{${spring.kafka.outbox.scheduler.statistics.log-interval-minutes:60} * 60 * 1000}")
    public void logStatistics(){
        if(!this.integrationManager.isKafkaEnabled()){
            logger
                .atLevel(this.configuration.getStatisticsLogLevel())
                .log("[Kafka is disabled]");
        }
        logger
                .atLevel(this.configuration.getStatisticsLogLevel())
                .log(this.kafkaOutboxStatistics.toString());
    }
}
