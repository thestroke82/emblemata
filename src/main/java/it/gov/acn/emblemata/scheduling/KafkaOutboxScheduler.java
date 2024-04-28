package it.gov.acn.emblemata.scheduling;


import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty( name = "spring.kafka.enabled", havingValue = "true" )
@RequiredArgsConstructor
public class KafkaOutboxScheduler {
    private final Logger logger = LoggerFactory.getLogger(KafkaOutboxScheduler.class);

    @Scheduled(fixedDelayString  = "${spring.kafka.outbox.scheduler.delayms}",
            initialDelayString = "${spring.kafka.outbox.scheduler.delayms}")
    public void schedule() {
        logger.info("Scheduled task");
    }

}
