package it.gov.acn.emblemata.scheduling;


import it.gov.acn.emblemata.config.KafkaOutboxSchedulerConfiguration;
import it.gov.acn.emblemata.integration.kafka.KafkaOutboxProcessor;
import it.gov.acn.emblemata.integration.kafka.KafkaOutboxStatistics;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.repository.KafkaOutboxRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.Sort;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(value = "spring.kafka.outbox.scheduler.enabled", havingValue = "true")
public class KafkaOutboxScheduler {
    private final Logger logger = LoggerFactory.getLogger(KafkaOutboxScheduler.class);

    private final KafkaOutboxSchedulerConfiguration configuration;
    private final KafkaOutboxRepository kafkaOutboxRepository;
    private final KafkaOutboxProcessor kafkaOutboxProcessor;
    private final KafkaOutboxStatistics kafkaOutboxStatistics;

    @Transactional
    @Scheduled(fixedDelayString = "${spring.kafka.outbox.scheduler.delayms}")
    @SchedulerLock(name = "kafkaOutbox")
    public void processKafkaOutbox() {

        // oldest publications first
        Sort sort = Sort.by("publishDate").ascending();

        // we have to take into account the initial attempt
        int maxAttempts  = this.configuration.getMaxAttempts()+(this.configuration.isInitialAttempt()?1:0);

        List<KafkaOutbox> outstandingEvents = this.kafkaOutboxRepository.findOutstandingEvents(maxAttempts, sort);

        Instant now = Instant.now();
        outstandingEvents = outstandingEvents.stream().filter(oe->{
            if(oe.getTotalAttempts()==0 ||  oe.getLastAttemptDate()==null){
                return true;
            }
            if(oe.getTotalAttempts()>maxAttempts){
                return false;
            }
            // Filtering out outbox items that have not yet completed the exponential backoff period
            Instant backoffProjection = oe.getLastAttemptDate()
                    .plus(Duration.ofMinutes((long) Math.pow(this.configuration.getBackoffBase(), oe.getTotalAttempts())));
            return backoffProjection.isBefore(now);
        }).toList();

        if(outstandingEvents.isEmpty()){
            logger.trace("No Kafka outbox items to process. See you later!");
            return;
        }

        logger.trace("Kafka outbox scheduler processing {} items", outstandingEvents.size());
        outstandingEvents.forEach(this.kafkaOutboxProcessor::processOutbox);
    }


    @Scheduled(fixedDelayString = "#{${spring.kafka.outbox.scheduler.statistics.log-interval-minutes:60} * 60 * 1000}")
    public void logStatistics(){
        if(!this.configuration.isLogStatistics()){
            return;
        }
        logger
            .atLevel(this.configuration.getStatisticsLogLevel())
            .log(this.kafkaOutboxStatistics.toString());
    }

}
