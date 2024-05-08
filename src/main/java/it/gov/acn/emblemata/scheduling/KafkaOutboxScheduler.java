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

        // load all outstanding events
        List<KafkaOutbox> outstandingEvents = this.kafkaOutboxRepository.findOutstandingEvents(maxAttempts, sort);

        outstandingEvents = this.filterOutboxItems(outstandingEvents,this.configuration.getMaxAttempts(),this.configuration.getBackoffBase());

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

    /**
      * Filter out outbox items that have not yet completed the exponential backoff period or have exceeded the maximum number of attempts
      * If an outbox item has not yet been attempted, it is considered for processing blindly
      * @param outstandingEvents list of outbox items to filter
      * @param maxAttempts maximum number of attempts
      * @param backoffBase base of the exponential backoff
     *                    @return filtered list of outbox items
     */
    // it's public for testing purposes
    public List<KafkaOutbox> filterOutboxItems(List<KafkaOutbox> outstandingEvents, int maxAttempts, int backoffBase) {
        if(outstandingEvents==null || outstandingEvents.isEmpty()){
            return outstandingEvents;
        }
        Instant now = Instant.now();
        return outstandingEvents.stream().filter(oe->{
            if(oe.getTotalAttempts()==0 ||  oe.getLastAttemptDate()==null){
                return true;
            }
            if(oe.getTotalAttempts()>maxAttempts){
                return false;
            }
            // accepting only outbox for which the current backoff period has expired
            // the backoff period is calculated as base^attempts
            Instant backoffProjection = oe.getLastAttemptDate()
                    .plus(Duration.ofMinutes((long) Math.pow(backoffBase, oe.getTotalAttempts())));

            // if the projection is before now, it's time to retry, i.e. the backoff period has expired
            return backoffProjection.isBefore(now);
        }).toList();
    }

}
