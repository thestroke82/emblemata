package it.gov.acn.emblemata.scheduling;


import it.gov.acn.emblemata.config.KafkaOutboxSchedulerConfiguration;
import it.gov.acn.emblemata.integration.kafka.KafkaOutboxProcessor;
import it.gov.acn.emblemata.model.KafkaOutbox;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

import it.gov.acn.emblemata.repository.KafkaOutboxRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.data.domain.Page;
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
    private final KafkaOutboxRepository kafkaOutboxRepository;
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

        // oldest publications first
        Sort sort = Sort.by("publishDate").ascending();


        int maxAttempts  = this.configuration.getMaxAttempts()+1; // we have to
        List<KafkaOutbox> outstandingEvents = this.kafkaOutboxRepository.findOutstandingEvents(maxAttempts, sort );
        Instant now = Instant.now();

        outstandingEvents = outstandingEvents.stream().filter(oe->{
            if(oe.getTotalAttempts()==0 ||  oe.getLastAttemptDate()==null){
                return true;
            }

            // Filtering out events that have not yet completed the exponential backoff period
            Instant backoffProjection = oe.getLastAttemptDate()
                    .plus(Duration.ofMinutes((long) Math.pow(this.configuration.getBackoffBase(), oe.getTotalAttempts())));
            return backoffProjection.isBefore(now);
        }).collect(Collectors.toList());

        if(outstandingEvents.isEmpty()){
            logger.debug("No Kafka outbox items to process. See you later!");
            return;
        }

        logger.debug("Kafka outbox scheduler processing {} items", outstandingEvents.size());
        outstandingEvents.forEach(this.kafkaOutboxProcessor::processOutbox);
    }

}
