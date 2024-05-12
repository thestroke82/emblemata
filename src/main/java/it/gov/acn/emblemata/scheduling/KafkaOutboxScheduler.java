package it.gov.acn.emblemata.scheduling;


import it.gov.acn.emblemata.config.KafkaOutboxSchedulerConfiguration;
import it.gov.acn.emblemata.integration.IntegrationManager;
import it.gov.acn.emblemata.integration.kafka.KafkaOutboxProcessor;
import it.gov.acn.emblemata.integration.kafka.KafkaOutboxStatistics;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.repository.KafkaOutboxRepository;

import java.util.List;

import jakarta.annotation.PostConstruct;
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
@ConditionalOnProperty(
        name = {"spring.kafka.outbox.scheduler.enabled","spring.kafka.enabled"},
        havingValue = "true"
)
public class KafkaOutboxScheduler {
    private final Logger logger = LoggerFactory.getLogger(KafkaOutboxScheduler.class);

    private final KafkaOutboxSchedulerConfiguration configuration;
    private final KafkaOutboxRepository kafkaOutboxRepository;
    private final KafkaOutboxProcessor kafkaOutboxProcessor;

    private OutboxItemSelectionStrategy outboxItemSelectionStrategy;

    @PostConstruct
    public void init(){
        // here one should look at the configuration to decide which strategy to use
        // for now only the exponential backoff strategy as been implemented
        this.outboxItemSelectionStrategy = new ExponentialBackoffStrategy(this.configuration.getBackoffBase());
    }

    @Transactional
    @Scheduled(fixedDelayString = "${spring.kafka.outbox.scheduler.delayms}")
    @SchedulerLock(name = "kafkaOutbox")
    public void processKafkaOutbox() {

        // oldest publications first
        Sort sort = Sort.by("publishDate").ascending();

        // we have to take into account the initial attempt
        int maxAttempts  = this.configuration.getMaxAttempts()+(this.configuration.isInitialAttempt()?1:0);

        // load all outstanding items on a simple basis: when they have no completion date and are below the max attempts
        List<KafkaOutbox> outstandingItems = this.kafkaOutboxRepository.findOutstandingItems(maxAttempts, sort);

        // select the outbox items to process in a more fine grained way, i.e. exponential backoff
        // in fact, exp backoff is the only strategy implemented so far, but others could be added
        outstandingItems = this.outboxItemSelectionStrategy.execute(outstandingItems);

        if(outstandingItems.isEmpty()){
            logger.trace("No Kafka outbox items to process. See you later!");
            return;
        }

        logger.trace("Kafka outbox scheduler processing {} items", outstandingItems.size());
        outstandingItems.forEach(this.kafkaOutboxProcessor::processOutbox);
    }


}
