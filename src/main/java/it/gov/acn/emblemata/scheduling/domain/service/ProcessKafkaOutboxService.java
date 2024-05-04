package it.gov.acn.emblemata.scheduling.domain.service;

import it.gov.acn.emblemata.config.KafkaOutboxSchedulerConfiguration;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.scheduling.adapter.in.KafkaOutboxScheduler;
import it.gov.acn.emblemata.scheduling.domain.model.FindKafkaOutboxItemsQuery;
import it.gov.acn.emblemata.scheduling.domain.port.in.ProcessKafkaOutboxUseCase;
import it.gov.acn.emblemata.scheduling.domain.port.out.DoTheKafkaOutboxJobPort;
import it.gov.acn.emblemata.scheduling.domain.port.out.FindKafkaOutboxItemsPort;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;

@Component
@RequiredArgsConstructor
public class ProcessKafkaOutboxService implements ProcessKafkaOutboxUseCase {
    private final Logger logger = LoggerFactory.getLogger(KafkaOutboxScheduler.class);

    private final KafkaOutboxSchedulerConfiguration configuration;
    private final FindKafkaOutboxItemsPort findOutstandingOutboxItemsPort;
    private final DoTheKafkaOutboxJobPort doTheKafkaOutboxJobPort;


    @Transactional
    @Override
    public void processKafkaOutbox() {
        Instant retentionStart = Instant.now().minus(Duration.ofDays(this.configuration.getRetentionDays()));
        Pageable pageable = PageRequest.of(0, this.configuration.getBatchSize(), Sort.by("publishDate").ascending());
        FindKafkaOutboxItemsQuery query = FindKafkaOutboxItemsQuery.builder()
                .publishDateAfter(retentionStart)
                .pageable(pageable)
                .build();
        Page<KafkaOutbox> batch = this.findOutstandingOutboxItemsPort.findOutstandingOutboxItems(query);

        if(batch.isEmpty()){
            logger.info("No events to process");
            return;
        }
        batch.getContent().forEach(this.doTheKafkaOutboxJobPort::doTheJob);
    }
}
