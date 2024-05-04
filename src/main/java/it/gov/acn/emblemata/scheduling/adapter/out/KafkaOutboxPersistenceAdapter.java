package it.gov.acn.emblemata.scheduling.adapter.out;

import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.repository.KafkaOutboxRepositoryPaged;
import it.gov.acn.emblemata.scheduling.domain.model.FindKafkaOutboxItemsQuery;
import it.gov.acn.emblemata.scheduling.domain.port.out.FindKafkaOutboxItemsPort;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaOutboxPersistenceAdapter implements FindKafkaOutboxItemsPort {

    private final KafkaOutboxRepositoryPaged kafkaOutboxRepositoryPaged;

    @Override
    public Page<KafkaOutbox> findOutstandingOutboxItems(FindKafkaOutboxItemsQuery query) {
        if(query == null) {
            throw new IllegalArgumentException("Query cannot be null");
        }
        return kafkaOutboxRepositoryPaged.findOutstandingEvents(query.getPublishDateAfter(), query.getTotalAttemptsLessThan(), query.getPageable());
    }
}
