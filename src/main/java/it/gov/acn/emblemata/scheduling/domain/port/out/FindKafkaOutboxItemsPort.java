package it.gov.acn.emblemata.scheduling.domain.port.out;

import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.scheduling.domain.model.FindKafkaOutboxItemsQuery;
import org.springframework.data.domain.Page;

public interface FindKafkaOutboxItemsPort {
    Page<KafkaOutbox> findOutstandingOutboxItems(FindKafkaOutboxItemsQuery query);
}
