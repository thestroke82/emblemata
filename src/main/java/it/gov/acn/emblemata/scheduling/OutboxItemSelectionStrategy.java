package it.gov.acn.emblemata.scheduling;

import it.gov.acn.emblemata.model.KafkaOutbox;

import java.util.List;

public interface OutboxItemSelectionStrategy {
    /**
      * Select outbox items based on the concrete implementation of a strategy
      * @param outstandingItems list of outbox items to filter
     *                    @return filtered list of outbox items
     */
    List<KafkaOutbox> execute(List<KafkaOutbox> outstandingItems);
}
