package it.gov.acn.emblemata.repository;

import it.gov.acn.emblemata.model.KafkaOutbox;

import java.util.List;
import java.util.UUID;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface KafkaOutboxRepository extends CrudRepository<KafkaOutbox, UUID> {
//  List<EventOutbox> findByCompletionDateIsNullAndRemainingAttemptsGreaterThanZeroAndPublishDateBefore(
//      Instant date);
}
