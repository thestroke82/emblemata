package it.gov.acn.emblemata.repository;

import it.gov.acn.emblemata.model.KafkaOutbox;

import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface KafkaOutboxRepository extends CrudRepository<KafkaOutbox, UUID> {

    /**
     * Return all outbox items that have a null completion date and total attempts less than the given value.
     * @param totalAttemptsLessThan Maximum number of attempts
     * @param sort Sorting criteria
     * @return List of KafkaOutbox as requested
     */
    @Query("select k from KafkaOutbox k"
            + " where k.completionDate is null"
            + " and (?1 is null or k.totalAttempts < ?1)")
    List<KafkaOutbox> findOutstandingItems(Integer totalAttemptsLessThan, Sort sort);
}
