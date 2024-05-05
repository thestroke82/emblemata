package it.gov.acn.emblemata.repository;

import it.gov.acn.emblemata.model.KafkaOutbox;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface KafkaOutboxRepository extends CrudRepository<KafkaOutbox, UUID> {
    @Query("select k from KafkaOutbox k"
            + " where k.completionDate is null"
            + " and (?1 is null or k.totalAttempts < ?1)")
    List<KafkaOutbox> findOutstandingEvents(Integer totalAttemptsLessThan, Sort sort);
}
