package it.gov.acn.emblemata.repository;

import it.gov.acn.emblemata.model.KafkaOutbox;
import java.time.Instant;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface KafkaOutboxRepositoryPaged extends PagingAndSortingRepository<KafkaOutbox, UUID> {
  @Query("select k from KafkaOutbox k"
      + " where k.completionDate is null"
      + " and (cast(?1 as timestamp) is null or k.publishDate > ?1 )"
      + " and (?2 is null or k.totalAttempts < ?2)")
  Page<KafkaOutbox> findOutstandingEvents(Instant publishDateAfter, Integer totalAttemptsLessThan, Pageable pageable);
}
