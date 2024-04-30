package it.gov.acn.emblemata.repository;

import it.gov.acn.emblemata.model.KafkaOutbox;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface KafkaOutboxRepository extends CrudRepository<KafkaOutbox, UUID> {
}
