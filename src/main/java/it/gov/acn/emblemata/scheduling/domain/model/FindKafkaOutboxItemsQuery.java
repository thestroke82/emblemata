package it.gov.acn.emblemata.scheduling.domain.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.domain.Pageable;

import java.time.Instant;

@Data
@Builder
public class FindKafkaOutboxItemsQuery {
    private Instant publishDateAfter;
    private Integer totalAttemptsLessThan;
    private Pageable pageable;
}
