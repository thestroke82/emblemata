package it.gov.acn.emblemata.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Builder
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class KafkaOutbox {
  @Id
  @GeneratedValue(strategy = GenerationType.UUID)
  private UUID id;

  private String eventClass;

  private Instant publishDate;
  private Instant lastAttemptDate;
  private Instant completionDate;
  private int totalAttempts;

  @Column(columnDefinition = "text")
  private String event;

  @Column(columnDefinition = "text")
  private String lastError;

}
