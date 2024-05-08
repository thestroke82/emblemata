package it.gov.acn.emblemata.unit;

import static org.assertj.core.api.Assertions.assertThat;

import it.gov.acn.emblemata.PersistenceTestContext;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.repository.KafkaOutboxRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Sort;


@SpringBootTest
@ExtendWith(MockitoExtension.class)
public class KafkaOutboxRepositoryTest extends PersistenceTestContext {
  @Autowired
  private KafkaOutboxRepository kafkaOutboxRepository;
  @Autowired
  private EntityManagerFactory entityManagerFactory;

  @Test
  @Tag("clean")
  void given_no_events_findOutstandingEvents_empty_result() {
    List<KafkaOutbox> result = kafkaOutboxRepository.findOutstandingEvents(1, Sort.unsorted());

    assertThat(result).isEmpty();
  }

  @Test
  @Tag("clean")
  void given_some_events_findOutstandingEvents_not_empty_result() {
    KafkaOutbox kafkaOutbox = KafkaOutbox.builder()
        .publishDate(Instant.now())
        .event("gigi")
        .eventClass("gigi.class")
        .lastError("error")
        .id(UUID.randomUUID())
        .build();

    KafkaOutbox kafkaOutbox1 = KafkaOutbox.builder()
        .publishDate(Instant.now())
        .event("gigi 1")
        .eventClass("gigi.class 1")
        .lastError("error 1")
        .id(UUID.randomUUID())
        .build();

    this.kafkaOutboxRepository.save(kafkaOutbox);
    this.kafkaOutboxRepository.save(kafkaOutbox1);

    List<KafkaOutbox> result = kafkaOutboxRepository.findOutstandingEvents(1, Sort.unsorted());

    assertThat(result).isNotEmpty();
  }

  @Test
  @Tag("clean")
  void given_some_events_findOutstandingEvents_test_totalAttempts_condition() {

    for(int i=0; i<10; i++){
      KafkaOutbox kafkaOutbox = KafkaOutbox.builder()
          .publishDate(Instant.now())
          .event("gigi")
          .eventClass("gigi.class")
          .lastError("error")
          .id(UUID.randomUUID())
          .build();
      kafkaOutbox.setTotalAttempts(i);
      this.kafkaOutboxRepository.save(kafkaOutbox);
    }

    List<KafkaOutbox> result = kafkaOutboxRepository.findOutstandingEvents(5, Sort.unsorted());

    assertEquals(5, result.size());
    assertThat(result).allMatch(kafkaOutbox -> kafkaOutbox.getTotalAttempts() < 5);
  }

  @Test
  @Tag("clean")
  void given_some_events_findOutstandingEvents_test_sort() {
    KafkaOutbox kafkaOutbox = KafkaOutbox.builder()
        .publishDate(Instant.now())
        .event("gigi")
        .eventClass("gigi.class")
        .lastError("error")
        .id(UUID.randomUUID())
        .build();

    KafkaOutbox kafkaOutbox1 = KafkaOutbox.builder()
        .publishDate(Instant.now().minus(1000, ChronoUnit.MILLIS))
        .event("gigi before")
        .eventClass("gigi.class 1")
        .lastError("error 1")
        .id(UUID.randomUUID())
        .build();

    this.kafkaOutboxRepository.save(kafkaOutbox);
    this.kafkaOutboxRepository.save(kafkaOutbox1);

    List<KafkaOutbox> result = kafkaOutboxRepository.findOutstandingEvents(null, Sort.by("publishDate").ascending());

    assertThat(result).isSortedAccordingTo((a, b) -> {
      if(a.getPublishDate().toEpochMilli()== b.getPublishDate().toEpochMilli()){
        return 0;
      }
      return a.getPublishDate().isAfter(b.getPublishDate())?1:-1;
    });
    assertEquals("gigi before", result.get(0).getEvent());
  }

  @BeforeEach
  void clean(TestInfo info) {
    if(!info.getTags().contains("clean")) {
      return;
    }
    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();
    em.createNativeQuery("truncate table kafka_outbox").executeUpdate();
    em.getTransaction().commit();
  }

}
