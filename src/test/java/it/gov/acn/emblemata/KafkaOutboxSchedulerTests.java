package it.gov.acn.emblemata;

import it.gov.acn.emblemata.config.KafkaOutboxSchedulerConfiguration;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.service.ConstituencyService;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

@SpringBootTest(properties = {
    "spring.kafka.initial-attempt=false",
    "spring.kafka.outbox.scheduler.enabled=true",
    "spring.kafka.outbox.scheduler.batch-size=2",
    "spring.kafka.outbox.scheduler.delay-ms=5000"
})
@ExtendWith(MockitoExtension.class)
public class KafkaOutboxSchedulerTests {
  @Autowired
  private EntityManagerFactory entityManagerFactory;

  @Autowired
  private KafkaOutboxSchedulerConfiguration kafkaOutboxSchedulerConfiguration;


  @Autowired
  private ConstituencyService constituencyService;


  @Test
  @Tag("clean")
  void save_10_constituency_test_scheduler() throws InterruptedException {
    for(int i=0; i<10; i++){
      this.constituencyService.saveConstituency(TestUtil.createEnel());
    }
    Instant now = Instant.now();
    Page<KafkaOutbox> activeOutboxes  = this.kafkaOutboxRepositoryPaged.findOutstandingEvents(null, null, Pageable.unpaged());
    Assertions.assertEquals(10, activeOutboxes.getTotalElements());
    Assertions.assertEquals( 10, activeOutboxes.getContent().size());

    Thread.sleep(this.kafkaOutboxSchedulerConfiguration.getDelayMs()+500);

    activeOutboxes  = this.kafkaOutboxRepositoryPaged.findOutstandingEvents(null, null, Pageable.unpaged());
    Assertions.assertEquals(10 - this.kafkaOutboxSchedulerConfiguration.getBatchSize() , activeOutboxes.getTotalElements());
    Assertions.assertEquals( 10 - this.kafkaOutboxSchedulerConfiguration.getBatchSize(), activeOutboxes.getContent().size());

    Thread.sleep(this.kafkaOutboxSchedulerConfiguration.getDelayMs());

    activeOutboxes  = this.kafkaOutboxRepositoryPaged.findOutstandingEvents(null, null, Pageable.unpaged());
    Assertions.assertEquals(10 - this.kafkaOutboxSchedulerConfiguration.getBatchSize()*2 , activeOutboxes.getTotalElements());
    Assertions.assertEquals( 10 - this.kafkaOutboxSchedulerConfiguration.getBatchSize()*2, activeOutboxes.getContent().size());

    long totalEstimatedTimeToWait = this.kafkaOutboxSchedulerConfiguration.getDelayMs()*(10 / this.kafkaOutboxSchedulerConfiguration.getBatchSize());

    Thread.sleep(totalEstimatedTimeToWait);

    activeOutboxes  = this.kafkaOutboxRepositoryPaged.findOutstandingEvents(null, null, Pageable.unpaged());
    Assertions.assertEquals(0 , activeOutboxes.getTotalElements());
    Assertions.assertEquals( 0, activeOutboxes.getContent().size());

  }

  @BeforeEach
  void clean(TestInfo info) {
    if(!info.getTags().contains("clean")) {
      return;
    }
    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();
    em.createNativeQuery("truncate table constituency").executeUpdate();
    em.createNativeQuery("truncate table kafka_outbox").executeUpdate();
    em.getTransaction().commit();
  }

}
