package it.gov.acn.emblemata;

import it.gov.acn.emblemata.integration.kafka.KafkaClient;
import it.gov.acn.emblemata.model.Constituency;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.model.event.ConstituencyCreatedEvent;
import it.gov.acn.emblemata.repository.KafkaOutboxRepository;
import it.gov.acn.emblemata.service.ConstituencyService;
import it.gov.acn.emblemata.service.KafkaOutboxService;
import it.gov.acn.emblemata.util.Commons;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;


@SpringBootTest
@ExtendWith(MockitoExtension.class)
class EmblemataApplicationTests {
	@Autowired
	private ConstituencyService service;

	@Autowired
	private KafkaOutboxRepository kafkaOutboxRepository;

	@Autowired
	private EntityManagerFactory entityManagerFactory;

	@SpyBean
	private KafkaClient kafkaClient;

	@SpyBean
	private KafkaOutboxService kafkaOutboxService;


	@Test
	@Tag("clean")
	void save_constituency_kafka_exception() {
		Mockito.doThrow(new RuntimeException("Kafka is down"))
				.when(this.kafkaClient).sendConstituencyEvent(Mockito.any());

		Constituency constituency = Constituency.builder()
				.name("Enel")
				.address("Via dei salici, XXX Roma(RM)")
				.build();

		Constituency savedConstituency = this.service.saveConstituency(constituency);

		KafkaOutbox outbox = null;
		Iterable<KafkaOutbox> outboxes = this.kafkaOutboxRepository.findAll();
    for (KafkaOutbox currOutbox : outboxes) {
      ConstituencyCreatedEvent constituencyCreatedEvent = Commons.gson.fromJson(
          currOutbox.getEvent(), ConstituencyCreatedEvent.class);
      if (savedConstituency.getId().equals(constituencyCreatedEvent.getPayload().getId())) {
        outbox = currOutbox;
        break;
      }
    }
		Assertions.assertNotNull(outbox);
		Assertions.assertEquals(1, outbox.getTotalAttempts());
		Assertions.assertNotNull(outbox.getError());
		Assertions.assertNotNull(outbox.getLastAttemptDate());
	}

	@Test
	@Tag("clean")
	void save_constituency_happy_path() {

		Constituency constituency = Constituency.builder()
				.name("Enel")
				.address("Via dei salici, XXX Roma(RM)")
				.build();

		Constituency savedConstituency = this.service.saveConstituency(constituency);

		KafkaOutbox outbox = null;
		Iterable<KafkaOutbox> outboxes = this.kafkaOutboxRepository.findAll();
		for (KafkaOutbox currOutbox : outboxes) {
			ConstituencyCreatedEvent constituencyCreatedEvent = Commons.gson.fromJson(
					currOutbox.getEvent(), ConstituencyCreatedEvent.class);
			if (savedConstituency.getId().equals(constituencyCreatedEvent.getPayload().getId())) {
				outbox = currOutbox;
				break;
			}
		}
		Assertions.assertNotNull(outbox);
		Assertions.assertEquals(1, outbox.getTotalAttempts());
		Assertions.assertNull(outbox.getError());
		Assertions.assertNotNull(outbox.getCompletionDate());
	}

	@Test
	@Tag("clean")
	void save_constituency_exception_outbox_service() {
		Mockito.doThrow(new RuntimeException("Outbox write error"))
				.when(this.kafkaOutboxService).succesfulAttempt(Mockito.any());


		Constituency constituency = Constituency.builder()
				.name("Spritz")
				.address("Piazza del spumante, 001122 Roma(RM)")
				.build();

		Constituency savedConstituency = this.service.saveConstituency(constituency);

		KafkaOutbox outbox = null;
		Iterable<KafkaOutbox> outboxes = this.kafkaOutboxRepository.findAll();
		for (KafkaOutbox currOutbox : outboxes) {
			ConstituencyCreatedEvent constituencyCreatedEvent = Commons.gson.fromJson(
					currOutbox.getEvent(), ConstituencyCreatedEvent.class);
			if (savedConstituency.getId().equals(constituencyCreatedEvent.getPayload().getId())) {
				outbox = currOutbox;
				break;
			}
		}
		Assertions.assertNotNull(outbox);
		Assertions.assertEquals(1, outbox.getTotalAttempts());
		Assertions.assertNotNull(outbox.getError());
		Assertions.assertNull(outbox.getCompletionDate());
	}

	@AfterEach
	void afterEach(TestInfo info) {
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
