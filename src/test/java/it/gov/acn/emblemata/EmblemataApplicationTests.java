package it.gov.acn.emblemata;

import it.gov.acn.emblemata.integration.kafka.KafkaClient;
import it.gov.acn.emblemata.listener.KafkaApplicationEventListener;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.model.event.ConstituencyCreatedEvent;
import it.gov.acn.emblemata.repository.ConstituencyRepository;
import it.gov.acn.emblemata.repository.KafkaOutboxRepository;
import it.gov.acn.emblemata.service.ConstituencyService;
import it.gov.acn.emblemata.service.KafkaOutboxService;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.time.Instant;
import java.util.List;


@SpringBootTest
@ExtendWith(MockitoExtension.class)
class EmblemataApplicationTests {
	@Autowired
	private ConstituencyService service;

	@SpyBean
	private ConstituencyRepository constituencyRepository;

	@SpyBean
	private KafkaOutboxRepository kafkaOutboxRepository;

	@Autowired
	private EntityManagerFactory entityManagerFactory;

	@SpyBean
	private KafkaClient kafkaClient;

	@SpyBean
	private KafkaOutboxService kafkaOutboxService;

	@SpyBean
	private KafkaApplicationEventListener applicationEventsListener;


	@Test
	@Tag("clean")
	void save_constituency_process_immediately_happy_path(){

		this.service.saveConstituency(TestUtil.createTelecom());

		// the constituency must have been created
		Assertions.assertTrue(this.constituencyRepository.findAll().iterator().hasNext());

		// there must be a single outbox record and it must have 1 succesful send attempt
		KafkaOutbox outbox = this.kafkaOutboxRepository.findAll().iterator().next();
		Assertions.assertEquals(1, outbox.getTotalAttempts());
		Assertions.assertNotNull(outbox.getCompletionDate());
	}

	@Test
	@Tag("clean")
	void save_constituency_process_immediately_kafka_error(){
		Mockito.doThrow(new RuntimeException("Kafka is down"))
				.when(this.kafkaClient).send(Mockito.any());

		this.service.saveConstituency(TestUtil.createTelecom());

		// the constituency must have been created
		Assertions.assertTrue(this.constituencyRepository.findAll().iterator().hasNext());

		// there must be a single outbox record and it must have 1 send attempt with error
		KafkaOutbox outbox = this.kafkaOutboxRepository.findAll().iterator().next();
		Assertions.assertEquals(1, outbox.getTotalAttempts());
		Assertions.assertNull(outbox.getCompletionDate());
		Assertions.assertNotNull(outbox.getLastError());
	}




	@BeforeEach
	void beforeEach(TestInfo info) {
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