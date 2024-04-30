package it.gov.acn.emblemata;

import it.gov.acn.emblemata.integration.kafka.KafkaClient;
import it.gov.acn.emblemata.listener.KafkaApplicationEventListener;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.model.event.ConstituencyCreatedEvent;
import it.gov.acn.emblemata.repository.ConstituencyRepository;
import it.gov.acn.emblemata.repository.KafkaOutboxRepository;
import it.gov.acn.emblemata.repository.KafkaOutboxRepositoryPaged;
import it.gov.acn.emblemata.service.ConstituencyService;
import it.gov.acn.emblemata.service.KafkaOutboxService;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import java.time.Instant;
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


@SpringBootTest
@ExtendWith(MockitoExtension.class)
class EmblemataApplicationTests {
	@Autowired
	private ConstituencyService service;

	@SpyBean
	private ConstituencyRepository constituencyRepository;

	@SpyBean
	private KafkaOutboxRepository kafkaOutboxRepository;

	@SpyBean
	private KafkaOutboxRepositoryPaged kafkaOutboxRepositoryPaged;

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

	@Test
	@Tag("clean")
	void test_kafka_outbox_repository_fetch_outbox_page(){
		for(int i = 0; i < 10; i++){
			this.kafkaOutboxService.saveOutbox(
					ConstituencyCreatedEvent.builder()
							.payload(TestUtil.createTelecom())
							.build()
			);
		}
		Instant now = Instant.now();
//
//		TestUtil.changeLogLevels(List.of(
//				Pair.of("logging.level.org.hibernate.SQL", Level.DEBUG),
//				Pair.of("logging.level.org.hibernate.type.descriptor.sql", Level.TRACE)
//		));

		Page<KafkaOutbox> result = this.kafkaOutboxRepositoryPaged.findOutstandingEvents(null,  null, null);
		Assertions.assertEquals(10, result.getTotalElements());
		Assertions.assertEquals(10, result.getSize());
		Assertions.assertEquals(10, result.getNumberOfElements());

		Pageable pageable = PageRequest.of(0, 3, Sort.by("publishDate").descending());
		result = this.kafkaOutboxRepositoryPaged.findOutstandingEvents(null,   null, pageable);
		Assertions.assertEquals(10, result.getTotalElements());
		Assertions.assertEquals(3, result.getSize());
		Assertions.assertEquals(3, result.getNumberOfElements());
		Assertions.assertEquals(4, result.getTotalPages());

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
