package it.gov.acn.emblemata.integration;

import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.concurrent.CompletedFuture;
import it.gov.acn.emblemata.PersistenceTestContext;
import it.gov.acn.emblemata.TestUtil;
import it.gov.acn.emblemata.config.KafkaConfiguration;
import it.gov.acn.emblemata.integration.kafka.KafkaClient;
import it.gov.acn.emblemata.locking.KafkaOutboxLockManager;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.repository.ConstituencyRepository;
import it.gov.acn.emblemata.repository.KafkaOutboxRepository;
import it.gov.acn.emblemata.service.ConstituencyService;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import java.util.concurrent.CompletableFuture;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;


@SpringBootTest(
		properties = {
				"spring.kafka.enabled=true",
				"spring.kafka.initial-attempt=true",
				"spring.kafka.outbox.scheduler.enabled=false"
		}
)
@ExtendWith(MockitoExtension.class)
class ConstituencyServiceTest extends PersistenceTestContext {
	@Autowired
	private ConstituencyService service;
	@Autowired
	private EntityManagerFactory entityManagerFactory;
	@Autowired
	private KafkaOutboxLockManager kafkaOutboxLockManager;
	@SpyBean
	private ConstituencyRepository constituencyRepository;
	@SpyBean
	private KafkaOutboxRepository kafkaOutboxRepository;
	@SpyBean
	private KafkaClient kafkaClient;



	@Test
	@Tag("clean")
	void when_kafkaSuccess_saveConstituency_bestEffort_succesful(){

		// given
		// mock kafka client to return a completed future
		Mockito.doReturn(CompletableFuture.completedFuture(null))
				.when(kafkaClient).send(Mockito.any());

		// when
		this.service.saveConstituency(TestUtil.createTelecom());

		// then:

		// the constituency must have been created
		Assertions.assertTrue(this.constituencyRepository.findAll().iterator().hasNext());

		// there must be a single outbox record and it must have 1 successful send attempt
		KafkaOutbox outbox = this.kafkaOutboxRepository.findAll().iterator().next();
		Assertions.assertEquals(1, outbox.getTotalAttempts());
		Assertions.assertNotNull(outbox.getCompletionDate());
	}

	@Test
	@Tag("clean")
	void when_kafkaError_saveConstituency_bestEffort_unsuccesful(){

		// given
		Mockito.doThrow(new RuntimeException("Kafka is down"))
				.when(this.kafkaClient).send(Mockito.any());

		// when
		this.service.saveConstituency(TestUtil.createTelecom());

		// then:

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
	void when_jpaError_saveConstituency_saves_nothing(){

		// given
		Mockito.doThrow(new RuntimeException("JPA is down"))
				.when(this.constituencyRepository).save(Mockito.any());
		// mock kafka client to return a completed future
		Mockito.doReturn(CompletableFuture.completedFuture(null))
				.when(kafkaClient).send(Mockito.any());

		// when
		Executable executable = () -> this.service.saveConstituency(TestUtil.createTelecom());

		// then:
		Assertions.assertThrows(RuntimeException.class, executable);

		// the constituency must not have been created
		Assertions.assertFalse(this.constituencyRepository.findAll().iterator().hasNext());

		// the outbox must not have been created
		Assertions.assertFalse(this.constituencyRepository.findAll().iterator().hasNext());
	}

	@BeforeEach
	void clean(TestInfo info) {
		if(!info.getTags().contains("clean")) {
			return;
		}
		this.kafkaOutboxLockManager.releaseAllLocks();
		EntityManager em = entityManagerFactory.createEntityManager();
		em.getTransaction().begin();
		em.createNativeQuery("truncate table constituency").executeUpdate();
		em.createNativeQuery("truncate table kafka_outbox").executeUpdate();
		em.getTransaction().commit();
	}
}
