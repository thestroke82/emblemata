package it.gov.acn.emblemata.unit;

import it.gov.acn.emblemata.config.KafkaOutboxSchedulerConfiguration;
import it.gov.acn.emblemata.integration.kafka.KafkaOutboxProcessor;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.repository.KafkaOutboxRepository;
import it.gov.acn.emblemata.scheduling.ExponentialBackoffStrategy;
import it.gov.acn.emblemata.scheduling.KafkaOutboxScheduler;
import it.gov.acn.emblemata.scheduling.OutboxItemSelectionStrategy;
import it.gov.acn.emblemata.scheduling.SchedulerExceptionHandlingAspect;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.List;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.springframework.test.util.ReflectionTestUtils;

public class KafkaOutboxSchedulerUnitTests {

    @Mock
    private KafkaOutboxSchedulerConfiguration configuration;

    @Mock
    private KafkaOutboxRepository kafkaOutboxRepository;

    @Mock
    private KafkaOutboxProcessor kafkaOutboxProcessor;

    @Mock
    private OutboxItemSelectionStrategy outboxItemSelectionStrategy;


    private KafkaOutboxScheduler kafkaOutboxScheduler;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        Mockito.doAnswer(invocation -> invocation.getArgument(0)).when(outboxItemSelectionStrategy).execute(Mockito.any());
        kafkaOutboxScheduler = new KafkaOutboxScheduler(configuration, kafkaOutboxRepository, kafkaOutboxProcessor);
        ReflectionTestUtils.setField(kafkaOutboxScheduler, "outboxItemSelectionStrategy", outboxItemSelectionStrategy);
    }

    @Test
    public void process_kafka_outbox_when_no_outstanding_items() {
        Mockito.when(kafkaOutboxRepository.findOutstandingItems(Mockito.anyInt(), Mockito.any())).thenReturn(Collections.emptyList());

        kafkaOutboxScheduler.processKafkaOutbox();

        Mockito.verify(kafkaOutboxProcessor, Mockito.never()).processOutbox(Mockito.any());
    }

    @Test
    public void process_kafka_outbox_when_outstanding_items_exist() {
        KafkaOutbox kafkaOutbox = new KafkaOutbox();
        Mockito.doAnswer(invocation -> Collections.singletonList(kafkaOutbox)).when(kafkaOutboxRepository).findOutstandingItems(Mockito.anyInt(), Mockito.any());

        kafkaOutboxScheduler.processKafkaOutbox();

        Mockito.verify(kafkaOutboxProcessor, Mockito.times(1)).processOutbox(kafkaOutbox);
    }

    @Test
    public void process_kafka_outbox_when_exception_occurs() {
        Mockito.when(kafkaOutboxRepository.findOutstandingItems(Mockito.anyInt(), Mockito.any())).thenThrow(new RuntimeException());

        Logger loggerMock = Mockito.mock(Logger.class);
        SchedulerExceptionHandlingAspect aspect = new SchedulerExceptionHandlingAspect();
        ReflectionTestUtils.setField(aspect, "logger", loggerMock);

        try {
            kafkaOutboxScheduler.processKafkaOutbox();
        } catch (RuntimeException e) {
            ReflectionTestUtils.invokeMethod(aspect, "handleException", e, "kafkaOutboxScheduler.processKafkaOutbox");
        }

        Mockito.verify(loggerMock).error(Mockito.eq("Error in scheduled task {}"),
            Mockito.eq("kafkaOutboxScheduler.processKafkaOutbox"), Mockito.any(RuntimeException.class));

        Mockito.verify(kafkaOutboxProcessor, Mockito.never()).processOutbox(Mockito.any());
    }
}