package it.gov.acn.emblemata.scheduling.adapter.out;

import it.gov.acn.emblemata.integration.kafka.KafkaOutboxProcessor;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.scheduling.domain.port.out.DoTheKafkaOutboxJobPort;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class DoTheKafkaOutboxJobAdapter implements DoTheKafkaOutboxJobPort {

    private final KafkaOutboxProcessor kafkaOutboxProcessor;


    @Override
    public void doTheJob(KafkaOutbox kafkaOutbox) {
        this.kafkaOutboxProcessor.processOutbox(kafkaOutbox);
    }
}
