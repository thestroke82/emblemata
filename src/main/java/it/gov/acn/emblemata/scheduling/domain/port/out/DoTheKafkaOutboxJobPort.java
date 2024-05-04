package it.gov.acn.emblemata.scheduling.domain.port.out;

import it.gov.acn.emblemata.model.KafkaOutbox;

public interface DoTheKafkaOutboxJobPort {

    void doTheJob(KafkaOutbox kafkaOutbox);
}
