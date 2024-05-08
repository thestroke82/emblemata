package it.gov.acn.emblemata.integration.kafka;

import it.gov.acn.emblemata.config.KafkaConfiguration;
import it.gov.acn.emblemata.config.KafkaOutboxSchedulerConfiguration;
import it.gov.acn.emblemata.integration.IntegrationManager;
import it.gov.acn.emblemata.model.KafkaOutbox;
import it.gov.acn.emblemata.model.event.BaseEvent;
import it.gov.acn.emblemata.service.KafkaOutboxService;
import it.gov.acn.emblemata.util.Commons;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.SimpleLock;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaOutboxProcessor {
  private final Logger logger = LoggerFactory.getLogger(KafkaOutboxService.class);

  private final KafkaClient kafkaClient;
  private final KafkaOutboxService kafkaOutboxService;
  private final KafkaOutboxStatistics kafkaOutboxStatistics;


  public void processOutbox(KafkaOutbox outbox){
    String errorMessage = null;
    try {
      BaseEvent<?> event = (BaseEvent<?>) Commons.gson.fromJson(outbox.getEvent(),  Class.forName(outbox.getEventClass()));
      this.kafkaClient.send(event).get();
    } catch (Exception e) {
      logger.error("Error processing outbox {}",outbox.getId());
      logger.error("Exception: ", e);
      errorMessage = e.getMessage();
    }

    if(errorMessage!= null){
      this.kafkaOutboxService.unsuccesfulAttempt(outbox.getId(), errorMessage);
      this.kafkaOutboxStatistics.registerUnsuccessfulAttempt(outbox);
    }else{
      this.kafkaOutboxService.succesfulAttempt(outbox.getId());
      this.kafkaOutboxStatistics.registerSuccessfulAttempt(outbox);
    }
  }
}
