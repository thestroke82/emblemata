package it.gov.acn.emblemata.outbox;

import it.gov.acn.emblemata.integration.kafka.KafkaClient;
import it.gov.acn.emblemata.model.event.BaseEvent;
import it.gov.acn.emblemata.util.Commons;
import it.gov.acn.outbox.model.OutboxItem;
import it.gov.acn.outbox.model.OutboxItemHandlerProvider;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OutboxHandler implements OutboxItemHandlerProvider {

  private final KafkaClient kafkaClient;

  @Override
  public void handle(OutboxItem outboxItem) {
    try {
      BaseEvent<?> event = (BaseEvent<?>) Commons.objectMapper.readValue(
          outboxItem.getEvent(),
          Class.forName(outboxItem.getEventType())
      );
      this.handle(event);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void handle(BaseEvent<?> event) {
    try {
      this.kafkaClient.send(event).get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
