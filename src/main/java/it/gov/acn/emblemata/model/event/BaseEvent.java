package it.gov.acn.emblemata.model.event;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class BaseEvent<T> {
  protected String eventId;
  protected String eventType;
  protected Instant timestamp;
  protected T payload;
}
