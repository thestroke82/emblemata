package it.gov.acn.emblemata.model.event;

import it.gov.acn.emblemata.model.Constituency;
import java.time.Instant;
import java.util.UUID;
import lombok.Builder;
import lombok.Getter;

@Getter
public class ConstituencyCreatedEvent extends BaseEvent< Constituency>{

  @Builder
  public ConstituencyCreatedEvent(Constituency payload) {
    super(UUID.randomUUID().toString(), "ConstituencyCreatedEvent", Instant.now(), payload);
  }
}
