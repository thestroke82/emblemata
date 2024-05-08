package it.gov.acn.emblemata.locking;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.SimpleLock;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaOutboxLockManager {
  private final LockProvider lockProvider;

  public synchronized Optional<SimpleLock> lock(){
    return this.lockProvider.lock(getLockConfiguration());
  }

  public synchronized void release(SimpleLock simpleLock){
    if(simpleLock != null){
      simpleLock.unlock();
    }
  }

  public LockConfiguration getLockConfiguration(){
    return new LockConfiguration(Instant.now(), "kafkaOutbox", Duration.ofSeconds(30), Duration.ofMillis(200));
  }

}
