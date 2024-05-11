package it.gov.acn.emblemata.locking;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockManager;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.SimpleLock;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaOutboxLockManager {
  private final LockProvider lockProvider;
  private List<SimpleLock> activeLocks = new ArrayList<SimpleLock>();

  public synchronized Optional<SimpleLock> lock(){
    Optional<SimpleLock> lock = this.lockProvider.lock(getLockConfiguration());
    if(lock.isPresent()){
      activeLocks.add(lock.get());
    }
    return lock;
  }

  public synchronized void release(SimpleLock simpleLock){
    if(simpleLock != null){
      activeLocks.remove(simpleLock);
      simpleLock.unlock();
    }
  }

  public synchronized void releaseAllLocks(){
    activeLocks.forEach(SimpleLock::unlock);
    activeLocks.clear();
  }

  public LockConfiguration getLockConfiguration(){
    return new LockConfiguration(Instant.now(), "kafkaOutbox", Duration.ofSeconds(30), Duration.ofMillis(200));
  }

}
