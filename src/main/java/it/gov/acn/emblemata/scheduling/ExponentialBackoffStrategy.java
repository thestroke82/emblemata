package it.gov.acn.emblemata.scheduling;

import it.gov.acn.emblemata.model.KafkaOutbox;
import lombok.RequiredArgsConstructor;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class ExponentialBackoffStrategy implements OutboxItemSelectionStrategy{
    private final int backoffBase;

     /**
      * Select outbox items that have not yet been attempted or have completed the exponential backoff period
      * @param outstandingItems list of outbox items to filter
     *                    @return filtered list of outbox items
     */
    @Override
    public List<KafkaOutbox> execute(List<KafkaOutbox> outstandingItems) {
        if(outstandingItems==null || outstandingItems.isEmpty()){
            return outstandingItems;
        }
        Instant now = Instant.now();
        return outstandingItems.stream().filter(oe->{
            // outbox items that have never been attempted are always accepted
            if(oe.getTotalAttempts()==0 ||  oe.getLastAttemptDate()==null){
                return true;
            }
            // accepting only outbox for which the current backoff period has expired
            // the backoff period is calculated as base^attempts
            Instant backoffProjection = oe.getLastAttemptDate()
                    .plus(Duration.ofMinutes((long) Math.pow(backoffBase, oe.getTotalAttempts())));

            // if the projection is before now, it's time to retry, i.e. the backoff period has expired
            return backoffProjection.isBefore(now);
        }).toList();
    }
}
