package it.gov.acn.emblemata.service;

import it.gov.acn.emblemata.model.Constituency;
import it.gov.acn.emblemata.model.event.ConstituencyCreatedEvent;
import it.gov.acn.emblemata.repository.ConstituencyRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class ConstituencyService {

    private final ConstituencyRepository constituencyRepository;
    private final ApplicationEventPublisher eventPublisher;

    @Transactional
    public Constituency saveConstituency(Constituency constituency) {

        Constituency ret = this.constituencyRepository.save(constituency);
        this.eventPublisher.publishEvent(
            ConstituencyCreatedEvent.builder()
                .payload(constituency)
                .build()
        );
        return ret;
    }

}
