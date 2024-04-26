package it.gov.acn.emblemata.repository;

import it.gov.acn.emblemata.model.Constituency;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ConstituencyRepository extends CrudRepository<Constituency, String> {



}
