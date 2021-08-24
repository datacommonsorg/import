package org.datacommons.server;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface ObservationRepository extends JpaRepository<Observation, Long> {
  @Query(
      value = "SELECT * FROM OBSERVATION o WHERE o.OBSERVATION_ABOUT = ?1 and o.VARIABLE = ?2",
      nativeQuery = true)
  List<Observation> findObservationByPlaceAndStatVar(String place, String statVar);
}
