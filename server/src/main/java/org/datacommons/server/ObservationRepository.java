// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.datacommons.server;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

// Interface of Observation table JPA repository.
// This defines the queries (functions) used to access the Observation table.
@Repository
public interface ObservationRepository extends JpaRepository<Observation, Long> {
  @Query(
      value =
          "SELECT * FROM OBSERVATION o WHERE o.OBSERVATION_ABOUT = ?1 and o.VARIABLE_MEASURED = ?2",
      nativeQuery = true)
  List<Observation> findObservationByPlaceAndStatVar(String place, String statVar);
}
