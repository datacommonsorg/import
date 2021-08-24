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

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import org.hibernate.annotations.GenericGenerator;

@Entity(name = "OBSERVATION")
public class Observation {

  @Id
  @GeneratedValue(strategy = GenerationType.AUTO, generator = "native")
  @GenericGenerator(name = "native", strategy = "native")
  @Column(name = "ID")
  private Long id;

  @Column(name = "OBSERVATION_ABOUT")
  private String observationAbout;

  @Column(name = "OBSERVATION_DATE")
  private String observationDate;

  @Column(name = "VALUE")
  private String value;

  @Column(name = "VARIABLE")
  private String variable;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getObservationAbout() {
    return observationAbout;
  }

  public void setObservationAbout(String observationAbout) {
    this.observationAbout = observationAbout;
  }

  public String getObservationDate() {
    return observationDate;
  }

  public void setObservationDate(String date) {
    this.observationDate = date;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public String getVariable() {
    return variable;
  }

  public void setVariable(String variable) {
    this.variable = variable;
  }
}
