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
