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

package org.datacommons.util;

import java.util.List;
import java.util.Set;

// A class of static constants and methods.
public final class Vocabulary {
  // Common types
  public static final String CLASS_TYPE = "Class";
  public static final String PROPERTY_TYPE = "Property";
  public static final String QUANTITY_RANGE_TYPE = "QuantityRange";
  public static final String QUANTITY_TYPE = "Quantity";
  public static final String CURATOR_TYPE = "Curator";
  public static final String PROVENANCE_TYPE = "Provenance";
  public static final String SOURCE_TYPE = "Source";
  public static final String STAT_VAR_TYPE = "StatisticalVariable";
  public static final String STAT_VAR_OBSERVATION_TYPE = "StatVarObservation";
  public static final String LEGACY_OBSERVATION_TYPE_SUFFIX = "Observation";
  public static final String LEGACY_POPULATION_TYPE_SUFFIX = "Population";
  public static final String STAT_VAR_GROUP_TYPE = "StatVarGroup";
  public static final String PLACE_TYPE = "Place";
  public static final String ADMIN_AREA_TYPE = "AdministrativeArea";
  public static final String THING_TYPE = "Thing";
  public static final String GEO_COORDINATES_TYPE = "GeoCoordinates";

  // Schema/Horizontal properties.
  public static final String TYPE_OF = "typeOf";
  public static final String SUB_CLASS_OF = "subClassOf";
  public static final String SUB_PROPERTY_OF = "subPropertyOf";
  public static final String RANGE_INCLUDES = "rangeIncludes";
  public static final String DOMAIN_INCLUDES = "domainIncludes";
  public static final String SPECIALIZATION_OF = "specializationOf";
  public static final String SUB_TYPE = "subType";
  public static final String MEMBER_OF = "memberOf";
  public static final String DCID = "dcid";
  public static final String PROVENANCE = "provenance";
  public static final String LOCAL_CURATOR_LEVEL_ID = "localCuratorLevelId";
  public static final String KEY_STRING = "keyString";
  public static final String NAME = "name";
  public static final String LABEL = "label";
  public static final String ALTERNATE_NAME = "alternateName";
  public static final String DESCRIPTION = "description";
  public static final String DESCRIPTION_URL = "descriptionUrl";
  public static final String URL = "url";

  // Place related properties.
  public static final String LATITUDE = "latitude";
  public static final String LONGITUDE = "longitude";
  public static final String LAT_AND_LONG = "latlong";
  public static final String ELEVATION = "elevation";
  public static final String TIME_ZERO = "timezone";
  public static final String DISSOLUTION_DATE = "dissolutionDate";
  public static final String GEO_DCID_PREFIX = "latLong";
  public static final String CONTAINED_IN_PLACE = "containedInPlace";
  public static final String CONTAINED_IN = "containedIn"; // Superseded by "containedInPlace".
  public static final String GEO_ID = "geoId";
  public static final String WIKIDATA_ID = "wikidataId";
  public static final String GEO_NAMES_ID = "geoNamesId";
  public static final String ISO_CODE = "isoCode";
  public static final String NUTS_CODE = "nutsCode";
  public static final String INDIAN_CENSUS_AREA_CODE_2011 = "indianCensusAreaCode2011";

  // StatVar/StatVarObs related things.
  public static final String POPULATION_TYPE = "populationType";
  public static final String MEASURED_PROP = "measuredProperty";
  public static final String OBSERVATION_ABOUT = "observationAbout";
  public static final String VARIABLE_MEASURED = "variableMeasured";
  public static final String STAT_TYPE = "statType";
  public static final String CONSTRAINT_PROPS = "constraintProperties";
  public static final String MEASUREMENT_DENOMINATOR = "measurementDenominator";
  public static final String MEASUREMENT_QUALIFIER = "measurementQualifier";
  public static final String SCALING_FACTOR = "scalingFactor";
  public static final String UNIT = "unit";
  public static final String MEASUREMENT_METHOD = "measurementMethod";
  public static final String OBSERVATION_DATE = "observationDate";
  public static final String OBSERVATION_PERIOD = "observationPeriod";
  public static final String GENERIC_VALUE = "value";
  public static final String OBSERVED_NODE = "observedNode"; // Deprecated
  public static final String LOCATION = "location"; // Deprecated
  public static final String POPULATION_GROUP = "populationGroup";

  // Values taken by statType.
  public static final String MEDIAN_VALUE = "medianValue";
  public static final String MEAN_VALUE = "meanValue";
  public static final String MIN_VALUE = "minValue";
  public static final String MAX_VALUE = "maxValue";
  public static final String SUM_VALUE = "sumValue";
  public static final String MEASURED_VALUE = "measuredValue";
  public static final String STD_DEVIATION_VALUE = "stdDeviationValue";
  public static final String PERCENTILE_10 = "percentile10";
  public static final String PERCENTILE_25 = "percentile25";
  public static final String PERCENTILE_75 = "percentile75";
  public static final String PERCENTILE_90 = "percentile90";
  public static final String MARGIN_OF_ERROR = "marginOfError";
  public static final String STD_ERROR = "stdError";
  public static final String MEAN_STD_ERROR = "meanStdError";
  public static final String SAMPLE_SIZE = "sampleSize";
  public static final String GROWTH_RATE = "growthRate";
  public static final String MEASUREMENT_RESULT = "measurementResult";

  // Quantity/QuantityRange props
  public static final String START_VALUE = "startValue";
  public static final String END_VALUE = "endValue";
  public static final String VALUE = "value";
  public static final String UNIT_OF_MEASURE = "unitOfMeasure";

  // MCF Things
  // ----------

  public static final String NODE = "Node";
  public static final String CONTEXT = "Context";
  // Property to define prefixes used in an MCF file.
  public static final String NAMESPACE = "namespace";
  public static final char VALUE_SEPARATOR = ',';

  // MCF references
  // --------------
  public static final char REFERENCE_DELIMITER = ':';
  // When a node is referenced in Node MCF files, those references are prefixed with this.
  public static final String INTERNAL_REF_PREFIX = "l:";
  public static final String DCID_PREFIX = "dcid:";
  public static final String DC_SCHEMA_PREFIX = "dcs:";
  public static final String SCHEMA_ORG_PREFIX = "schema:";

  // Template MCF CONSTANTS
  // ----------------------
  public static final String ENTITY_PREFIX = "E:";
  public static final String COLUMN_PREFIX = "C:";
  public static final String TABLE_DELIMITER = "->";
  public static final String FUNCTIONAL_DEPS = "functionalDeps";

  // Census specific property.
  public static final String CENSUS_ACS_TABLE_ID = "censusACSTableId";
  public static final String ISTAT_ID = "istatId";
  public static final String AUSTRIAN_MUNICIPALITY_KEY = "austrianMunicipalityKey";

  // Legacy properties
  public static final String IS_PUBLIC = "isPublic";
  public static final String DBG_MCF_FILE = "resMCFFile";

  // List of properties known not to be a constraint property.
  public static final Set<String> NON_CONSTRAINT_STAT_VAR_PROPERTIES =
      Set.of(
          // Basic Properties
          TYPE_OF,
          DCID,
          PROVENANCE,
          IS_PUBLIC,
          LOCAL_CURATOR_LEVEL_ID,
          URL,
          MEMBER_OF,
          NAME,
          LABEL,
          DESCRIPTION,
          DESCRIPTION_URL,
          ALTERNATE_NAME,
          KEY_STRING,
          DBG_MCF_FILE,
          // StatPop / StatVar properties (current + past)
          POPULATION_TYPE,
          POPULATION_GROUP,
          LOCATION,
          CONSTRAINT_PROPS,
          MEASURED_PROP,
          STAT_TYPE,
          MEASUREMENT_DENOMINATOR,
          MEASUREMENT_QUALIFIER,
          MEASUREMENT_METHOD,
          SCALING_FACTOR,
          UNIT,
          CENSUS_ACS_TABLE_ID);

  // NOTE: This is an ordered list. When an entity has multiple properties in this
  // list, we prefer the one ordered first.
  public static final List<String> PLACE_RESOLVABLE_AND_ASSIGNABLE_IDS =
      List.of(
          Vocabulary.GEO_ID,
          Vocabulary.ISO_CODE,
          Vocabulary.NUTS_CODE,
          Vocabulary.WIKIDATA_ID,
          Vocabulary.GEO_NAMES_ID,
          Vocabulary.ISTAT_ID,
          Vocabulary.AUSTRIAN_MUNICIPALITY_KEY,
          Vocabulary.INDIAN_CENSUS_AREA_CODE_2011);

  public static boolean isSchemaReferenceProperty(String prop) {
    return prop.equals(TYPE_OF)
        || prop.equals(SUB_CLASS_OF)
        || prop.equals(SUB_PROPERTY_OF)
        || prop.equals(RANGE_INCLUDES)
        || prop.equals(DOMAIN_INCLUDES)
        || prop.equals(SPECIALIZATION_OF)
        || prop.equals(MEMBER_OF);
  }

  public static boolean isReferenceProperty(String prop) {
    return isSchemaReferenceProperty(prop)
        || prop.equals(LOCATION)
        || prop.equals(VARIABLE_MEASURED)
        || prop.equals(OBSERVATION_ABOUT)
        || prop.equals(OBSERVED_NODE)
        || prop.equals(CONTAINED_IN_PLACE)
        || prop.equals(CONTAINED_IN)
        || prop.equals(POPULATION_TYPE)
        || prop.equals(MEASURED_PROP)
        || prop.equals(POPULATION_GROUP)
        || prop.equals(CONSTRAINT_PROPS)
        || prop.equals(MEASUREMENT_METHOD)
        || prop.equals(MEASUREMENT_DENOMINATOR)
        || prop.equals(MEASUREMENT_QUALIFIER)
        || prop.equals(STAT_TYPE)
        || prop.equals(UNIT);
  }

  public static boolean isGlobalReference(String val) {
    return val.startsWith(DCID_PREFIX)
        || val.startsWith(DC_SCHEMA_PREFIX)
        || val.startsWith(SCHEMA_ORG_PREFIX);
  }

  public static boolean isInternalReference(String val) {
    return val.startsWith(INTERNAL_REF_PREFIX);
  }

  public static boolean isStatValueProperty(String val) {
    String lcVal = val.toLowerCase();
    return lcVal.endsWith("value")
        || lcVal.endsWith("estimate")
        || lcVal.endsWith("percentile")
        || lcVal.equals("marginoferror")
        || lcVal.endsWith("stderror")
        || lcVal.endsWith("samplesize")
        || lcVal.endsWith("growthrate");
  }

  public static boolean isStatVar(String type) {
    return type.equals(STAT_VAR_TYPE);
  }

  public static boolean isStatVarObs(String type) {
    return type.equals(STAT_VAR_OBSERVATION_TYPE);
  }

  public static boolean isStatVarGroup(String type) {
    return type.equals(STAT_VAR_GROUP_TYPE);
  }

  public static boolean isLegacyObservation(String type) {
    return type.endsWith(LEGACY_OBSERVATION_TYPE_SUFFIX) && !type.equals(STAT_VAR_OBSERVATION_TYPE);
  }

  public static boolean isPopulation(String type) {
    return type.endsWith(LEGACY_POPULATION_TYPE_SUFFIX);
  }
}
