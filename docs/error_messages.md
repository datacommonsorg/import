# Error Counters

Issues found in the input files are categorized under "counters". Counters
aggregate the issues, and provide a high-level overview of what went wrong.

Error counters with "**Suffix Description:**" are prefix counters that are
logged with a suffix detailing the subject of the error. The For example,
`Resolution_UnresolvedExternalId_` will be suffixed with the ID that could not 
be resolved. The "**Suffix Description:**" field describes the nature of the
suffix for these counters.

## MCF_UnenclosedComplexValue

**Description:** Complex value was not enclosed in brackets `[]` in the MCF.

### Suggested User Actions

1. Check complex values in your MCF and make sure they are enclosed in brackets
1. Refer to the [documentation of valid complex values](complex_values.md)

## MCF_MalformedComplexValueParts

**Description:** Complex value had less than 2 or more than 3 parts

### Suggested User Actions

1. Check that all of your complex values have either 2 or 3 parts
1. Refer to the [documentation of valid complex values](complex_values.md)

## MCF_QuantityMalformedValue

**Description:** In a complex value with 2 parts, the part that was expected to be a number was not a number

### Suggested User Actions

1. Refer to the [documentation of valid complex values](complex_values.md)

## MCF_InvalidLatitude

**Description:** Invalid latitude part in complex value; latitude must be decimal degrees with an optional N/S suffix

### Suggested User Actions

1. Refer to the [documentation of valid complex values](complex_values.md)

## MCF_InvalidLongitude

**Description:** Invalid longitude part in complex value; longitude must be decimal degrees with an optional E/W suffix

### Suggested User Actions

1. Refer to the [documentation of valid complex values](complex_values.md)

## MCF_QuantityRangeMalformedValues

**Description:** An unexpected part was found in the complex value in MCF, error message will specify the type of issue

### Suggested User Actions

1. Either the start or end components are wrong, or at least one of the components have to be a number. Check the error message in `report.json`.
1. Refer to the [documentation of valid complex values](complex_values.md)

## Resolution_UnresolvedExternalId_

**Description:** External ID reference could not be resolved.

**Suffix Description:** External ID reference could not be resolved.

### Suggested User Actions

1. Try searching for the ID on the [Data Commons Browser](https://datacommons.org/search)
1. Ensure you have added all new schema files as inputs to the import tool

## Resolution_DivergingDcidsForExternalIds_

**Description:** Resolving external IDs found different DCIDs.

**Suffix Description:** Resolving external IDs found different DCIDs.

### Suggested User Actions

1. Try searching for the DCIDs on the [Data Commons Browser](https://datacommons.org/search) and/or in your local schema (.mcf) files

## Mutator_MissingTypeOf

**Description:** MCF node missing required [typeOf](https://datacommons.org/browser/typeOf) property

## Mutator_InvalidObsValue

**Description:** Observation value must be either a number or text

## MCF_MalformedColonLessLine

**Description:** MCF line was missing a colon to separate the property and the value

### Suggested User Actions

1. Ensure that the lines in your MCF are in the form `<property>: <value>

## MCF_MalformedNodeName

**Description:** Value of `Node` prop either included a comma or started with a quote.

### Suggested User Actions

1. Check the error message for the specifics of the malformation.

## TMCF_MalformedEntity

**Description:** When parsing the first (`Node: <value>`) line of a node in TMCF, the value did not have the required E: prefix to be an entity name

## MCF_UnexpectedProperty

**Description:** A regular `<property>: <value>` line was found without a preceding `Node` line to associate with

## MCF_MalformedNode

**Description:** Found a 'Node' without properties, or the value of the `Node` property was surrounded by quotes (must be non-quoted), or the value of the `Node` property included a comma (must be a unary value)

## Resolution_IrreplaceableLocalRef

**Description:** Unable to replace a local reference

### Suggested User Actions

1. This is likely a cycle of local references, which the import tool is not able to resolve. Check your MCF files for potential cycles.

## Resolution_UnassignableNodeDcid

**Description:** Unable to assign DCID due to unresolved local reference

### Suggested User Actions

1. See actions for `Resolution_IrreplaceableLocalRef`

## Resolution_DcidAssignmentFailure_

**Description:** The node could not be assigned a DCID based on the data available

**Suffix Description:** The node could not be assigned a DCID based on the data available

## Resolution_OrphanLocalReference_

**Description:** The local ID of the node is missing from the entire sub-graph

**Suffix Description:** The local ID of the node is missing from the entire sub-graph

## Resolution_ReferenceToFailedNode_

**Description:** The reference was resolved, but to a failed node, therefore, this node was also failed to resolve

**Suffix Description:** The reference was resolved, but to a failed node, therefore, this node was also failed to resolve

## Sanity_InconsistentSvObsValues

**Description:** Found nodes with different values for the same StatVarObservation

## Sanity_SameDcidForDifferentStatVars

**Description:** Found nodes with different values for the same StatVarObservation

## Sanity_DifferentDcidsForSameStatVar

**Description:** Found different curated IDs for same StatVar

## CSV_HeaderFailure

**Description:** Unable to parse header from CSV file

## CSV_TmcfCheckFailure

**Description:** There was a fatal sanity error in TMCF

### Suggested User Actions

1. Check counter messages that start with Sanity_ for FATAL error level

## Sanity_TmcfMissingEntityDef

**Description:** No definition found for a referenced 'E:' value

## Sanity_UnexpectedNonColumn

**Description:** Expected value to be a TMCF column that starts with 'C:', but did not find a column.

## Sanity_TmcfMissingColumn

**Description:** Column referred to in TMCF is missing from CSV header

## Sanity_UnknownStatType

**Description:** Found an unknown statType value

## Sanity_InvalidObsDate

**Description:** Found a non-ISO8601 compliant date value

### Suggested User Actions

1. ISO8601 is in the format YYYY-MM-DD (e.g. 2020-07-10), optionally with time of day appended.

## Sanity_NonDoubleObsValue

**Description:** Found an Observation value that was not a number.

## Sanity_ObsMissingValueProp

**Description:** Observation node missing value property

## Sanity_EmptyProperty

**Description:** An empty property (property with no text) was found

## Sanity_NotInitLowerPropName

**Description:** Found property name that does not start with a lower-case

## Sanity_MultipleDcidValues

**Description:** Found dcid with more than one value

## Sanity_DcidTableEntity

**Description:** Value of DCID property was an 'E:' reference in TMCF, which is invalid

## Sanity_VeryLongDcid

**Description:** Found a DCID that was too long

## Sanity_NonAsciiValueInNonText

**Description:** Found non-ASCII characters in a value which was not a text (a text value is a value surrounded by quotes)

## Sanity_RefPropHasNonRefValue

**Description:** Found text/numeric value in a property where the value is expected to be a reference

## Sanity_InvalidChars_

**Description:** DCID included invalid characters.

**Suffix Description:** DCID included invalid characters.

## Sanity_UnexpectedPropIn

**Description:** A property was found that was not expected for the type of the Node

**Suffix Description:** A property was found that was not expected for the type of the Node

## Sanity_EmptySchemaValue

**Description:** Found empty value for a property

## Sanity_NonAsciiValueInSchema

**Description:** Schema node has property values with non-ascii characters

## Sanity_DcidNameMismatchInSchema

**Description:** The name and the DCID of Schema nodes must match, but this node did not satisfy this requirement

## Sanity_MissingOrEmpty_

**Description:** Found a missing or empty property value

**Suffix Description:** Found a missing or empty property value

## Sanity_MultipleVals_

**Description:** Found multiple values for single-value property

**Suffix Description:** Found multiple values for single-value property

## Sanity_NotInitUpper_

**Description:** Found a class reference that does not start with an upper-case

**Suffix Description:** Found a class reference that does not start with an upper-case

## Sanity_NotInitLower_

**Description:** Found a property reference that does not start with a lower-case

**Suffix Description:** Found a property reference that does not start with a lower-case

## CSV_InconsistentRows

**Description:** A CSV row had different number of columns from the rest of the file

## CSV_MalformedDCIDFailures

**Description:** Malformed CSV value for dcid property; must be a text or reference

## TMCF_TmcfEntityAsDcid

**Description:** In TMCF, value of DCID was an E: entity. Must be a C: column or a constant value instead.

## CSV_EmptyDcidReferences

**Description:** A reference in the form of dcid:{entity} was detected, but {entity} was empty

## TMCF_UnexpectedNonColumn

**Description:** Expected value to be a TMCF column that starts with 'C:' :: value, but it was not.

## CSV_TmcfMissingColumn

**Description:** Column referred to in TMCF is missing from CSV header

## CSV_UnexpectedRow

**Description:** Found row with fewer columns than expected

## Existence_FailedDcCall

**Description:** Network request to DataCommons API failed

## Existence_MissingReference

**Description:** External reference existence check with the DataCommons API returned no results

## Existence_MissingTriple

**Description:** External triple existence check with the DataCommons API returned no results

## TMCF_UnsupportedColumnNameInProperty

**Description:** TMCF properties as references to CSV columns are not supported yet

## MCF_MalformedComplexValue

**Description:** Found malformed Complex value without a closing ] bracket

## MCF_LocalReferenceInResolvedFile

**Description:** Found an internal 'l:' reference in resolved entity value

## TMCF_MalformedSchemaTerm

**Description:** TMCF had a malformed entity/column; the value must have a '->' delimeter but this was not found