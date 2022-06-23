# Counters

Issues found in the input files are categorized under "counters" in `report.json`
and `summary_report.html`. Counters aggregate the issues, and provide a
high-level overview of what went wrong.

Counters with "Suffix Description" have an additional suffix that details the
subject of the error. For example, `Resolution_UnresolvedExternalId_` will be
suffixed with the ID property (like `isoCode`) that could not be resolved. The
"Suffix Description" field describes the nature of the suffix for these counters.

## CSV, MCF, and TMCF Counters
These counters are logged from parsing the corresponding types of files.

### CSV_InconsistentRows

A CSV row had different number of columns from the rest of the file.

### CSV_MalformedDCIDFailures

Malformed CSV value for dcid property; must be a text or reference.

### CSV_EmptyDcidReferences

A reference in the form of `dcid:<entity>` was detected, but `<entity>` was empty.

**Suggested User Actions:**

1. Look at the file location in the `report.json` to find a line that looks like:
    ```
    <property>: dcid:
    ```

### CSV_TmcfMissingColumn

Column referred to in TMCF is missing from CSV header. 

Column references are TMCF values that look like `C:<table>-<CSVColumnName>`,
please ensure that "`CSVColumnName`" exists in the input CSV. 

### CSV_UnexpectedRow

Found a row in input CSV with fewer columns than expected.

The number of columns to expect is the number of columns that exist in the header.

**Suggested User Actions:**

1. Ensure that your CSV shape is uniform (every row has the same number of columns).

### CSV_HeaderFailure

Unable to parse header from CSV file.

**Suggested User Actions:**

1. Check for malformation in the headers of input CSV files.

### CSV_TmcfCheckFailure

There was a fatal sanity error in TMCF.

**Suggested User Actions:**

1. Check counter messages that start with Sanity_ for FATAL error level.

### MCF_UnenclosedComplexValue

[Complex value](complex_values.md) was not enclosed in brackets `[]` in the MCF.

**Suggested User Actions:**

1. Check complex values in your MCF and make sure they are enclosed in brackets.
1. Refer to the [documentation of valid complex values](complex_values.md).

### MCF_MalformedComplexValueParts

[Complex value](complex_values.md) had less than 2 or more than 3 parts.

**Suggested User Actions:**

1. Check that all of your complex values have either 2 or 3 parts.

### MCF_QuantityMalformedValue

In a [complex value](complex_values.md) with 2 parts, the part that was expected to be a number was not a number.

### MCF_InvalidLatitude

Invalid latitude part in [complex value](complex_values.md); latitude must be decimal degrees with an optional N/S suffix.

### MCF_InvalidLongitude

Invalid longitude part in [complex value](complex_values.md); longitude must be decimal degrees with an optional E/W suffix.

### MCF_QuantityRangeMalformedValues

An unexpected part was found in the [complex value](complex_values.md) in MCF, error message will specify the type of issue.

**Suggested User Actions:**

1. Either the start or end components are wrong, or at least one of the components have to be a number. Check the error message in `report.json`.

### MCF_MalformedColonLessLine

MCF line was missing a colon to separate the property and the value.

**Suggested User Actions:**

1. Ensure that the lines in your MCF are in the form `<property>: <value>`.

### MCF_MalformedNodeName

Value of `Node` prop either included a comma or started with a quote.

**Suggested User Actions:**

1. Check the error message for the specifics of the malformation.

### MCF_UnexpectedProperty

A regular `<property>: <value>` line was found without a preceding `Node` line to associate with.

### MCF_MalformedNode

Either;
1. Found a 'Node' without properties, or
1. The value of the `Node` property was surrounded by quotes (must be non-quoted), or
1. The value of the `Node` property included a comma (must be a unary value).

### MCF_MalformedComplexValue

Found malformed [complex value](complex_values.md) without a closing bracket (`]`).

### MCF_LocalReferenceInResolvedFile

Found an internal `l:` reference in resolved entity value.

### TMCF_MalformedEntity

When parsing the first (`Node: <value>`) line of a node in TMCF, the value did
not have the required `E:` prefix to be an entity name.

### TMCF_MalformedSchemaTerm

TMCF had a malformed entity/column; the value must have a `->` delimeter that was missing.

### TMCF_UnsupportedColumnNameInProperty

A TMCF property referencing a CSV column was found. This is not supported yet.

### TMCF_TmcfEntityAsDcid

In TMCF, value of DCID was an `E:` entity. However, this must instead be a `C:`
column or a constant value.

### TMCF_UnexpectedNonColumn

Expected value to be a TMCF column that starts with a `C:` value, but it was not.

## Resolution Counters
These counters are logged when there are errors assigning DCIDs to each node in
the graph.

### Resolution_UnresolvedExternalId_

External ID reference could not be resolved.

**Suffix Description:** Property for which the ID could not be resolved.

**Suggested User Actions:**

1. Try searching for the ID on the [Data Commons Browser](https://datacommons.org/search).

### Resolution_DivergingDcidsForExternalIds_

External IDs resolved to different DCIDs, however, they must all map to the same DCID.

**Suffix Description:** The properties that were found, separated by an underscore `_`.
For example, a counter named `Resolution_DivergingDcidsForExternalIds_isoCode_wikidataId`
means that the `isoCode` and `wikidataId` properties were both external IDs, but
they resolved to different DCIDs (which is not permitted).

**Suggested User Actions:**

1. Try searching for the DCIDs on the [Data Commons Browser](https://datacommons.org/search) and/or in your local schema (.mcf) files, and making sure they resolve to the correct and identical entity as
uniquely identified by its DCID.

### Resolution_IrreplaceableLocalRef

Unable to replace a local reference.

This is likely a cycle of local references, which the import tool is not able to resolve. 

**Suggested User Actions:**

1. Check your MCF files for potential cycles.

### Resolution_UnassignableNodeDcid

Unable to assign DCID due to an unresolved local reference.

**Suggested User Actions:**

1. See [`Resolution_IrreplaceableLocalRef`](#Resolution_IrreplaceableLocalRef).

### Resolution_DcidAssignmentFailure_

The node could not be assigned a DCID based on the data available.

The tool can generate DCID for;
- StatVarObs,
- legacy population types (type ends with `Population`),
- legacy observation types (type ends with `Observation` and is not `StatVarObservation`);

or if there is an external ID resolver provided.

**Suffix Description:** The typeOf value of the node (first value, if multiple).

**Suggested User Actions:**

1. If none of the conditions in the description apply to your node, provide a non-empty DCID for the node.

### Resolution_ReferenceToFailedNode_

The reference was resolved, but it was to a failed node, therefore, this node
is also marked as a failure.

**Suffix Description:** The property this reference was found in.

**Suggested User Actions:**

1. Check the logs for the failure of the node identified the error message and address that issue.

## Sanity Counters
These counters log issues raised from sanity checks of nodes against a simple
set of assumptions expected of DC nodes.

### Sanity_InconsistentSvObsValues

Found different values provided for the same `StatVarObservation`.

**Suggested User Actions:**

1. Check for any duplication in the input CSV.

### Sanity_SameDcidForDifferentStatVars

The same curated DCID was found for different StatVars.

**Suggested User Actions:**

1. Ensure that StatVars have distinct names.

### Sanity_DifferentDcidsForSameStatVar

Found different curated IDs for same StatVar.

**Suggested User Actions:**

1. Ensure that the StatVars have consistent DCIDs.

### Sanity_TmcfMissingEntityDef

An node was references using an entity (`E:`) reference in TMCF, but this node was not found in the parsed graph.

### Sanity_UnexpectedNonColumn

Expected value to be a TMCF column that starts with `C:`, but did not find such a value.

### Sanity_TmcfMissingColumn

Column referred to in TMCF is missing from CSV header.

**Suggested User Actions:**

1. Check that the TMCF references with `C:` match the names of the columns in the header line in your CSV.

### Sanity_UnknownStatType

Found an unknown statType value.

StatTypes values either:
- end with one of {`value`, `estimate`, `stderror`, `samplesize`, `growthrate`}, or
- start with `percentile`, or
- equal any one of {`marginoferror`, `measurementResult`}.

### Sanity_InvalidObsDate

Found a non-ISO8601 compliant date value.

**Suggested User Actions:**

1. [ISO8601](https://en.wikipedia.org/wiki/ISO_8601) is in the format YYYY-MM-DD (e.g. 2020-07-10), optionally with time of day appended.

### Sanity_NonDoubleObsValue

Found an `StatVarObservation` node with a value that was not a number.

### Sanity_ObsMissingValueProp

`StatVarObservation` node is missing the required `value` property.

### Sanity_EmptyProperty

An empty property (property with no text) was found.

**Suggested User Actions:**

1. Try searching your input files for a line that starts with a colon (`:`).

### Sanity_NotInitLowerPropName

Found property name that does not start with a lower-case. All property names
must start with a lower-case letter.

### Sanity_MultipleDcidValues

The value of the `dcid` property had more than one value.

### Sanity_DcidTableEntity

Value of the `dcid` property was an `E:` reference in TMCF, which is invalid.

### Sanity_VeryLongDcid

Found a DCID that was too long. In the current configuration, the maximum allowed
length of DCID is 256 characters.

### Sanity_NonAsciiValueInNonText

Found non-ASCII characters in a value which was not a text.

A text value is a value surrounded by quotes.

### Sanity_RefPropHasNonRefValue

Found text/numeric value in a property where the value is expected to be a reference.

### Sanity_InvalidChars_

DCID reference included invalid characters.

**Suffix Description:** The property whose value included invalid chars.

### Sanity_UnexpectedPropIn

A property was found that was not expected for the type of the Node.

**Suffix Description:** The type of the node.

### Sanity_EmptySchemaValue

Found empty value for a property.

### Sanity_NonAsciiValueInSchema

Schema node has property values with non-ascii characters.

### Sanity_DcidNameMismatchInSchema

The name and the DCID of Schema nodes must match, but this node did not satisfy this requirement.

### Sanity_MissingOrEmpty_

Found a missing or empty property value.

**Suffix Description:** The required property that was missing from this node.

### Sanity_MultipleVals_

Found multiple values for single-value property.

**Suffix Description:** The property with the multiple values.

### Sanity_NotInitUpper_

Found a class reference that does not start with an upper-case.

**Suffix Description:** The property, and optionally, the type of the node separated with an underscore (`_`) from the property.

### Sanity_NotInitLower_

Found a property reference that does not start with a lower-case.

**Suffix Description:** The property, and optionally, the type of the node separated with an underscore (`_`) from the property.

## Existence Counters

Existence counters are logged for issues relating to the existence check of references
against Data Commons.

### Existence_FailedDcCall

Network request to DataCommons API failed.

### Existence_MissingReference

External reference existence check with the DataCommons API returned no results.

### Existence_MissingTriple

External triple existence check with the DataCommons API returned no results.

## Stats Check Counters
These counters represent potential issues found in the statistical analysis of the input data for pitfalls such as extreme outliers, holes in dates that the data is available for, etc.

### StatsCheck_Inconsistent_Values

Two different values were found for the same StatVarObservation.

### StatsCheck_3_Sigma

A datapoint with a value farther than 3 standard deviations (sigma) to the mean of the series was found.

### StatsCheck_MaxPercentFluctuationGreaterThan500 and StatsCheck_MaxPercentFluctuationGreaterThan100

These two stat counters look at adjacent datapoints in each 
timeseries, and reports a log if any two adjacent values are more 
than 100% (or 500%) different.

Note that only the largest difference
in each bucket will be logged.

### StatsCheck_Invalid_Date

This counter will be logged if the date could not be parsed as an
ISO8601 string.

Please check that your dates are formatted according to the [ISO 8601 standard](https://en.wikipedia.org/wiki/ISO_8601).

### StatsCheck_Inconsistent_Date_Granularity

This stats check logs a counter when the timeseries have datapoints with varying date lenghts. For example, if 9 points in a timeseries are monthly (in the form `yyyy-MM`), but another point is a day (`yyyy-MM-dd`), this counter will be logged.

The problematic datapoints that will be logged in `report.json` are those with the less common date length.

### StatsCheck_Data_Holes

This stats check considers the gaps between adjacent datapoint dates.
If any two adjacent datapoints have a different gap than the rest of the dataset, this flag is raised.

Currently, the tool only checks for inconsistent gaps in the unit of months.

## Mutator Counters

Mutation is a step of MCF parsing where e.g. complex values are expanded.

### Mutator_MissingTypeOf

MCF node missing required [typeOf](https://datacommons.org/browser/typeOf) property.

### Mutator_InvalidObsValue

Observation value must be either a number or text.
