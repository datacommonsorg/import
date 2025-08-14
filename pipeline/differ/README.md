# Import Differ Pipeline

This module implements a Dataflow pipeline that generates a summary of changes between two versions (current and previous) of the same dataset. It does that by performing an outer join on the datasets. It supports both MCF and TFRecord file formats as input. The generated output (CSV) includes a summary of the changes in the dataset.

The pipeline can be run locally (DirectRunner) or in the cloud (DataflowRunner). It supports the following options:

- currentData: Location of the current data (wildcard on local/GCS supported)
- previousData: Location of the previous data (wildcard on local/GCS supported)
- outputLocation: Path to the output data folder (local/GCS)
- useOptimizedGraphFormat: optionally set to use TFRecord file format

Sample command to generate diff for MCF files:

```
mvn compile exec:java  -pl differ -am -Dexec.mainClass=org.datacommons.pipeline.differ.DifferPipeline -Dexec.args="--currentData=gs://bucket/current/*.mcf --previousData=gs://bukcketprevious/*.mcf --outputLocation=gs://bucket/differ/output"
```

Sample command to generate diff for tfrecord graph files:
```
mvn compile exec:java  -pl differ -am -Dexec.mainClass=org.datacommons.pipeline.differ.DifferPipeline -Dexec.args="--currentData=gs://bucket/current/*.gz --previousData=gs://bucket/previous/*.gz --outputLocation=gs://bucket/differ/output --useOptimizedGraphFormat=true" 
```

The differ output includes the following information:

- key_combined: GroupBy key used for performing the diff operation. For schema diff, it includes dcid of the node (value of the Node property). For observations, it includes the following property values {variableMeasured, observationAbout, observationDate, observationPeriod, measurementMethod, unit, scalingFactor} separated by a ';'.
- value_combined_current: Node value in the current dataset. For observations, it includes the value of the property 'value'. For schema nodes, it includes all the property and values (except 'Node' property) in property:value format separated by a ';'
- value_combined_previous: Node value in the previous dataset. Same format as above.
- diff_type: one of (ADDED,DELETED,MODIFIED)

Sample observation diff output

```
key_combined,value_combined_current,value_combined_previous,diff_type
dcid:InterestRate_TreasuryBond_20Year;dcid:country/USA;"2025-01-30";;dcid:ConstantMaturityRate;dcid:Percent;,4.85,4.81,MODIFIED
dcid:InterestRate_TreasuryNote_10Year;dcid:country/USA;"2025-01-31";;dcid:ConstantMaturityRate;dcid:Percent;,4.58,,ADDED
dcid:InterestRate_TreasuryBill_3Month;dcid:country/USA;"2025-01-30";;dcid:ConstantMaturityRate;dcid:Percent;,,4.30,DELETED
```

Sample schema diff output
```
key_combined,value_combined_current,value_combined_previous,diff_type
dcid:InterestRate_TreasuryBill_1Month,,maturity:[1 Month];measuredProperty:dcs:interestRate;name:"InterestRate_TreasuryBill_1Month";populationType:dcs:TreasuryBill;statType:dcs:measuredValue;typeOf:dcs:StatisticalVariable,DELETED
dcid:InterestRate_TreasuryNote_3Year,maturity:[3 Year];measuredProperty:dcs:interestRate;name:"InterestRate_TreasuryNote_3Year";populationType:dcs:TreasuryNote;statType:dcs:measuredValue;typeOf:dcs:StatisticalVariable,,ADDED
```