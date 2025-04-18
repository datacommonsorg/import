# Data Differ Pipeline

This module implements a Dataflow pipeline that generates a summary of changes between two versions (current and previous) of the same dataset. It does that by performing an outer join on the datasets. It supports both MCF and TFRecord file formats as input. The generated output (CSV) includes the following information:

- Stat var: CSV string containing the property values in the observation in the order {variableMeasured, observationAbout, observationDate, observationPeriod, measurementMethod, unit, scalingFactor}
- Curent value: value in the current dataset
- Previous value: value in the previous dataset
- Diff type: one of (ADDED,DELETED,MODIFIED)

The pipeline can be run locally (DirectRunner) or in the cloud (DataflowRunner). It supports the following options:

- currentData: Location of the current data (wildcard on local/GCS supported)
- previousData: Location of the previous data (wildcard on local/GCS supported)
- outputLocation: Path to the output data folder (local/GCS)
- useOptimizedGraphFormat: optionally set to use TFRecord file format


```
mvn compile exec:java  -pl differ -am -Dexec.mainClass=org.datacommons.DifferPipeline -Dexec.args="--currentData=current*.gz --previousData=previous*.gz --useOptimizedGraphFormat=true i--outputLocation=gs://bucket/differ/output"
```

