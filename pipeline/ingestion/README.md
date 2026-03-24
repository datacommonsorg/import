# Graph Ingestion Pipeline

The `GraphIngestionPipeline` is an Apache Beam-based ingestion pipeline designed to load graph data (nodes, edges, and observations) into Google Cloud Spanner.

## Key Features

- **Multi-Import Support**: Processes a list of imports and their respective graph data paths in a single execution.
- **Data Formats**: Automatically detects and handles both MCF (multi-line) and TFRecord file formats.
- **Automated Schema Management**: Automatically creates the necessary Spanner database and tables (`Node`, `Edge`, `Observation`) if they do not exist.
- **Intelligent Processing**:
  - Splits graph data into schema and observation nodes.
  - Combines schema nodes for specific imports (e.g., `Schema`, `Place`).
  - Optimizes observation graphs for efficient Spanner storage.
  - Transforms nodes before ingestion.
- **Data Integrity**: Manages data deletion for re-imports and ensures proper write ordering (writing nodes before edges).

## Pipeline Configuration

The pipeline is configured using `IngestionPipelineOptions`. Key options include:

- `--projectId`: GCP project ID (default: `datcom-store`).
- `--spannerInstanceId`: Spanner Instance ID (default: `dc-kg-test`).
- `--spannerDatabaseId`: Spanner Database ID (default: `dc_graph_5`).
- `--importList`: A JSON array of objects specifying the imports to process. Each object must contain `importName` and `graphPath`.
- `--skipDelete`: Boolean flag to skip deleting existing data for the specified imports (default: `false`).
- `--numShards`: Number of shards for writing mutations to Spanner (default: `1`).
- `--spannerNodeTableName`: Name of the Spanner Node table (default: `Node`).
- `--spannerEdgeTableName`: Name of the Spanner Edge table (default: `Edge`).
- `--spannerObservationTableName`: Name of the Spanner Observation table (default: `Observation`).

## Example Usage

First, ensure all dependencies are installed locally. After cloning the datacommons/import repository, run the following command from the root directory:

```bash
mvn clean install
```

To run the pipeline locally using the Direct runner, cd to the `pipeline/ingestion` directory and run:

```bash
mvn -Pdirect-runner compile exec:java \
  -pl ingestion -am \
  -Dexec.mainClass=org.datacommons.ingestion.pipeline.GraphIngestionPipeline \
  -Dexec.args="--projectId=YOUR_PROJECT_ID \
    --spannerInstanceId=YOUR_INSTANCE_ID \
    --spannerDatabaseId=YOUR_DATABASE_ID \
    --importList='[{\"importName\": \"Schema\", \"graphPath\": \"gs://path/to/schema/mcf/\"}, {\"importName\": \"SampleImport\", \"graphPath\": \"gs://path/to/data.tfrecord\"}]' \
    --runner=DirectRunner"
```

To run the pipeline using the Dataflow runner:

```bash
mvn -Pdataflow-runner compile exec:java \
  -pl ingestion -am \
  -Dexec.mainClass=org.datacommons.ingestion.pipeline.GraphIngestionPipeline \
  -Dexec.args="--projectId=YOUR_PROJECT_ID \
    --spannerInstanceId=YOUR_INSTANCE_ID \
    --spannerDatabaseId=YOUR_DATABASE_ID \
    --importList='[{\"importName\": \"Schema\", \"graphPath\": \"gs://path/to/schema/mcf/\"}, {\"importName\": \"SampleImport\", \"graphPath\": \"gs://path/to/data.tfrecord\"}]' \
    --runner=DataflowRunner \
    --region=us-central1 \
    --numWorkers=10 \
    --maxNumWorkers=20"
```

## Input Format for `--importList`

The `--importList` argument expects a JSON string representing an array of import configurations.

Example:
```json
[
  {
    "importName": "Schema",
    "graphPath": "gs://datcom-store/graph/schema/"
  },
  {
    "importName": "Place",
    "graphPath": "gs://datcom-store/graph/place/place.tfrecord"
  }
]
```
