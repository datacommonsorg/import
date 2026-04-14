# Graph Ingestion Pipeline Explanation

This document explains the structure and stages of the Graph Ingestion Pipeline located in this directory.

## Overview

The `pipeline/ingestion` directory contains an Apache Beam-based pipeline (`GraphIngestionPipeline`) designed to load graph data (nodes, edges, and observations) into Google Cloud Spanner.

## Pipeline Stages

The `GraphIngestionPipeline` processes data in several distinct stages to ensure optimization and data integrity.

### 1. Data Reading and Format Support
The pipeline can read data from two different formats:
*   **MCF (Multi-Column Framework) Text Files**: Standard text files representing graph nodes.
*   **TFRecord Files**: A binary format containing serialized `McfOptimizedGraph` protocol buffers.

The pipeline automatically detects the format based on the file extension (looking for `tfrecord`) and uses the appropriate Beam reader (`TextIO` or `TFRecordIO`).

### 2. Graph Splitting
Once the data is read into memory as `McfGraph` objects, the pipeline splits the stream into two branches based on the content of the nodes:
*   **Schema Nodes**: Nodes representing entities, properties, classifications, etc.
*   **Observation Nodes**: Nodes representing statistical observations (time series data).

This splitting allows for specialized processing for each type of data.

### 3. Schema Node Processing
For the schema branch:
*   **Node Combination**: For specific imports like `Schema` and `Place`, the pipeline combines nodes with the same identifier from different files, merging their properties and removing duplicates.
*   **Mutation Generation**: The combined graph is converted into Spanner `Mutation` objects for the `Node` and `Edge` tables.

### 4. Observation Node Processing (Optimization)
This is a critical stage for performance:
*   **Extraction and Grouping**: The pipeline extracts observations and groups them by a composite key (typically combining the Statistical Variable and the Place).
*   **Sorting and Series Building**: Observations within the same group are sorted by date.
*   **Optimization**: It builds an `McfStatVarObsSeries` which groups all time-series observations for a specific variable and place together. This structure is highly optimized for Spanner storage and querying.
*   **Mutation Generation**: The optimized graph is converted into Spanner `Mutation` objects for the `Observation` table.

### 5. Data Deletion and Write Ordering (Data Integrity)
To ensure clean imports and proper referential integrity, the pipeline manages the execution order carefully:
*   **Pre-import Deletes**: If not explicitly skipped, the pipeline first generates and executes mutations to delete existing data for the specific import being processed from the `Observation` and `Edge` tables.
*   **Ordered Writes**:
    1.  **Nodes** are written to Spanner first.
    2.  **Edges** are written only *after* the Node writes are complete and internal Edge deletes are finished. This ensures edges always point to existing nodes.
    3.  **Observations** are written after the Observation deletes are finished.

## Legacy Pipeline

There is also a legacy pipeline (`IngestionPipeline`) referenced in `README_LegacyPipeline.md`. This version loads all tables for all or specified import groups fetched from a version endpoint. It is more memory-intensive and requires careful tuning of worker memory and CPU ratios on Google Cloud Dataflow to avoid garbage collection issues.
