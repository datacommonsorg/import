# Decoupling Provenance from Data Imports
**Status:** Proposed  
**Author:** dwnoble
**Date:** July 10, 2026  

---

## 1. Objective

This document proposes decoupling the concepts of **Data Imports** and **Provenances** in Data Commons. 

Currently, an import maps 1:1 with a provenance. While these concepts are operationally related, coupling them creates circular definitions in the knowledge graph, limits the granularity of metadata, and creates operational confusion and brittle configuration mappings during the data ingestion and aggregation (merging) phases.

By separating them—tracking **Imports** as transactional/ingestion units at the database level and keeping **Provenances** strictly as logical data source descriptors—we resolve these issues, allow an import to contain multiple provenances, simplify the operational setup, and ensure robust, predictable deletion and aggregation behavior.

---

## 2. Background and Terminology

*   **Provenance:** A logical metadata identifier representing the origin of a schema node or statistical observation. 
    *   For example, all graph edges link to a provenance indicating who defined that edge. The edge `(subject="StatVarObservation", predicate="name", object="Statistical Variable Observation")` is associated with the provenance [`BaseSchema`](https://datacommons.org/browser/dc/base/BaseSchema), indicating it was defined by the Data Commons team.
    *   Similarly, all observations are linked to a provenance indicating who published them. A population count observation for [`Count_Person`](https://datacommons.org/browser/Count_Person) might be defined by the provenance [`CensusACS5YearSurvey_SubjectTables_S0101`](https://datacommons.org/browser/dc/base/CensusACS5YearSurvey_SubjectTables_S0101) (a specific subject table from the US Census).
*   **Data Import (Import):** An operational/transactional unit of ingestion comprising MCF, CSV, or JSON-LD files ingested during a single workflow run (e.g., `USCensus_2020`). It represents the ingestion transaction boundary.

### Current Implementation
Today, when data is ingested, the pipeline assumes a 1:1 mapping between the import name and the provenance ID. 
1.  All `Edge` rows in Spanner are saved with a `provenance` column.
2.  All `TimeSeries` and `Observation` rows in Spanner are saved with a `provenance` attribute inside their `facet` JSON, which is extracted and stored in a indexed column.
3.  Ingestion cleanup (deletion of old data) and post-ingestion aggregations locate data solely by querying the `provenance` column, which is hardcoded/derived from the `importName` argument of the ingestion runner.

---

## 3. Problem Statements

### Issue #1: Circular Schema Definitions of Provenances
A [`Provenance`](https://datacommons.org/browser/Provenance) is itself a schema entity represented as a set of graph edges. If every `Edge` requires a `provenance` to indicate where the edge came from:
*   The provenance record for the [`Provenance`](https://datacommons.org/browser/Provenance) record itself must exist.
*   This forces circular references (e.g., a provenance record pointing to itself as its provenance) or using confusing fallbacks like [`BaseSchema`](https://datacommons.org/browser/dc/base/BaseSchema) for schema metadata. 
*   This circularity is confusing to developers and users navigating the knowledge graph.

### Issue #2: Coarse-Grained Provenance for Operational Convenience
Because an import must map to a single provenance, users are discouraged from declaring fine-grained provenances. 
*   *Example:* If a country's census publishes many different datasets or subject tables (as seen on the [Cote d'Ivoire Open Data Portal](https://cotedivoire.opendataforafrica.org/data/)), it is operationally easier to ingest them under a single import. Consequently, they are all lumped under a single generic provenance [`Cotedivoire_Census`](https://datacommons.org/browser/dc/base/Cotedivoire_Census) instead of mapping each to their specific, precise source tables (which is how the US Census is modeled).
*   Decoupling them allows a single ingestion run (`Import`) to write data for multiple distinct, granular logical sources (`Provenances`) without running separate pipelines.

### Issue #3: Confusing Operational User Experience & Brittle Mappings
The 1:1 coupling between imports and provenances forces developers to align two separate layers: the operational execution layer (the batch job import name) and the logical schema/data layer (provenance ID inside the MCF files). 
*   **Confusing Setup:** Users must define an operational import name (e.g., `USCensus_2020`) and then remember to create a matching provenance record (e.g., `dc/base/USCensus_2020`) inside their MCF files.
*   **Brittle Deletion Boundaries:** If a user specifies custom provenances inside the MCF (e.g., to match fine-grained source tables like `US_Census_ACS_TableS0101`), the ingestion runner will still try to delete old data based on the command-line argument import name. This leads to stale data remaining in Spanner since the DML query `DELETE FROM Edge WHERE provenance = @import_name` misses the custom-named records.
*   **Brittle Aggregation/Merging Mappings:** Post-processing script queries (which filter by `provenance` derived from the import name) will silently fail to find any data (finding 0 rows) because the data was ingested under the MCF's custom provenances.

### Issue #4: Schema Edges' Provenance is De-Facto Operational
For core schema definitions in Data Commons (for example, the class [`StatisticalVariable`](https://datacommons.org/browser/StatisticalVariable) and its properties), the provenance in the graph is universally set to [`BaseSchema`](https://datacommons.org/browser/dc/base/BaseSchema). 
*   **The Theoretical vs. Practical Use:** Theoretically, schema provenance *could* be used logically to distinguish competing schema definitions (e.g., whether Wikidata or Schema.org defined a class name). However, in practice, Data Commons does not use provenance this way today. 
*   **De-Facto Operational Tracking:** Currently, the schema is queried globally as a single, unified type system. We only use the schema edge provenance for operational tracking: knowing which schema file/batch release imported these edges so they can be cleaned up, replaced, or rolled back.
*   **Aligning Code with Reality:** Forcing schema edges to have a "provenance" column in the `Edge` table complicates the schema definition and leads back to Issue #1 (circular references). Rather than designing for a theoretical use case we do not support, we should align our data model with current reality and track schema edge origins using **`import_id`** (e.g., `Schema_Release_v32`), treating them as operational batches.

---

## 4. Proposed Design

We propose decoupling data imports from provenances by:
1.  Using `import_id` for transactional operations (cleanup, deletions, rollbacks).
2.  Using `provenance` strictly for schema metadata and observation sourcing.

### A. Database Schema Changes

#### 1. `Edge` Table
We will add `import_id` as a column and deprecate the operational use of `provenance` on this table.
*   *For Backwards Compatibility:* We will keep the `provenance` column on `Edge` so that existing graph queries do not break. However, this column will only serve as a metadata tag and will not be used to manage deletions or ingestions.
*   *Primary Key Change:* The primary key of the `Edge` table (which is interleaved in `Node`) will include `import_id` instead of `provenance` for write operations, or we keep both in the key for compatibility.
    ```sql
    CREATE TABLE Edge (
      subject_id STRING(1024) NOT NULL,
      predicate STRING(1024) NOT NULL,
      object_id STRING(1024) NOT NULL,
      provenance STRING(1024) NOT NULL, -- Keep for backwards compatibility
      import_id STRING(1024) NOT NULL,   -- Add for operational management
    ) PRIMARY KEY(subject_id, predicate, object_id, provenance, import_id),
      INTERLEAVE IN Node;
    ```

#### 2. `TimeSeries` Table
We will add `import_id` to the `TimeSeries` table as a non-key column.
*   *Why not in the Primary Key?* A single `TimeSeries` (uniquely identified by variable, entity, and facet/provenance) can receive data from multiple imports (e.g., a time series updated with historical data by Import A and current data by Import B). Placing `import_id` in the primary key would split them into separate time series, which is incorrect.
    ```sql
    CREATE TABLE TimeSeries (
      variable_measured STRING(1024) NOT NULL,
      entity1 STRING(1024) NOT NULL,
      extra_entities_id STRING(1024) NOT NULL,
      facet_id STRING(1024) NOT NULL,
      entities JSON NOT NULL,
      facet JSON NOT NULL,
      provenance STRING(1024) NOT NULL,
      import_id STRING(1024) NOT NULL, -- Add as operational tag (latest writing import)
      last_update_timestamp TIMESTAMP NOT NULL,
    ) PRIMARY KEY(variable_measured, entity1, extra_entities_id, facet_id);
    ```

#### 3. `Observation` Table
We will add `import_id` to the `Observation` table as a non-key column.
*   This allows us to identify exactly which observations were added by which import run, enabling safe deletes and rollbacks of individual data points.
    ```sql
    CREATE TABLE Observation (
      variable_measured STRING(1024) NOT NULL,
      entity1 STRING(1024) NOT NULL,
      extra_entities_id STRING(1024) NOT NULL,
      facet_id STRING(1024) NOT NULL,
      date STRING(32) NOT NULL,
      value STRING(MAX) NOT NULL,
      import_id STRING(1024) NOT NULL, -- Add to track ingestion transaction
      last_update_timestamp TIMESTAMP NOT NULL,
    ) PRIMARY KEY(variable_measured, entity1, extra_entities_id, facet_id, date DESC),
      INTERLEAVE IN PARENT TimeSeries ON DELETE CASCADE;
    ```

### B. Operational Pipeline Changes

#### 1. Ingestion Cleanup (Delete Stage)
When the pipeline runs for a specific `import_id` (e.g., `Place`), it deletes existing records using `import_id` instead of `provenance`:
```sql
DELETE FROM Edge WHERE import_id = @import_id;
DELETE FROM Observation WHERE import_id = @import_id;
```
This ensures that all data previously imported by this specific transaction is deleted, regardless of what `provenance` value was written in the nodes or observations.

#### 2. Aggregation & Merging Logic (Using `import_id` vs `provenance`)
To aggregate and merge data, the aggregation pipelines (like `PlaceAggregationGenerator` and `StatVarAggregator`) should query source observations from Spanner by filtering on **`import_id`** (e.g., `import_id = @source_import_id`) rather than `provenance`.

```sql
SELECT ts.variable_measured, ts.entities, ts.facet, obs.date, obs.value, ts.provenance
FROM TimeSeries ts
JOIN Observation obs ON ...
WHERE ts.import_id = @source_import_id
```

**Why this makes sense:**
1.  **Single-Job Processing (Operational):** An import (e.g. `USCensus_2020`) can contain multiple different provenances (e.g. `TableS0101`, `TableS0102`). By querying the source data by `import_id`, we can load all observations from the entire ingestion run in a **single database job**. This simplifies pipeline orchestration because the runner doesn't need to parse the MCF files beforehand to know which provenances were ingested.
2.  **Isolating Data by Provenance (Logical):** The aggregation SQL script will group the observations by `variable_measured, entity1, date` and their respective `provenance`/`facet`. This ensures that we group and sum the observations separately for `TableS0101` and `TableS0102` (producing `TableS0101_Agg` and `TableS0102_Agg` respectively) without mixing different logical data sources.
3.  **Correct Deletions & Tracking:** The aggregated results can be written back to Spanner under a new `import_id` (e.g., `USCensus_2020_Agg`), making them easily cleanable or roll-backable, while their logical `provenance` attributes remain correct.

---

## 5. Configuration Format Changes

We will update the Data Commons Parser (DCP) configuration schema to treat the **entire config file** as the transactional Import, and allow files within it to specify their own metadata provenances.

### Current config.json structure:
Today, every pattern entry in `inputFiles` must specify its own provenance, and they are treated as separate import operations:
```json
{
  "inputFiles": [
    {
      "pattern": "schema.mcf",
      "provenance": "dcid:UN_WHO"
     },
     {
      "pattern": "smokers.csv",
      "provenance": "dcid:UN_WHO",
      "columnMappings": { ... }
    }
  ]
}
```

### Proposed config.json structure:
We introduce `importId` at the root of the configuration. The entire configuration represents a single import unit. We also allow specifying `provenance` at the root level as a default, or overriding it at the file level for granular tracking:

```json
{
  "importId": "UN_WHO_Smokers_Import",
  "provenance": "dcid:UN_WHO_Default", // Default provenance for the import
  "inputFiles": [
    {
      "pattern": "schema.mcf",
      "provenance": "dcid:UN_WHO_Schema" // Override for schema elements
    },
    {
      "pattern": "smokers_single_entity.csv",
      "provenance": "dcid:UN_WHO_Global_Survey", // Specific survey provenance
      "columnMappings": {
        "dcid:variableMeasured": "variable",
        "dcid:observationAbout": "country",
        "dcid:observationDate": "year",
        "dcid:value": "value"
      }
    },
    {
      "pattern": "smokers_multi_entity.csv",
      "columnMappings": {
        "dcid:variableMeasured": "variable",
        "dcid:country": "country",
        "dcid:gender": "sex",
        "dcid:observationDate": "year",
        "dcid:value": "value"
      }
      // Uses root level default: dcid:UN_WHO_Default
    }
  ]
}
```

---

## 6. Benefits

1.  **Supports Granular Provenance:** Ingestion pipelines can now write data to highly specific provenances (e.g., individual survey tables) without needing to configure or execute separate imports for each file.
2.  **Transactional Safety:** Imports can be safely updated, cleaned up, or rolled back in a single operation because they are managed via `import_id`.
3.  **Robust Aggregation/Merging:** Aggregations are no longer vulnerable to missing data caused by mismatches between MCF-defined provenances and input import arguments.
4.  **Cleaner Knowledge Graph:** Avoids circular dependency issues with the schema `Provenance` node itself.

---

## 7. Migration & Transition Strategy

To implement this change without breaking existing deployments:
1.  **DDL Update:** Deploy the Spanner schema updates to add `import_id` to `Edge`, `TimeSeries`, and `Observation` tables.
2.  **Backfill Data:** Run a migration query to populate `import_id` for existing records. Since they were previously mapped 1:1, we can copy the value of `provenance` into `import_id`:
    ```sql
    UPDATE Edge SET import_id = REPLACE(provenance, 'dc/base/', '') WHERE import_id IS NULL;
    UPDATE TimeSeries SET import_id = REPLACE(provenance, 'dc/base/', '') WHERE import_id IS NULL;
    UPDATE Observation SET import_id = REPLACE(facet_id, ...) -- derived from TimeSeries
    ```
3.  **Code Deployment:** Update the Java parser (`GraphReader.java`), Spanner client (`SpannerClient.java`), and Python aggregators (`PlaceAggregationGenerator`, `StatVarAggregator`) to execute deletes, writes, and aggregation queries using `import_id`.
4.  **Deprecate operational provenance queries:** Gradually migrate internal tools away from querying `Edge.provenance` for ingestion lifecycle operations.
