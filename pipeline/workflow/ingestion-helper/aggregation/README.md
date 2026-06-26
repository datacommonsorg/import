# Aggregations

This module orchestrates the execution of Data Commons aggregations through BigQuery Federation. The aggregations include place rollups, statistical variable aggregations, linked edges, and metadata summaries.

## Core Concepts

*   **Sequential Stages**: Aggregations are executed sequentially by their `stage` number (e.g., Stage 1 steps are guaranteed to complete before Stage 2 steps begin). This is useful when later steps depend on the output of earlier ones.
*   **Parallel Execution**: All aggregation steps configured in the same stage are executed in parallel to maximize performance.

---

## Configuration Guide (`aggregation.yaml`)

The entire aggregation pipeline is configured via `aggregation.yaml`. This file defines which aggregations run, what their dependencies are, and in what order they execute.

### Common Configuration Fields
Every step in the configuration supports these common fields:
*   `type` (string, Required): The type of aggregation step to run.
*   `stage` (integer, Optional, default: 1): The sequential stage number. Steps in lower stages are guaranteed to finish before higher stages start.
*   `imports` (list of strings, Required): The list of import names this step applies to. Use `["*"]` (wildcard) to apply the step to **all** imports in the current run.
*   `disabled` (boolean, Optional, default: false): Set to `true` to temporarily disable a step without deleting it.

---

### Supported Aggregation Types

#### 1. Place (`place`)
Aggregates and rolls up statistical data from a smaller place type (source) to a larger place type (destination).
*   **Fields**:
    *   `source_type` (string, Required): The source place type (e.g., `County`).
    *   `destination_type` (string, Required): The destination place type (e.g., `State`).
    *   `allow_multiple_to_places` (boolean, Optional, default: false): Allows mapping to multiple parent places if true.
*   **Example**:
    ```yaml
    - type: place
      stage: 1
      imports: ["USFed_Census"]
      source_type: County
      destination_type: State
    ```

#### 2. Statistical Variable Aggregation (`stat_var`)
Aggregates raw statistical variables into a summarized ancestor variable (e.g., summing up individual age group counts to get a total population count).
*   **Fields**:
    *   `ancestor_sv_id` (string, Required): The ID of the parent/summary statistical variable (e.g., `Count_Person`).
    *   `source_sv_ids` (list of strings, Required): The list of individual statistical variables to sum up.
    *   `output_import_name` (string, Optional): Custom import name to write output under.
    *   `skip_all_sources_present_check` (boolean, Optional, default: false): If true, aggregates even if some source variables are missing.
*   **Example**:
    ```yaml
    - type: stat_var
      stage: 2
      imports: ["USFed_Census"]
      ancestor_sv_id: Count_Person
      source_sv_ids:
        - Count_Person_Male
        - Count_Person_Female
    ```

#### 3. Linked Edges (`linked_edges`)
Constructs and aggregates structural graph links (edges) between nodes in the Data Commons graph.
*   **Example**:
    ```yaml
    - type: linked_edges
      stage: 1
      imports: ["*"] # Runs for all imports
    ```

#### 4. Provenance Summary (`provenance_summary`)
Generates metadata and provenance summaries for all aggregated statistical observations, establishing data lineage.
*   **Example**:
    ```yaml
    - type: provenance_summary
      stage: 3
      imports: ["USFed_Census"]
    ```

#### 5. Statistical Variable Groups (`stat_var_groups`)
Aggregates and structures statistical variables into hierarchical groups for display in the Data Commons UI.
*   **Example**:
    ```yaml
    - type: stat_var_groups
      stage: 3
      imports: ["*"]
    ```

---

### Example `aggregation.yaml`

This example demonstrates a typical multi-stage aggregation workflow.

```yaml
# aggregation.yaml
aggregations:
  # Stage 1: Parallel Place Rollups and Linked Edges
  - type: linked_edges
    stage: 1
    imports: ["*"]

  - type: place
    stage: 1
    imports: ["USFed_Census"]
    source_type: County
    destination_type: State

  # Stage 2: Parallel Stat Var Aggregations (Depends on Stage 1 completing)
  - type: stat_var
    stage: 2
    imports: ["USFed_Census"]
    ancestor_sv_id: Count_Person
    source_sv_ids:
      - Count_Person_Male
      - Count_Person_Female

  # Stage 3: Metadata and UI Summaries (Depends on Stage 2 completing)
  - type: provenance_summary
    stage: 3
    imports: ["USFed_Census"]

  - type: stat_var_groups
    stage: 3
    imports: ["*"]
```

---

## Local Configuration Validation

The orchestrator strictly validates the `aggregation.yaml` file on startup against a strict JSON Schema (`schema.json`). If there is any syntax error, type mismatch, or missing required field, the service will fail to start.

### Running the Validator Locally
You can validate your `aggregation.yaml` file locally using the built-in CLI tool before committing or deploying changes.

1.  **Navigate to the ingestion-helper root**:
    ```bash
    cd pipeline/workflow/ingestion-helper
    ```
2.  **Run the validator**:
    ```bash
    python3 -m aggregation.validator --config ../aggregation.yaml

    # sample output...
    # Validating 'aggregation.yaml' against 'schema.json'...
    # [SUCCESS] Configuration is valid!
    # Parsed 5 aggregation steps successfully.
    ```
