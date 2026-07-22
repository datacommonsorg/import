# Aggregations

This module orchestrates the execution of Data Commons aggregations through BigQuery Federation. The aggregations include place rollups, statistical variable aggregations, entity aggregations, linked edges, and metadata summaries.

## Core Concepts

*   **Sequential Stages**: Aggregations are executed sequentially by their `stage` number (e.g., Stage 1 steps are guaranteed to complete before Stage 2 steps begin). This is useful when later steps depend on the output of earlier ones.
*   **Parallel Execution**: All aggregation steps configured in the same stage are executed in parallel to maximize performance.
*   **Per-Import Isolation**: Aggregations run independently for each active import dataset.

---

## Configuration Guide (`configs/*.yaml`)

The aggregation pipeline is configured via YAML files in the `configs/` directory (`place.yaml`, `statvar.yaml`, `common.yaml`, etc.). Each file defines a top-level `calculations:` list.

### Common Configuration Fields
Every calculation step supports these common fields:
*   `type` (string, Required): The type of calculation step (e.g. `PLACE_AGGREGATION`, `STAT_VAR_AGGREGATION`, `LINKED_EDGES`).
*   `stage` (integer, Optional, default: 1): The sequential stage number. Steps in lower stages are guaranteed to finish before higher stages start.
*   `input_imports` (list of strings, Required): The list of import names this step applies to. Use `["*"]` (wildcard) to apply the step to **all** active imports.
*   `output_import` (string, Optional): The output import dataset name to write aggregated observations under.
*   `disabled` (boolean, Optional, default: false): Set to `true` to temporarily disable a step without deleting it.

---

### Supported Calculation Types

#### 1. Place Aggregation (`PLACE_AGGREGATION`)
Aggregates and rolls up statistical data from a smaller place type (source) to a larger place type (destination).
*   **Fields**:
    *   `from_place_types` (string, Required): The source place type (e.g., `State`).
    *   `to_place_types` (string, Required): The destination place type (e.g., `Country`).
    *   `allow_multiple_to_places` (boolean, Optional, default: false): Allows mapping to multiple parent places if true.
*   **Example**:
    ```yaml
    - type: PLACE_AGGREGATION
      stage: 1
      input_imports:
        - CensusACS5YearSurvey
      output_import: CensusACS5YearSurvey_AggCountry
      place_aggregation:
        from_place_types: State
        to_place_types: Country
    ```

#### 2. Statistical Variable Aggregation (`STAT_VAR_AGGREGATION`)
Aggregates raw statistical variables into a summarized ancestor variable.
*   **Fields**:
    *   `ancestor_sv_id` (string, Required): The ID of the parent/summary statistical variable (e.g., `Count_Person`).
    *   `source_sv_ids` (list of strings, Required): The list of individual statistical variables to sum up.
    *   `skip_all_sources_present_check` (boolean, Optional, default: false): If true, aggregates even if some source variables are missing.
*   **Example**:
    ```yaml
    - type: STAT_VAR_AGGREGATION
      stage: 2
      input_imports:
        - CensusACS5YearSurvey
      output_import: CensusACS5YearSurvey_StatVarAgg
      stat_var_aggregation:
        aggregations:
          - ancestor_sv_id: Count_Person
            source_sv_ids:
              - Count_Person_Male
              - Count_Person_Female
    ```

#### 3. Common Aggregations (`LINKED_EDGES`, `PROVENANCE_SUMMARY`, `STAT_VAR_GROUPS`)
Common graph structure, lineage, and UI group hierarchy rollups defined in `common.yaml`.
*   **Example**:
    ```yaml
    - type: LINKED_EDGES
      stage: 1
      input_imports:
        - "*"
    ```

---

## Local Configuration Validation

The orchestrator strictly validates configuration files against `schema.json`. If there is any syntax error, type mismatch, or missing required field, validation will fail.

### Running the Validator Locally
You can validate all configuration files locally using the built-in CLI tool:

1.  **Navigate to the aggregation-helper root**:
    ```bash
    cd pipeline/workflow/aggregation-helper
    ```
2.  **Run the validator**:
    ```bash
    python3 -m aggregation.validator

    # Sample output:
    # Validating 7 configuration file(s) in 'aggregation/configs' against 'schema.json'...
    #   ✓ common.yaml (3 calculation steps)
    #   ✓ place.yaml (16 calculation steps)
    #   ✓ statvar.yaml (21 calculation steps)
    # [SUCCESS] All 7 configuration file(s) passed validation! (64 calculation steps total)
    ```
