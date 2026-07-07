# StatVar Series Aggregation: C++ to Python Parity Implementation Plan

This document tracks all required changes to bring `import/pipeline/workflow/aggregation-helper/aggregation/stat_var_series_aggregator.py` into **100% parity** with the Google3 Flume/C++ implementation (`//depot/google3/datacommons/prophet/derived_graph/stat_var_series_aggregation/processor.cc`).

To make this process manageable, items are ordered from **Least Complex (Item 1)** to **Most Complex (Item 5)** and will be implemented iteratively upon user approval.

---

## 📋 Implementation Status & Order

### [x] Item 1: Config Key Unwrapping & Function Name Mapping in `_run_round()` (Least Complex)
- **Problem**:
  In protobuf (`AggrFunc`) and YAML configuration files (`configs/statvar_series.yaml`), aggregation functions are defined as single-key dictionaries (`{func_name: func_config_dict}`) because `aggr_funcs` is a protobuf `oneof`.
  Currently, `stat_var_series_aggregator.py` (`_run_round`, line 98) checks `func.get("type")`. When given real YAML/protobuf configs, `func.get("type")` returns `None`, logging an warning and skipping execution entirely.
  Additionally, alias names differ between C++ protobuf definitions and YAML configurations:
  - `aggr_stats_across_models` vs `stats_across_models`
  - `count_threshold_exception_over_time` vs `count_threshold`
- **Required Fix**:
  Update `_run_round()` in `stat_var_series_aggregator.py` to normalize each item in `aggr_funcs`. If an item is `{key: config_dict}`, unwrap the key into `func_type` and pass `func_config` to the handler methods. Map `aggr_stats_across_models` -> `stats_across_models` and `count_threshold_exception_over_time` -> `count_threshold`.
- **Complexity**: Low (~15 lines of logic in `_run_round()`).

---

### [x] Item 2: Output Naming Parity for `aggr_over_time` when `use_input_sv_for_output = false` (Low Complexity)
- **Problem**:
  In `stat_var_series_aggregator.py` (line 368), when `use_input_sv_for_output` is `False`, the output StatVar ID is constructed using `AggregateMax_<sv>` or `AggregateMin_<sv>`.
  In C++ `processor.cc` (`GenerateDiffStatVar()`, line 1056), when `property == kExtremesOverTime` (`extremesOverTime`), `OPERATOR_MAX` ("AggregateMax") is rewritten to `"HighestValue"`, yielding `HighestValue_<sv>`, and `OPERATOR_MIN` ("AggregateMin") is rewritten to `"LowestValue"`, yielding `LowestValue_<sv>`.
- **Required Fix**:
  In `_add_aggr_over_time_fragments()`, when `use_input_sv_for_output` is `False` (or when `op_name` is used for SV prefix), map `MAX` $\rightarrow$ `"HighestValue"` and `MIN` $\rightarrow$ `"LowestValue"` to match C++ DCID generation.
- **Complexity**: Low (~5 lines changed in `_add_aggr_over_time_fragments()`).

---

### [x] Item 3: `diff_relative_to_base_date` `date_specs` Unwrapping & Post-Base-Date Filtering (Moderate Complexity)
- **Problem**:
  1. **`date_specs` Unwrapping**: In protobuf and `statvar_series.yaml`, base dates are specified inside a nested `date_specs` list (`date_specs: [{dates: [...]}, {start_date: ..., end_date: ...}]`). Currently, `_add_diff_relative_fragments()` directly checks `func_config.get("dates", [])` and ignores `date_specs`.
  2. **Post-Base-Date Filtering (`year > base_date_year`)**: In C++ `ProcessDiffRelativeToBaseDate()` (`processor.cc` line 907: `if (year <= base_date_year) continue;`), relative anomalies are **only** emitted for observation dates strictly after the base date year. Currently, Python computes differences across all available observation dates (including those before or on the base date).
- **Required Fix**:
  Update `_add_diff_relative_fragments()` to iterate through `func_config.get("date_specs", [func_config])`. In the `DiffRelObs_{idx}` CTE, add `WHERE SUBSTR(r.date, 1, 4) > SUBSTR(b.base_date, 1, 4)` (or exact year comparison) so only strictly later dates are emitted.
- **Complexity**: Moderate (~25 lines changed/refactored in `_add_diff_relative_fragments()`).

---

### [x] Item 4: `count_threshold_exception_over_time` Nested `time_range` & `thresholds` Unwrapping (Moderate-High Complexity)
- **Problem**:
  In protobuf and `statvar_series.yaml`, threshold counts are structured with nested blocks:
  ```yaml
  count_threshold_exception_over_time:
    time_range: { input_obs_period: P1M, output_obs_period: P10Y, output_obs_date: "2030" }
    thresholds:
      - { sv_regex: "^.*Max_Temperature.*", threshold_value: 5, unit: Celsius, comparison: OPERATOR_GE }
  ```
  Currently, `_add_count_threshold_fragments()` expects flat keys (`threshold_value`, `comparison`, `input_period`) directly on `func_config`. When given real YAML, `func_config.get("threshold_value")` returns `None` and the function skips.
- **Required Fix**:
  Extract `input_period`, `output_period`, `output_obs_date` from `func_config.get("time_range", func_config)`. Loop over `func_config.get("thresholds", [func_config])`, extract `sv_regex` to filter source observations via `REGEXP_CONTAINS(variable_measured, r"^{sv_regex}$")`, and map `"GE"`/`"OPERATOR_GE"` $\rightarrow$ `>=` (`"LE"`/`"OPERATOR_LE"` $\rightarrow$ `<=`).
- **Complexity**: Moderate-High (~40 lines changed/refactored in `_add_count_threshold_fragments()`).

---

### [x] Item 5: `aggr_over_time` & `stats_across_models` (`aggr_stats_across_models`) `sv_regex` Filtering, `input_obs_period` Filtering, and Dynamic Operations (Most Complex)
- **Problem**:
  1. **`aggr_over_time` (`time_range` & `sv_configs`)**:
     Real configs structure temporal aggregations using `time_range` and a list of `sv_configs`, where each `sv_config` specifies an `sv_regex` and `aggregation_op` (`OPERATOR_MAX`, `OPERATOR_MIN`, `OPERATOR_MEAN`). Currently, Python reads flat `operator` and applies it across all input SVs without `sv_regex` filtering.
     Furthermore, C++ (`processor.cc` line 422) explicitly checks `obs_period == time_range.input_obs_period()`. Python currently does not filter `RawObs` by `input_obs_period`, meaning daily and monthly source rows could be incorrectly mixed together.
  2. **`stats_across_models` (`aggr_stats_across_models`)**:
     Currently, Python hardcodes `UNNEST(['MedianAcrossModels', 'Percentile10AcrossModels', 'Percentile90AcrossModels'])` across all input SVs. In C++, only SVs matching `sv_regex` are processed, and the exact quantiles computed depend on the configured `aggregation_ops` (`OPERATOR_MEDIAN`, `OPERATOR_PERCENTILE90`, `OPERATOR_PERCENTILE10`).
- **Required Fix**:
  - In `_add_aggr_over_time_fragments()`, unwrap `time_range` and loop over `sv_configs`. In the observation CTE, add `WHERE JSON_VALUE(ts.facet, '$.observationPeriod') = '{input_obs_period}' AND REGEXP_CONTAINS(variable_measured, r"^{svc['sv_regex']}$")`.
  - In `_add_stats_across_models_fragments()`, filter by `sv_regex` and dynamically build the `UNNEST(...)` and `UnrolledStats` UNION ALL branches based on `aggregation_ops`.
- **Complexity**: High (~75 lines changed/refactored across `_add_aggr_over_time_fragments()` and `_add_stats_across_models_fragments()`).
