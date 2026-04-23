#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "Usage: $0 <project_id> <instance_id> <database_id>" >&2
  exit 1
fi

project_id="$1"
instance_id="$2"
database_id="$3"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
schema_file="${script_dir}/../../../rk-experiments/mixer/spanner/bq_spanner_ingestion/timeseries_schema.sql"
schema_with_suffix_file="$(mktemp)"

if [[ ! -f "${schema_file}" ]]; then
  echo "Schema file not found: ${schema_file}" >&2
  exit 1
fi

current_schema_file="$(mktemp)"
drop_ddl_file="$(mktemp)"
trap 'rm -f "${current_schema_file}" "${drop_ddl_file}" "${schema_with_suffix_file}"' EXIT

perl -0pe '
  s/\bTimeSeriesAttributePropertyValue\b/TimeSeriesAttributePropertyValue_rk/g;
  s/\bTimeSeriesAttributeValue\b/TimeSeriesAttributeValue_rk/g;
  s/\bTimeSeriesByProvenance\b/TimeSeriesByProvenance_rk/g;
  s/\bTimeSeriesByVariableMeasured\b/TimeSeriesByVariableMeasured_rk/g;
  s/\bObservationAttribute\b/ObservationAttribute_rk/g;
  s/\bStatVarObservation\b/StatVarObservation_rk/g;
  s/\bTimeSeriesAttribute\b/TimeSeriesAttribute_rk/g;
  s/\bTimeSeries\b/TimeSeries_rk/g;
' "${schema_file}" > "${schema_with_suffix_file}"

gcloud spanner databases ddl describe "${database_id}" \
  --project="${project_id}" \
  --instance="${instance_id}" \
  > "${current_schema_file}"

append_if_present() {
  local pattern="$1"
  local ddl="$2"
  if rg -q "${pattern}" "${current_schema_file}"; then
    printf '%s\n' "${ddl}" >> "${drop_ddl_file}"
  fi
}

append_if_present "CREATE INDEX TimeSeriesByProvenance_rk " "DROP INDEX TimeSeriesByProvenance_rk"
append_if_present "CREATE INDEX TimeSeriesByVariableMeasured_rk " "DROP INDEX TimeSeriesByVariableMeasured_rk"
append_if_present "CREATE INDEX TimeSeriesAttributePropertyValue_rk " "DROP INDEX TimeSeriesAttributePropertyValue_rk"
append_if_present "CREATE INDEX TimeSeriesAttributeValue_rk " "DROP INDEX TimeSeriesAttributeValue_rk"
append_if_present "CREATE TABLE ObservationAttribute_rk " "DROP TABLE ObservationAttribute_rk"
append_if_present "CREATE TABLE StatVarObservation_rk " "DROP TABLE StatVarObservation_rk"
append_if_present "CREATE TABLE TimeSeriesAttribute_rk " "DROP TABLE TimeSeriesAttribute_rk"
append_if_present "CREATE TABLE TimeSeries_rk " "DROP TABLE TimeSeries_rk"

if [[ -s "${drop_ddl_file}" ]]; then
  gcloud spanner databases ddl update "${database_id}" \
    --project="${project_id}" \
    --instance="${instance_id}" \
    --ddl-file="${drop_ddl_file}"
fi

gcloud spanner databases ddl update "${database_id}" \
  --project="${project_id}" \
  --instance="${instance_id}" \
  --ddl-file="${schema_with_suffix_file}"
