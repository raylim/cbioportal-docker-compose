#!/usr/bin/env bash
set -euo pipefail

# Replace every molecular category for one existing study from its checked-in
# snapshot. This is intentionally separate from the Databricks WSI loader, but
# it is the required molecular step in a complete study hydration.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage: scripts/hydrate-study-molecular.sh \
  --study-dir PATH --study-id ID --clickhouse-container NAME --importer-jar PATH

Required environment:
  CLICKHOUSE_PASSWORD       ClickHouse password (provided via environment)

Optional environment:
  CLICKHOUSE_USER           default: cbio_user
  CLICKHOUSE_DB             default: cbioportal
  CLICKHOUSE_HTTP_PORT      default: 19225
  PORTAL_HOME               default: ../cbioportal/target/classes
  JAVA_BIN                  default: java
  MAINTENANCE_CONFIRMED     must be 1; prevents rebuilding while traffic is live
  DERIVED_TABLE_RECEIPT      optional path for the completion receipt

Stop the portal before running this command. After it completes, restart the
portal and run verify-study-load.py separately. The command bulk-replaces the
five molecular categories and rebuilds ClickHouse derived tables. It
intentionally does not use the importer’s per-sample --overwrite-existing mode.
USAGE
}

study_dir=""
study_id=""
clickhouse_container=""
importer_jar=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --study-dir) study_dir="$2"; shift 2 ;;
    --study-id) study_id="$2"; shift 2 ;;
    --clickhouse-container) clickhouse_container="$2"; shift 2 ;;
    --importer-jar) importer_jar="$2"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ -z "$study_dir" || -z "$study_id" || -z "$clickhouse_container" || -z "$importer_jar" ]]; then
  echo "--study-dir, --study-id, --clickhouse-container, and --importer-jar are required" >&2
  usage >&2
  exit 2
fi
if [[ ! "$study_id" =~ ^[A-Za-z0-9_.-]+$ ]]; then
  echo "study id contains unsupported characters: $study_id" >&2
  exit 2
fi
if [[ ! -d "$study_dir" || ! -f "$importer_jar" ]]; then
  echo "study directory or importer jar does not exist" >&2
  exit 2
fi
if [[ -z "${CLICKHOUSE_PASSWORD:-}" ]]; then
  echo "CLICKHOUSE_PASSWORD is required" >&2
  exit 2
fi
if [[ "${MAINTENANCE_CONFIRMED:-}" != "1" ]]; then
  echo "set MAINTENANCE_CONFIRMED=1 after stopping all portal consumers" >&2
  exit 1
fi

clickhouse_user="${CLICKHOUSE_USER:-cbio_user}"
clickhouse_db="${CLICKHOUSE_DB:-cbioportal}"
clickhouse_http_port="${CLICKHOUSE_HTTP_PORT:-19225}"
portal_home="${PORTAL_HOME:-$ROOT_DIR/../cbioportal/target/classes}"
java_bin="${JAVA_BIN:-java}"

metadata_value() {
  local meta="$1" key="$2"
  awk -F: -v wanted="$key" '$1 == wanted {sub(/^[[:space:]]+/, "", $2); print $2; exit}' "$meta"
}

find_meta() {
  local name
  for name in "$@"; do
    if [[ -f "$study_dir/$name" ]]; then
      printf '%s\n' "$study_dir/$name"
      return 0
    fi
  done
  return 1
}

require_meta_data() {
  local meta="$1" data_name
  [[ "$(metadata_value "$meta" cancer_study_identifier)" == "$study_id" ]] || {
    echo "study identifier mismatch in $(basename "$meta")" >&2
    exit 1
  }
  data_name="$(metadata_value "$meta" data_filename)"
  [[ -n "$data_name" && -f "$study_dir/$data_name" ]] || {
    echo "$(basename "$meta") references missing data_filename: $data_name" >&2
    exit 1
  }
  printf '%s\n' "$study_dir/$data_name"
}

mutation_meta="$(find_meta meta_mutations_extended.txt meta_mutations.txt)" || {
  echo "molecular snapshot is missing a mutation metadata file" >&2; exit 1;
}
cna_meta="$(find_meta meta_CNA.txt meta_cna.txt)" || {
  echo "molecular snapshot is missing a CNA metadata file" >&2; exit 1;
}
sv_meta="$study_dir/meta_sv.txt"
seg_meta="$(find_meta mskimpact_meta_cna_hg19_seg.txt meta_cna_hg19_seg.txt)" || {
  echo "molecular snapshot is missing a segment metadata file" >&2; exit 1;
}
panel_meta="$(find_meta meta_gene_matrix.txt meta_gene_panel_matrix.txt)" || {
  echo "molecular snapshot is missing a gene-panel metadata file" >&2; exit 1
}
[[ -f "$sv_meta" ]] || { echo "molecular snapshot is missing meta_sv.txt" >&2; exit 1; }
[[ "$(metadata_value "$mutation_meta" variant_classification_filter)" != "" ]] || {
  echo "mutation metadata must explicitly declare variant_classification_filter" >&2
  exit 1
}

mutation_data="$(require_meta_data "$mutation_meta")"
cna_data="$(require_meta_data "$cna_meta")"
sv_data="$(require_meta_data "$sv_meta")"
seg_data="$(require_meta_data "$seg_meta")"
panel_data="$(require_meta_data "$panel_meta")"

docker_ch() {
  docker exec -e "CLICKHOUSE_PASSWORD=$CLICKHOUSE_PASSWORD" "$clickhouse_container" \
    clickhouse-client --user "$clickhouse_user" --password "$CLICKHOUSE_PASSWORD" \
    --database "$clickhouse_db" "$@"
}

profile_id() {
  local meta="$1" alteration_type="$2" stable_id datatype profile_info profile_count profile
  stable_id="$(metadata_value "$meta" stable_id)"
  datatype="$(metadata_value "$meta" datatype)"
  [[ -n "$stable_id" && -n "$datatype" ]] || {
    echo "$(basename "$meta") must define stable_id and datatype" >&2
    exit 1
  }
  if [[ "$stable_id" != "${study_id}_"* ]]; then
    stable_id="${study_id}_${stable_id}"
  fi
  [[ "$stable_id" =~ ^[A-Za-z0-9_.-]+$ && "$datatype" =~ ^[A-Za-z0-9_.-]+$ ]] || {
    echo "unsupported profile identifier in $(basename "$meta")" >&2
    exit 1
  }
  profile_info="$(docker_ch --format TSVRaw --query \
    "SELECT toString(count()), toString(any(genetic_profile_id)) FROM genetic_profile WHERE cancer_study_id = $study_numeric_id AND stable_id = '$stable_id' AND genetic_alteration_type = '$alteration_type' AND datatype = '$datatype'")"
  read -r profile_count profile <<< "$profile_info"
  [[ "$profile_count" == "1" && "$profile" =~ ^[0-9]+$ ]] || {
    echo "metadata $(basename "$meta") does not resolve to exactly one profile: $stable_id" >&2
    exit 1
  }
  printf '%s\n' "$profile"
}

study_numeric_id="$(docker_ch --format Raw --query "SELECT cancer_study_id FROM cancer_study WHERE cancer_study_identifier = '$study_id' LIMIT 1")"
[[ -n "$study_numeric_id" ]] || { echo "study is not present in ClickHouse: $study_id" >&2; exit 1; }
derived_sql="$portal_home/db-scripts/clickhouse/populate_derived_tables.sql"
[[ -f "$derived_sql" ]] || { echo "derived-table SQL does not exist: $derived_sql" >&2; exit 1; }
mutation_profile="$(profile_id "$mutation_meta" MUTATION_EXTENDED)"
cna_profile="$(profile_id "$cna_meta" COPY_NUMBER_ALTERATION)"
sv_profile="$(profile_id "$sv_meta" STRUCTURAL_VARIANT)"
[[ -n "$mutation_profile" && -n "$cna_profile" && -n "$sv_profile" ]] || {
  echo "study is missing one or more required molecular profiles" >&2
  exit 1
}
if [[ "$mutation_profile" == "$cna_profile" || "$mutation_profile" == "$sv_profile" || "$cna_profile" == "$sv_profile" ]]; then
  echo "molecular metadata resolved to duplicate profile IDs" >&2
  exit 1
fi

echo "Replacing molecular data for $study_id (study id $study_numeric_id)"
docker_ch --mutations_sync 2 --query "ALTER TABLE mutation DELETE WHERE genetic_profile_id = $mutation_profile"
docker_ch --mutations_sync 2 --query "ALTER TABLE sample_cna_event DELETE WHERE genetic_profile_id = $cna_profile"
docker_ch --mutations_sync 2 --query "ALTER TABLE structural_variant DELETE WHERE genetic_profile_id = $sv_profile"
docker_ch --mutations_sync 2 --query "ALTER TABLE genetic_alteration DELETE WHERE genetic_profile_id = $cna_profile"
docker_ch --mutations_sync 2 --query "ALTER TABLE sample_profile DELETE WHERE genetic_profile_id IN ($mutation_profile,$cna_profile,$sv_profile)"
docker_ch --mutations_sync 2 --query "ALTER TABLE copy_number_seg DELETE WHERE cancer_study_id = $study_numeric_id"

jdbc_opts="-Dspring.profiles.active=dbcp -Dspring.datasource.url=jdbc:ch://127.0.0.1:${clickhouse_http_port}/${clickhouse_db} -Dspring.datasource.username=${clickhouse_user} -Dspring.datasource.password=${CLICKHOUSE_PASSWORD} -Dspring.datasource.driver-class-name=com.clickhouse.jdbc.ClickHouseDriver"
run_java() {
  PORTAL_HOME="$portal_home" "$java_bin" $jdbc_opts -cp "$importer_jar" "$@"
}

# No --overwrite-existing: the scoped bulk deletes above make this retry-safe
# without issuing one ClickHouse mutation per sample.
run_java org.mskcc.cbio.portal.scripts.ImportProfileData \
  --meta "$mutation_meta" --loadMode bulkload --update-info False --data "$mutation_data" --noprogress
run_java org.mskcc.cbio.portal.scripts.ImportProfileData \
  --meta "$cna_meta" --loadMode bulkload --update-info False --data "$cna_data" --noprogress
run_java org.mskcc.cbio.portal.scripts.ImportProfileData \
  --meta "$sv_meta" --loadMode bulkload --update-info False --data "$sv_data" --noprogress
run_java org.mskcc.cbio.portal.scripts.ImportCopyNumberSegmentData \
  --meta "$seg_meta" --loadMode bulkload --data "$seg_data" --noprogress
run_java org.mskcc.cbio.portal.scripts.ImportGenePanelProfileMap \
  --meta "$panel_meta" --data "$panel_data" --noprogress

docker exec -i "$clickhouse_container" clickhouse-client --user "$clickhouse_user" \
  --password "$CLICKHOUSE_PASSWORD" --database "$clickhouse_db" --multiquery \
  --param_optimize_backoff_secs=0 < "$derived_sql"

verify_derived_pair() {
  local source_table="$1" derived_table="$2" counts source_count derived_count
  counts="$(docker_ch --format TSVRaw --query \
    "SELECT toString(count()), toString((SELECT count() FROM $derived_table)) FROM $source_table")"
  read -r source_count derived_count <<< "$counts"
  if [[ "$source_count" =~ ^[0-9]+$ && "$derived_count" =~ ^[0-9]+$ \
      && "$source_count" -gt 0 && "$derived_count" -eq 0 ]]; then
    echo "derived table $derived_table is empty while $source_table has $source_count rows" >&2
    exit 1
  fi
  printf '%s\t%s\t%s\n' "$source_table" "$source_count" "$derived_count"
}

receipt_path="${DERIVED_TABLE_RECEIPT:-}"
receipt_tmp=""
if [[ -n "$receipt_path" ]]; then
  receipt_tmp="${receipt_path}.tmp.$$"
  {
    printf '{\n  "database": %s,\n  "study_id": %s,\n  "source_derived_counts": {\n' \
      "\"$clickhouse_db\"" "\"$study_id\""
    verify_derived_pair mutation mutation_derived | awk -F '\t' \
      '{printf "    \"mutation\": {\"source\": %s, \"derived\": %s},\n", $2, $3}'
    verify_derived_pair genetic_alteration genetic_alteration_derived | awk -F '\t' \
      '{printf "    \"genetic_alteration\": {\"source\": %s, \"derived\": %s}\n", $2, $3}'
    printf '  }\n}\n'
  } > "$receipt_tmp"
  mv "$receipt_tmp" "$receipt_path"
else
  verify_derived_pair mutation mutation_derived >/dev/null
  verify_derived_pair genetic_alteration genetic_alteration_derived >/dev/null
fi

echo "molecular hydration complete for $study_id; restart the portal and run verify-study-load.py"
