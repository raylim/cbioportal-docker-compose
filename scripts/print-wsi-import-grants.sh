#!/usr/bin/env bash
set -euo pipefail

# Print the least-privilege grants needed by
# hydrate_databricks_wsi_clickhouse.py.  This command never connects to
# ClickHouse and never handles credentials; pipe the output to an
# administrator's ClickHouse console after reviewing the target database.

database="${1:-${CLICKHOUSE_DATABASE:-}}"
role="${2:-${CLICKHOUSE_IMPORT_ROLE:-}}"
if [[ -z "$database" || -z "$role" ]]; then
  echo "usage: $0 DATABASE ROLE" >&2
  exit 2
fi
if [[ ! "$database" =~ ^[A-Za-z0-9_]+$ || ! "$role" =~ ^[A-Za-z0-9_]+$ ]]; then
  echo "DATABASE and ROLE must contain only letters, digits, and underscores" >&2
  exit 2
fi

grant() {
  printf 'GRANT %s ON %s.%s TO %s;\n' "$1" "$database" "$2" "$role"
}

for table in wsi_patient clinical_event clinical_event_data sample_profile \
  sample_to_gene_panel_derived gene_panel_to_gene_derived sample_derived \
  genomic_event_derived clinical_data_derived clinical_event_derived \
  clinical_event_data_derived genetic_alteration_derived generic_assay_data_derived \
  mutation_derived generic_assay_profile_entity_derived generic_assay_meta_derived; do
  grant INSERT "$table"
done

for table in wsi_patient wsi_part wsi_block wsi_slide wsi_slide_placement \
  clinical_attribute_meta clinical_sample clinical_patient clinical_event clinical_event_data; do
  grant "ALTER DELETE" "$table"
done

for table in sample_to_gene_panel_derived gene_panel_to_gene_derived sample_derived \
  genomic_event_derived clinical_data_derived clinical_event_derived \
  clinical_event_data_derived genetic_alteration_derived generic_assay_data_derived \
  mutation_derived generic_assay_profile_entity_derived generic_assay_meta_derived; do
  grant TRUNCATE "$table"
  grant OPTIMIZE "$table"
done

for table in clinical_patient clinical_sample genetic_alteration \
  genetic_profile_samples sample_profile; do
  grant OPTIMIZE "$table"
done
