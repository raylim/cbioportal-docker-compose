#!/usr/bin/env python3
"""Hydrate cBioPortal ClickHouse WSI tables from the Databricks contract.

This is deliberately a database-to-database loader rather than a study-file
shortcut.  It resolves every de-identified Databricks association against the
target portal's patient/sample tables, keeps explicit non-servable provenance,
loads the five WSI tables and the six WSI count attributes, and materializes
the pathology timeline in ``clinical_event``/``clinical_event_data``.

The loader is idempotent for studies represented by the current Databricks
snapshot.  It only replaces WSI rows, WSI count attributes, and pathology
timeline events for those studies; unrelated clinical events and studies are
left untouched.  No PHI table is queried.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

_TILE_SERVER_ROOT = Path(
    os.environ.get(
        "WSI_TILE_SERVER_ROOT",
        str(Path(__file__).resolve().parents[2] / "cbioportal-tile-server"),
    )
).resolve()
if _TILE_SERVER_ROOT.is_dir() and str(_TILE_SERVER_ROOT) not in sys.path:
    sys.path.insert(0, str(_TILE_SERVER_ROOT))

import export_databricks_wsi_snapshot as exporter  # noqa: E402

_TIMELINE_MODULE = None
try:
    from tools import generate_pathology_timeline_files as _TIMELINE_MODULE  # type: ignore
except ImportError:  # pragma: no cover - only possible in an incomplete checkout
    pass

WSI_ATTRS = (
    ("WSI_SAMPLE_SLIDE_COUNT", "WSI Slides per Sample", "Associated pathology slide count for the sample.", 0),
    ("WSI_SAMPLE_PART_MATCHED_SLIDE_COUNT", "WSI Slides per Sample, Part-matched", "Associated pathology slides matched to a specimen part.", 0),
    ("WSI_SAMPLE_BLOCK_MATCHED_SLIDE_COUNT", "WSI Slides per Sample, Block-matched", "Associated pathology slides matched to a specimen block.", 0),
    ("WSI_PATIENT_SLIDE_COUNT", "WSI Slides per Patient", "Associated pathology slide count for the patient.", 1),
    ("WSI_PATIENT_PART_MATCHED_SLIDE_COUNT", "WSI Slides per Patient, Part-matched", "Associated pathology slides matched to a specimen part for the patient.", 1),
    ("WSI_PATIENT_BLOCK_MATCHED_SLIDE_COUNT", "WSI Slides per Patient, Block-matched", "Associated pathology slides matched to a specimen block for the patient.", 1),
)

DATA_COLUMNS = exporter.DATA_COLUMNS
STAGING_SCHEMA_VERSION = "1"
WSI_FORMAT_VERSION = "3"
STAGING_METADATA_TABLE = "staging_metadata"
TIMELINE_COORDINATE_SYSTEM = "patient_first_tumor_sequencing_day_zero"
TIMELINE_STATUSES = {
    "AVAILABLE",
    "MISSING_PROCEDURE_DATE",
    "MISSING_REFERENCE_SEQUENCING_DATE",
}
TIMELINE_KINDS = {"RECORDED", "ESTIMATED", "UNDATED"}


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--clickhouse-config", type=Path, required=True,
                   help="0600-mode clickhouse-client YAML config")
    p.add_argument("--clickhouse-bin", default="clickhouse")
    p.add_argument("--database", required=True)
    p.add_argument(
        "--study-identifier",
        help="hydrate only this cBioPortal study (recommended for a targeted dev repair)",
    )
    p.add_argument(
        "--study-inventory",
        type=Path,
        help=(
            "JSON inventory generated from canonical IMPACT sample membership; "
            "required for a production-wide hydration"
        ),
    )
    p.add_argument(
        "--write-study-inventory",
        type=Path,
        help="write the read-only canonical IMPACT study inventory and exit",
    )
    p.add_argument(
        "--import-role",
        default=os.environ.get("CLICKHOUSE_IMPORT_ROLE", "beta_wsi_import_role"),
        help="role named in missing-permission remediation instructions",
    )
    p.add_argument("--warehouse-id", default=os.environ.get("DATABRICKS_WAREHOUSE_ID", "0b49b7d78734ad5c"))
    p.add_argument("--canonical-table", default="cdsi_prod.pathology_data_mining.canonical_slide_associations")
    p.add_argument("--registry-table", default="cdsi_prod.pathology_data_mining.slide_thumbnail_registry")
    p.add_argument("--allowed-source-prefix", dest="allowed_source_prefixes", action="append",
                   help="repeat for each approved S3 source prefix")
    p.add_argument("--keep-staging", action="store_true",
                   help="keep the temporary SQLite staging database for diagnosis")
    p.add_argument("--staging-db", type=Path,
                   help="reuse a completed SQLite staging database (skips Databricks scan)")
    p.add_argument(
        "--derived-tables-sql",
        type=Path,
        default=os.environ.get("CBIOPORTAL_DERIVED_TABLES_SQL", ""),
        help=(
            "release-pinned populate_derived_tables.sql; required for hydration "
            "so the rebuild cannot silently use another checkout"
        ),
    )
    p.add_argument(
        "--timeline-generator-sha",
        default=os.environ.get("WSI_TIMELINE_GENERATOR_SHA", ""),
        help=(
            "expected git SHA for WSI_TILE_SERVER_ROOT; required with "
            "--study-inventory"
        ),
    )
    p.add_argument(
        "--allow-incomplete-assets",
        action="store_true",
        help=(
            "diagnostic only: retain canonical rows whose asset bundle is incomplete; "
            "never use this mode for an accepted release"
        ),
    )
    return p.parse_args()


def _run_client(args: argparse.Namespace, query: str, *, multiquery: bool = False,
                capture: bool = True) -> str:
    command = [args.clickhouse_bin, "client", "--config-file", str(args.clickhouse_config),
               "--database", args.database, "--mutations_sync", "2"]
    if multiquery:
        command.append("--multiquery")
        input_text = query
    else:
        command.extend(["--query", query])
        input_text = None
    result = subprocess.run(command, input=input_text, text=True,
                            capture_output=capture, check=False)
    if result.returncode:
        raise RuntimeError(f"clickhouse query failed (exit {result.returncode}): {result.stderr[-4000:]}")
    return result.stdout if capture else ""


def _query_rows(args: argparse.Namespace, query: str) -> list[list[str]]:
    output = _run_client(args, query).strip("\n")
    if not output:
        return []
    rows: list[list[str]] = []
    for line in output.splitlines():
        values = []
        for value in line.split("\t"):
            values.append("" if value == r"\N" else value)
        rows.append(values)
    return rows


def _check_import_permissions(args: argparse.Namespace) -> None:
    """Fail before staging cleanup if the importer cannot complete a release.

    The previous flow discovered missing RBAC only after deleting part of a
    study.  ``SHOW GRANTS`` is read-only and lets the caller fix the role before
    any Databricks scan or ClickHouse mutation begins.  We inspect the role's
    grant text instead of issuing ``CHECK GRANT`` because older ClickHouse
    clients parse that statement as the unrelated table-check command.
    """
    readable = ("cancer_study", "patient", "sample")
    wsi_tables = (
        "wsi_patient",
        "wsi_part",
        "wsi_block",
        "wsi_slide",
        "wsi_slide_placement",
        "wsi_slide_timing",
    )
    clinical_tables = (
        "clinical_attribute_meta",
        "clinical_sample",
        "clinical_patient",
        "clinical_event",
        "clinical_event_data",
    )
    derived_tables = (
        "sample_to_gene_panel_derived",
        "gene_panel_to_gene_derived",
        "sample_derived",
        "genomic_event_derived",
        "clinical_data_derived",
        "clinical_event_derived",
        "clinical_event_data_derived",
        "genetic_alteration_derived",
        "generic_assay_data_derived",
        "mutation_derived",
        "generic_assay_profile_entity_derived",
        "generic_assay_meta_derived",
    )
    checks: list[tuple[str, str]] = [("SELECT", table) for table in readable]
    checks.extend(
        ("INSERT", table)
        for table in (*wsi_tables, *clinical_tables, "sample_profile", *derived_tables)
    )
    checks.extend(("ALTER DELETE", table) for table in (*wsi_tables, *clinical_tables))
    checks.extend(("TRUNCATE", table) for table in derived_tables)
    checks.extend(
        ("OPTIMIZE", table)
        for table in (
            "clinical_patient",
            "clinical_sample",
            "genetic_alteration",
            "genetic_profile_samples",
            "sample_profile",
            *derived_tables,
        )
    )
    if re.fullmatch(r"[A-Za-z0-9_]+", args.import_role) is None:
        raise RuntimeError("--import-role must contain only letters, digits, and underscores")
    grant_lines = _run_client(
        args, f"SHOW GRANTS FOR {args.import_role}", capture=True
    ).splitlines()

    def split_privileges(value: str) -> list[str]:
        parts: list[str] = []
        start = 0
        depth = 0
        for index, character in enumerate(value):
            if character == "(":
                depth += 1
            elif character == ")":
                depth = max(0, depth - 1)
            elif character == "," and depth == 0:
                parts.append(value[start:index].strip())
                start = index + 1
        parts.append(value[start:].strip())
        return [part for part in parts if part]

    grants: list[tuple[set[str], str]] = []
    for line in grant_lines:
        match = re.fullmatch(r"GRANT (.+) ON ([^ ]+) TO .+", line.strip())
        if not match:
            continue
        grants.append((set(split_privileges(match.group(1))), match.group(2)))

    def covered(privilege: str, table: str) -> bool:
        object_names = {f"{args.database}.{table}", f"{args.database}.*", "*.*"}
        for granted, object_name in grants:
            if object_name not in object_names:
                continue
            if privilege in granted or (privilege.startswith("ALTER ") and "ALTER" in granted):
                return True
        return False

    missing: list[tuple[str, str]] = []
    for privilege, table in checks:
        if not covered(privilege, table):
            missing.append((privilege, table))
    if missing:
        grants = "; ".join(
            f"GRANT {privilege} ON {args.database}.{table} TO {args.import_role}"
            for privilege, table in missing
        )
        raise RuntimeError(
            "ClickHouse importer role is not authorized for a complete WSI "
            f"hydration ({len(missing)} missing privileges). Run: {grants}"
        )


def _sql_ids(values: Iterable[int]) -> str:
    unique = sorted({int(value) for value in values})
    if not unique:
        return "0"
    return ",".join(str(value) for value in unique)


def _tsv(value: Any, *, nullable: bool = False) -> str:
    if value is None or (nullable and value == ""):
        return r"\N"
    if isinstance(value, bool):
        return "1" if value else "0"
    text = str(value)
    # ClickHouse TSV escaping; all published free text has already had tabs and
    # newlines normalized by the exporter, but metadata JSON can contain '\\'.
    return (text.replace("\\", "\\\\").replace("\t", "\\t")
            .replace("\n", "\\n").replace("\r", "\\r"))


def _insert_stream(args: argparse.Namespace, table: str, columns: list[str],
                   rows: Iterable[Iterable[Any]], nullable: set[int] | None = None) -> int:
    nullable = nullable or set()
    command = [args.clickhouse_bin, "client", "--config-file", str(args.clickhouse_config),
               "--database", args.database, "--mutations_sync", "2", "--query",
               f"INSERT INTO {table} ({', '.join(columns)}) FORMAT TSV"]
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)
    count = 0
    assert process.stdin is not None
    broken_pipe = False
    try:
        for row in rows:
            try:
                process.stdin.write("\t".join(_tsv(value, nullable=index in nullable)
                                              for index, value in enumerate(row)) + "\n")
            except BrokenPipeError:
                broken_pipe = True
                break
            count += 1
        if not broken_pipe:
            process.stdin.close()
        stderr = process.stderr.read() if process.stderr is not None else ""
        code = process.wait()
    except BaseException:
        process.kill()
        process.wait()
        raise
    if code or broken_pipe:
        raise RuntimeError(f"insert into {table} failed (exit {code}): {stderr[-4000:]}")
    return count


def _load_target_maps(args: argparse.Namespace, study_ids: set[int] | None = None) -> tuple[dict[str, list[tuple[int, int]]],
                                                          dict[str, list[tuple[int, int, int, str]]],
                                                          dict[int, str]]:
    patient_rows = _query_rows(args, "SELECT stable_id, internal_id, cancer_study_id FROM patient FORMAT TSV")
    patient_stable_by_internal: dict[int, str] = {}
    patient_study_by_internal: dict[int, int] = {}
    patient_targets: dict[str, list[tuple[int, int]]] = defaultdict(list)
    for stable, internal, study in patient_rows:
        if not stable:
            continue
        internal_id, study_id = int(internal), int(study)
        if study_ids is not None and study_id not in study_ids:
            continue
        patient_stable_by_internal[internal_id] = stable
        patient_study_by_internal[internal_id] = study_id
        patient_targets[stable].append((study_id, internal_id))

    # cBioPortal's sample table stores the owning patient, not a study column;
    # resolve the study through the already-loaded patient map.
    sample_rows = _query_rows(args, "SELECT stable_id, internal_id, patient_id FROM sample FORMAT TSV")
    sample_targets: dict[str, list[tuple[int, int, int, str]]] = defaultdict(list)
    for stable, internal, patient_internal in sample_rows:
        if not stable:
            continue
        sample_internal, patient_internal_id = int(internal), int(patient_internal)
        patient_stable = patient_stable_by_internal.get(patient_internal_id, "")
        # A sample belongs to exactly the study owning its internal patient.
        # Do not fan it out to every study that happens to reuse the same
        # de-identified patient stable identifier (study aliases/clones are
        # common in the portal database).
        study_id = patient_study_by_internal.get(patient_internal_id)
        if study_id is not None:
            sample_targets[stable].append((study_id, sample_internal, patient_internal_id, patient_stable))
    return patient_targets, sample_targets, patient_stable_by_internal


def _candidate_targets(record: dict[str, Any], patient_targets, sample_targets):
    patient = exporter._text(record.get("patient_id"))
    sample = exporter._text(record.get("sample_id"))
    candidates = []
    if sample:
        candidates.extend(item for item in sample_targets.get(sample, [])
                         if item[3] == patient)
    # A canonical sample can be absent from a study's sample table.  Retain it
    # as an unmatched slide in every study containing the patient rather than
    # dropping valid pathology provenance.
    if not candidates:
        candidates.extend((study, 0, patient_internal, patient)
                          for study, patient_internal in patient_targets.get(patient, []))
    seen = set()
    for item in candidates:
        key = (item[0], item[2])
        if key not in seen:
            seen.add(key)
            yield item


def _inventory_from_source(
    args: argparse.Namespace,
    study_stable_by_id: dict[int, str],
) -> dict[str, Any]:
    """Build a frozen study inventory from the canonical IMPACT source.

    This mode only reads the target portal's patient/sample maps and the
    Databricks canonical association contract. It never creates or deletes
    ClickHouse rows, which makes it safe to run before a production
    maintenance window.
    """
    patient_targets, sample_targets, _ = _load_target_maps(args)
    by_study: dict[int, dict[str, Any]] = {}
    for record in exporter._run_export_query(
        args.canonical_table, args.registry_table, args.warehouse_id
    ):
        for study_id, _sample_internal, _patient_internal, patient_stable in _candidate_targets(
            record, patient_targets, sample_targets
        ):
            entry = by_study.setdefault(
                study_id,
                {
                    "study_id": study_stable_by_id[study_id],
                    "canonical_row_count": 0,
                    "patient_ids": set(),
                    "sample_ids": set(),
                },
            )
            entry["canonical_row_count"] += 1
            entry["patient_ids"].add(patient_stable)
            sample_id = exporter._text(record.get("sample_id"))
            if sample_id:
                entry["sample_ids"].add(sample_id)

    studies = []
    for entry in sorted(by_study.values(), key=lambda value: value["study_id"]):
        studies.append(
            {
                "study_id": entry["study_id"],
                "canonical_row_count": entry["canonical_row_count"],
                "patient_count": len(entry["patient_ids"]),
                "sample_count": len(entry["sample_ids"]),
            }
        )
    if not studies:
        raise RuntimeError(
            "canonical IMPACT source did not overlap any target portal study; "
            "refusing to write an empty production inventory"
        )
    return {
        "version": 1,
        "kind": "canonical_impact_sample_membership",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "canonical_table": args.canonical_table,
        "registry_table": args.registry_table,
        "warehouse_id": args.warehouse_id,
        "studies": studies,
    }


def _inventory_sha256(value: dict[str, Any]) -> str:
    payload = {key: item for key, item in value.items() if key != "inventory_sha256"}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _read_study_inventory(
    path: Path, canonical_table: str, registry_table: str, warehouse_id: str
) -> list[str]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"cannot read study inventory: {path}") from exc
    if not isinstance(value, dict) or value.get("version") != 1:
        raise RuntimeError("study inventory must have version 1")
    if value.get("kind") != "canonical_impact_sample_membership":
        raise RuntimeError("study inventory is not a canonical IMPACT inventory")
    declared_digest = value.get("inventory_sha256")
    if not isinstance(declared_digest, str) or not re.fullmatch(r"[0-9a-f]{64}", declared_digest):
        raise RuntimeError("study inventory must contain a valid inventory_sha256")
    if declared_digest != _inventory_sha256(value):
        raise RuntimeError("study inventory checksum does not match its contents")
    for key, expected in (
        ("canonical_table", canonical_table),
        ("registry_table", registry_table),
        ("warehouse_id", warehouse_id),
    ):
        declared = value.get(key)
        if declared and declared != expected:
            raise RuntimeError(
                f"study inventory {key} does not match the hydration source: "
                f"{declared} != {expected}"
            )
    studies = value.get("studies")
    if not isinstance(studies, list) or not studies:
        raise RuntimeError("study inventory must contain a non-empty studies array")
    identifiers: list[str] = []
    for item in studies:
        if not isinstance(item, dict) or not isinstance(item.get("study_id"), str):
            raise RuntimeError("study inventory contains an invalid study entry")
        study_id = item["study_id"].strip()
        if not study_id or study_id in identifiers:
            raise RuntimeError(f"study inventory contains duplicate or empty study: {study_id}")
        identifiers.append(study_id)
    return identifiers


def _validate_timeline_generator(expected_sha: str) -> str:
    if not expected_sha:
        raise RuntimeError(
            "--timeline-generator-sha or WSI_TIMELINE_GENERATOR_SHA is required "
            "for production hydration"
        )
    if not re.fullmatch(r"[0-9a-f]{40}", expected_sha):
        raise RuntimeError("timeline generator SHA must be a full lowercase git SHA")
    try:
        completed = subprocess.run(
            ["git", "-C", str(_TILE_SERVER_ROOT), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(
            f"cannot resolve timeline generator checkout: {_TILE_SERVER_ROOT}"
        ) from exc
    actual_sha = completed.stdout.strip()
    if actual_sha != expected_sha:
        raise RuntimeError(
            "timeline generator checkout does not match the release pin: "
            f"{actual_sha} != {expected_sha}"
        )
    return actual_sha


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _create_staging_schema(db: sqlite3.Connection) -> None:
    """Create the versioned, self-describing staging database schema."""
    db.execute(
        """CREATE TABLE slides (
            study_id INTEGER NOT NULL,
            patient_internal INTEGER NOT NULL,
            image_id TEXT NOT NULL,
            sample_internal INTEGER,
            reference_internal INTEGER,
            rank_key TEXT NOT NULL,
            values_json TEXT NOT NULL,
            timing_json TEXT NOT NULL,
            PRIMARY KEY(study_id, patient_internal, image_id)
        )"""
    )
    db.execute(
        f"""CREATE TABLE {STAGING_METADATA_TABLE} (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )"""
    )


def _write_staging_metadata(
    db: sqlite3.Connection,
    *,
    selected_study_ids: set[int],
    prefixes: tuple[str, ...],
    timeline_generator_sha: str,
    derived_tables_sql_sha256: str,
    stats: dict[str, Any],
    diagnostic: bool,
) -> None:
    """Bind a staging snapshot to the exact release inputs that produced it."""
    metadata = {
        "staging_schema_version": STAGING_SCHEMA_VERSION,
        "wsi_format_version": WSI_FORMAT_VERSION,
        "data_columns": list(DATA_COLUMNS),
        "timeline_columns": list(exporter.TIMELINE_COLUMNS),
        "selected_study_ids": sorted(int(value) for value in selected_study_ids),
        "allowed_source_prefixes": list(prefixes),
        "timeline_generator_sha": timeline_generator_sha,
        "derived_tables_sql_sha256": derived_tables_sql_sha256,
        "diagnostic_incomplete_assets": bool(diagnostic),
        "stats": {key: int(value) for key, value in stats.items() if isinstance(value, int)},
    }
    db.executemany(
        f"INSERT INTO {STAGING_METADATA_TABLE}(key,value) VALUES(?,?)",
        ((key, json.dumps(value, sort_keys=True, separators=(",", ":")))
         for key, value in metadata.items()),
    )
    db.commit()


def _read_staging_metadata(db: sqlite3.Connection) -> dict[str, Any]:
    table = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (STAGING_METADATA_TABLE,),
    ).fetchone()
    if not table:
        raise RuntimeError(
            "staging snapshot is missing v3 metadata; refusing to mutate ClickHouse "
            "(legacy staging databases are not resumable)"
        )
    rows = db.execute(f"SELECT key,value FROM {STAGING_METADATA_TABLE}").fetchall()
    metadata: dict[str, Any] = {}
    try:
        for key, value in rows:
            metadata[str(key)] = json.loads(value)
    except (TypeError, json.JSONDecodeError) as exc:
        raise RuntimeError("staging snapshot has invalid metadata; refusing to mutate ClickHouse") from exc
    required = {
        "staging_schema_version",
        "wsi_format_version",
        "data_columns",
        "timeline_columns",
        "selected_study_ids",
        "allowed_source_prefixes",
        "timeline_generator_sha",
        "derived_tables_sql_sha256",
        "diagnostic_incomplete_assets",
        "stats",
    }
    missing = sorted(required - set(metadata))
    if missing:
        raise RuntimeError(
            "staging snapshot metadata is incomplete; missing " + ", ".join(missing)
        )
    return metadata


def _validate_staging(
    db: sqlite3.Connection,
    *,
    selected_study_ids: set[int],
    prefixes: tuple[str, ...],
    timeline_generator_sha: str,
    derived_tables_sql_sha256: str,
    allow_incomplete_assets: bool,
) -> dict[str, int]:
    """Validate all staged rows before any target-table cleanup is possible."""
    slides = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='slides'"
    ).fetchone()
    if not slides:
        raise RuntimeError("staging database has no slides table; refusing to mutate ClickHouse")
    metadata = _read_staging_metadata(db)
    expected_metadata = {
        "staging_schema_version": STAGING_SCHEMA_VERSION,
        "wsi_format_version": WSI_FORMAT_VERSION,
        "data_columns": list(DATA_COLUMNS),
        "timeline_columns": list(exporter.TIMELINE_COLUMNS),
        "selected_study_ids": sorted(int(value) for value in selected_study_ids),
        "allowed_source_prefixes": list(prefixes),
        "timeline_generator_sha": timeline_generator_sha,
        "derived_tables_sql_sha256": derived_tables_sql_sha256,
    }
    for key, expected in expected_metadata.items():
        if metadata.get(key) != expected:
            raise RuntimeError(
                f"staging snapshot {key} does not match the requested release inputs"
            )
    if metadata.get("diagnostic_incomplete_assets") and not allow_incomplete_assets:
        raise RuntimeError(
            "staging snapshot was created with diagnostic incomplete-asset mode; "
            "refusing to use it for a strict release"
        )

    stats_value = metadata.get("stats")
    if not isinstance(stats_value, dict):
        raise RuntimeError("staging snapshot has invalid stage statistics")
    actual_rows = int(db.execute("SELECT count(*) FROM slides").fetchone()[0])
    actual_studies = {
        int(row[0]) for row in db.execute("SELECT DISTINCT study_id FROM slides")
    }
    if actual_rows != int(stats_value.get("selected_rows", -1)):
        raise RuntimeError("staging snapshot row count does not match its metadata")
    if len(actual_studies) != int(stats_value.get("study_count", -1)):
        raise RuntimeError("staging snapshot study count does not match its metadata")
    if actual_studies != selected_study_ids:
        raise RuntimeError("staging database study scope does not match the selected release inventory")
    if actual_rows == 0:
        raise RuntimeError("staging snapshot is empty; refusing to mutate ClickHouse")

    can_serve_index = DATA_COLUMNS.index("CAN_SERVE_TILES")
    source_index = DATA_COLUMNS.index("SOURCE_URL")
    metadata_index = DATA_COLUMNS.index("TILE_METADATA_JSON")
    thumbnail_index = DATA_COLUMNS.index("THUMBNAIL_URL")
    width_index = DATA_COLUMNS.index("THUMBNAIL_WIDTH")
    height_index = DATA_COLUMNS.index("THUMBNAIL_HEIGHT")
    content_type_index = DATA_COLUMNS.index("THUMBNAIL_CONTENT_TYPE")
    for row_number, row in enumerate(
        db.execute(
            "SELECT study_id,values_json,timing_json FROM slides "
            "ORDER BY study_id,patient_internal,image_id"
        ),
        start=1,
    ):
        study_id, values_json, timing_json = row
        try:
            values = json.loads(values_json)
            timing = json.loads(timing_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"staging row {row_number} contains invalid JSON") from exc
        if not isinstance(values, list) or len(values) != len(DATA_COLUMNS):
            raise RuntimeError(
                f"staging row {row_number} does not use WSI format v3 ({len(DATA_COLUMNS)} data columns)"
            )
        if not isinstance(timing, list) or len(timing) != len(exporter.TIMELINE_COLUMNS):
            raise RuntimeError(
                f"staging row {row_number} does not use the v3 seven-field timeline contract"
            )
        if str(values[can_serve_index]).upper() not in {"TRUE", "FALSE"}:
            raise RuntimeError(f"staging row {row_number} has invalid CAN_SERVE_TILES")
        if str(values[can_serve_index]).upper() == "TRUE":
            if not all(str(values[index]).strip() for index in (
                source_index, metadata_index, thumbnail_index, width_index,
                height_index, content_type_index,
            )):
                raise RuntimeError(f"servable staging row {row_number} has an incomplete asset bundle")
            if not any(str(values[source_index]).startswith(prefix) for prefix in prefixes):
                raise RuntimeError(f"servable staging row {row_number} uses a disallowed source prefix")
            if not str(values[thumbnail_index]).startswith(exporter._THUMBNAIL_PREFIX):
                raise RuntimeError(f"servable staging row {row_number} uses an invalid thumbnail prefix")
            try:
                tile_metadata = json.loads(values[metadata_index])
                width = int(values[width_index])
                height = int(values[height_index])
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError(f"servable staging row {row_number} has invalid asset metadata") from exc
            if width <= 0 or height <= 0 or not exporter._metadata_is_safe_and_valid(tile_metadata):
                raise RuntimeError(f"servable staging row {row_number} has invalid asset metadata")

        status = str(timing[1] or "").strip().upper()
        kind = str(timing[2] or "").strip().upper()
        start = str(timing[0] or "").strip()
        reason = str(timing[4] or "").strip()
        coordinate_system = str(timing[5] or "").strip()
        if status not in TIMELINE_STATUSES or kind not in TIMELINE_KINDS:
            raise RuntimeError(f"staging row {row_number} has invalid v3 timeline status/kind")
        if coordinate_system != TIMELINE_COORDINATE_SYSTEM:
            raise RuntimeError(f"staging row {row_number} has an unsupported timeline coordinate system")
        if status == "AVAILABLE":
            try:
                int(start)
            except (TypeError, ValueError) as exc:
                raise RuntimeError(f"staging row {row_number} has an invalid available timeline offset") from exc
            if kind == "UNDATED" or reason:
                raise RuntimeError(f"staging row {row_number} has inconsistent AVAILABLE timing")
        elif start:
            raise RuntimeError(f"staging row {row_number} has an offset for non-AVAILABLE timing")
        if status == "MISSING_PROCEDURE_DATE" and kind != "UNDATED":
            raise RuntimeError(f"staging row {row_number} has an invalid missing-procedure timing kind")
        if status == "MISSING_REFERENCE_SEQUENCING_DATE" and kind == "UNDATED":
            raise RuntimeError(f"staging row {row_number} has an invalid missing-reference timing kind")
        if not str(timing[6] or "").strip():
            raise RuntimeError(f"staging row {row_number} has no timepoint source")
    validated_stats = {
        key: int(value)
        for key, value in stats_value.items()
        if isinstance(value, int) and not isinstance(value, bool)
    }
    validated_stats.update({"selected_rows": actual_rows, "study_count": len(actual_studies)})
    return validated_stats


def _rank(values: list[str]) -> str:
    levels = {"BLOCK": "0", "PART": "1", "UNMATCHED": "2"}
    return "{}|{}|{}|{}".format(
        levels.get(values[DATA_COLUMNS.index("MATCH_LEVEL")], "3"),
        "0" if values[DATA_COLUMNS.index("SAMPLE_ID")] else "1",
        "0" if values[DATA_COLUMNS.index("CAN_SERVE_TILES")] == "TRUE" else "1",
        values[DATA_COLUMNS.index("SAMPLE_ID")],
    )


def _stage(args: argparse.Namespace, db: sqlite3.Connection, patient_targets, sample_targets,
           patient_stable_by_internal, prefixes, *, require_complete_assets: bool) -> dict[str, int]:
    stats = defaultdict(int)
    asset_stats: dict[str, int] = {}
    selected = 0
    for record in exporter._run_export_query(args.canonical_table, args.registry_table, args.warehouse_id):
        stats["databricks_rows"] += 1
        patient = exporter._text(record.get("patient_id"))
        targets = list(_candidate_targets(record, patient_targets, sample_targets))
        if not targets:
            stats["unmatched_patient_rows"] += 1
            continue
        for study_id, sample_internal, patient_internal, patient_stable in targets:
            canonical_sample = exporter._text(record.get("sample_id"))
            valid_sample = canonical_sample and sample_internal and sample_targets.get(canonical_sample)
            if valid_sample:
                valid_sample = any(item[0] == study_id and item[1] == sample_internal and item[2] == patient_internal
                                   for item in sample_targets[canonical_sample])
            normalized = dict(record)
            normalized["sample_id"] = canonical_sample if valid_sample else None
            reference = exporter._text(record.get("reference_sample_id"))
            reference_internal = None
            if reference:
                for item in sample_targets.get(reference, []):
                    if item[0] == study_id and item[2] == patient_internal:
                        reference_internal = item[1]
                        break
            normalized["reference_sample_id"] = reference if reference_internal is not None else None
            values = exporter._row(
                normalized,
                {patient_stable},
                {canonical_sample} if valid_sample else set(),
                {canonical_sample: patient_stable} if valid_sample else {},
                require_complete_assets=require_complete_assets,
                asset_stats=asset_stats,
                allowed_source_prefixes=prefixes,
            )
            if values is None:
                stats["filtered_rows"] += 1
                continue
            row_key = (study_id, patient_internal, values[DATA_COLUMNS.index("IMAGE_ID")])
            rank_key = _rank(values)
            payload = json.dumps(values, separators=(",", ":"))
            timing = exporter._timeline_values(record)
            timing_json = json.dumps(timing, separators=(",", ":"))
            db.execute(
                """INSERT INTO slides(study_id,patient_internal,image_id,sample_internal,
                   reference_internal,rank_key,values_json,timing_json)
                   VALUES(?,?,?,?,?,?,?,?)
                   ON CONFLICT(study_id,patient_internal,image_id) DO UPDATE SET
                   sample_internal=excluded.sample_internal,
                   reference_internal=excluded.reference_internal,
                   rank_key=excluded.rank_key, values_json=excluded.values_json,
                   timing_json=excluded.timing_json
                   WHERE excluded.rank_key < slides.rank_key""",
                (study_id, patient_internal, row_key[2], sample_internal or None,
                 reference_internal, rank_key, payload, timing_json),
            )
            selected += 1
        if stats["databricks_rows"] % 100_000 == 0:
            print(f"scanned {stats['databricks_rows']:,} Databricks rows; selected attempts {selected:,}", file=sys.stderr, flush=True)
        if stats["databricks_rows"] % 10_000 == 0:
            db.commit()
    db.commit()
    stats.update(asset_stats)
    stats["selected_rows"] = db.execute("SELECT count(*) FROM slides").fetchone()[0]
    stats["study_count"] = db.execute("SELECT count(DISTINCT study_id) FROM slides").fetchone()[0]
    return dict(stats)


def _cleanup(args: argparse.Namespace, study_ids: list[int]) -> None:
    ids = _sql_ids(study_ids)
    for table in ("wsi_slide_timing", "wsi_slide_placement", "wsi_slide", "wsi_block", "wsi_part", "wsi_patient"):
        _run_client(args, f"ALTER TABLE {table} DELETE WHERE cancer_study_id IN ({ids})")
    # ReplacingMergeTree does not remove old versions immediately, so remove
    # WSI count attributes before inserting this snapshot.  The subqueries are
    # constrained to the selected studies and cannot affect other attributes.
    attr_ids = ",".join("'" + attr[0] + "'" for attr in WSI_ATTRS)
    sample_entity = ("SELECT s.internal_id FROM sample s INNER JOIN patient p "
                     "ON s.patient_id = p.internal_id WHERE p.cancer_study_id IN "
                     f"({ids})")
    for table, entity_query in (("clinical_sample", sample_entity),
                                ("clinical_patient", f"SELECT internal_id FROM patient WHERE cancer_study_id IN ({ids})")):
        query = (f"ALTER TABLE {table} DELETE WHERE attr_id IN ({attr_ids}) "
                 f"AND internal_id IN ({entity_query})")
        existing_query = query.replace(
            f"ALTER TABLE {table} DELETE WHERE ",
            f"SELECT count() FROM {table} WHERE ",
            1,
        )
        existing = _query_rows(args, existing_query)
        if existing and existing[0] and int(existing[0][0]) > 0:
            _run_client(args, query)
    meta_query = (f"ALTER TABLE clinical_attribute_meta DELETE WHERE attr_id IN ({attr_ids}) "
                  f"AND cancer_study_id IN ({ids})")
    meta_count = _query_rows(args, meta_query.replace(
        "ALTER TABLE clinical_attribute_meta DELETE WHERE ",
        "SELECT count() FROM clinical_attribute_meta WHERE ",
        1,
    ))
    if meta_count and int(meta_count[0][0]) > 0:
        _run_client(args, meta_query)

    old_event_ids = _query_rows(args, f"SELECT e.clinical_event_id FROM clinical_event e INNER JOIN patient p ON e.patient_id=p.internal_id WHERE e.event_type='PATHOLOGY SLIDES' AND p.cancer_study_id IN ({ids}) FORMAT TSV")
    event_ids = [int(row[0]) for row in old_event_ids if row and row[0]]
    for offset in range(0, len(event_ids), 5000):
        chunk = _sql_ids(event_ids[offset:offset + 5000])
        _run_client(args, f"ALTER TABLE clinical_event_data DELETE WHERE clinical_event_id IN ({chunk})")
        _run_client(args, f"ALTER TABLE clinical_event DELETE WHERE clinical_event_id IN ({chunk})")


def _iter_slides(db: sqlite3.Connection):
    return db.execute("SELECT study_id,patient_internal,image_id,sample_internal,reference_internal,values_json,timing_json FROM slides ORDER BY study_id,patient_internal,image_id")


def _populate_tables(args: argparse.Namespace, db: sqlite3.Connection, study_ids: list[int]) -> None:
    # Build patient rows and counts in a compact dictionary; hierarchy metadata
    # is bounded by distinct parts/blocks, not the number of image pixels.
    patients: dict[tuple[int, int], tuple[int | None, int, int, int]] = {}
    parts: dict[tuple[int, int, str], tuple[str, ...]] = {}
    blocks: dict[tuple[int, int, str, str], tuple[str, ...]] = {}
    sample_counts: dict[tuple[int, int], list[int]] = defaultdict(lambda: [0, 0, 0])
    patient_counts: dict[tuple[int, int], list[int]] = defaultdict(lambda: [0, 0, 0])

    def slide_rows(*, count_rows: bool):
        for study, patient, image, sample, reference, values_json, timing_json in _iter_slides(db):
            values = json.loads(values_json)
            sample = int(sample) if sample is not None else None
            reference = int(reference) if reference is not None else None
            level = values[DATA_COLUMNS.index("MATCH_LEVEL")]
            patients.setdefault((study, patient), (reference, 0, 0, 0))
            current = patients[(study, patient)]
            if current[0] is None and reference is not None:
                patients[(study, patient)] = (reference, current[1], current[2], current[3])
            part_key = values[DATA_COLUMNS.index("PART_KEY")]
            block_key = values[DATA_COLUMNS.index("BLOCK_KEY")]
            # part_key is the map key and is supplied separately to the
            # insert row; retain only the six nullable metadata columns here.
            parts.setdefault((study, patient, part_key), tuple(values[index] or None for index in (5, 6, 7, 8, 9, 10)))
            blocks.setdefault((study, patient, part_key, block_key), tuple(values[index] or None for index in (12, 13)))
            if count_rows and sample is not None:
                counts = sample_counts[(study, sample)]
                counts[0] += 1
                if level == "PART": counts[1] += 1
                if level == "BLOCK": counts[2] += 1
            if count_rows:
                counts = patient_counts[(study, patient)]
                counts[0] += 1
                if level == "PART": counts[1] += 1
                if level == "BLOCK": counts[2] += 1
            yield study, patient, image, sample, values, json.loads(timing_json)

    # Materialize the compact hierarchy/count dictionaries before opening any
    # ClickHouse insert stream.  ``slide_rows`` is intentionally a generator;
    # creating it alone does not populate ``patients`` or ``parts``.
    for _ in slide_rows(count_rows=True):
        pass
    slides_for_insert = slide_rows(count_rows=False)
    _insert_stream(args, "wsi_patient", ["cancer_study_id", "patient_id", "reference_sample_id"],
                   ((study, patient, data[0]) for (study, patient), data in sorted(patients.items())), {2})
    _insert_stream(args, "wsi_part", ["cancer_study_id", "patient_id", "part_key", "part_number", "part_designator", "part_type", "part_description", "subspecialty", "path_dx_title"],
                   ((study, patient, part_key, *data) for (study, patient, part_key), data in sorted(parts.items())), {3, 4, 5, 6, 7, 8})
    _insert_stream(args, "wsi_block", ["cancer_study_id", "patient_id", "part_key", "block_key", "block_number", "block_label"],
                   ((study, patient, part_key, block_key, *data) for (study, patient, part_key, block_key), data in sorted(blocks.items())), {4, 5})
    _insert_stream(args, "wsi_slide", ["cancer_study_id", "patient_id", "image_id", "stain_name", "stain_group", "is_hne", "is_ihc", "magnification", "file_size_bytes", "can_serve_tiles", "barcode", "slide_type", "source_url", "tile_metadata_json", "thumbnail_url", "thumbnail_width", "thumbnail_height", "thumbnail_content_type"],
                   ((study, patient, image, values[16] or None, values[17] or None, values[18] == "TRUE", values[19] == "TRUE", values[20] or None, int(values[21]) if values[21] else None, values[24] == "TRUE", values[22] or None, values[23] or None, values[25] or None, values[26] or None, values[27] or None, int(values[28]) if values[28] else None, int(values[29]) if values[29] else None, values[30] or None) for study, patient, image, sample, values, timing in slides_for_insert), {3, 4, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17})
    # The generator above is exhausted after wsi_slide; rescan for placements
    # and timeline aggregation.
    placements = ((study, patient, image, values[4], values[11], sample, values[14], values[15])
                  for study, patient, image, sample, values, timing in slide_rows(count_rows=False))
    _insert_stream(args, "wsi_slide_placement", ["cancer_study_id", "patient_id", "image_id", "part_key", "block_key", "sample_id", "match_level", "specimen_key"], placements, {5})
    timings = (
        (
            study,
            patient,
            image,
            int(timing[0]) if timing[0] else None,
            timing[1] or "MISSING_PROCEDURE_DATE",
            timing[2] or "UNDATED",
            timing[3] or None,
            timing[4] or None,
            timing[5] or None,
            timing[6]
            or (
                "Verified estimated procedure date relative to first tumor sequencing"
                if timing[2] == "ESTIMATED"
                else "Recorded procedure date relative to first tumor sequencing"
                if timing[2] == "RECORDED"
                else timing[4] or timing[3] or "Procedure date unavailable"
            ),
        )
        for study, patient, image, _sample, _values, timing in slide_rows(count_rows=False)
    )
    _insert_stream(
        args,
        "wsi_slide_timing",
        [
            "cancer_study_id",
            "patient_id",
            "image_id",
            "timeline_start_days",
            "timeline_date_status",
            "timeline_date_kind",
            "timeline_date_source",
            "timeline_date_reason",
            "timeline_coordinate_system",
            "timepoint_source",
        ],
        timings,
        {3, 6, 7, 8, 9},
    )

    # Attributes are inserted after old rows were removed.  Counts include all
    # selected associations, including explicitly non-servable slides.
    _insert_stream(args, "clinical_attribute_meta", ["attr_id", "display_name", "description", "datatype", "patient_attribute", "priority", "cancer_study_id"],
                   ((attr_id, display, description, "NUMBER", patient_attribute, "1", study)
                    for study in study_ids for attr_id, display, description, patient_attribute in WSI_ATTRS))
    _insert_stream(args, "clinical_sample", ["internal_id", "attr_id", "attr_value"],
                   ((sample, attr_id, str(value)) for (study, sample), values in sorted(sample_counts.items())
                    for attr_id, value in zip((WSI_ATTRS[0][0], WSI_ATTRS[1][0], WSI_ATTRS[2][0]), values)))
    _insert_stream(args, "clinical_patient", ["internal_id", "attr_id", "attr_value"],
                   ((patient, attr_id, str(value)) for (study, patient), values in sorted(patient_counts.items())
                    for attr_id, value in zip((WSI_ATTRS[3][0], WSI_ATTRS[4][0], WSI_ATTRS[5][0]), values)))


def _timeline_association_rows(
    db: sqlite3.Connection,
    selected_study_id: int,
) -> list[dict[str, Any]]:
    """Convert staged slides to the shared timeline generator input shape."""
    if _TIMELINE_MODULE is None:
        raise RuntimeError("shared pathology timeline formatter is unavailable")
    index = {name: position for position, name in enumerate(DATA_COLUMNS)}
    rows: list[dict[str, Any]] = []
    for study, _patient_internal, image, _sample_internal, _reference, values_json, timing_json in _iter_slides(db):
        if study != selected_study_id:
            continue
        values = json.loads(values_json)
        timing = json.loads(timing_json)
        timing.extend([""] * (len(exporter.TIMELINE_COLUMNS) - len(timing)))
        start, status, kind, date_source, reason, coordinate_system, source = timing
        display_source = source or (
            "Verified estimated procedure date relative to first tumor sequencing"
            if kind == "ESTIMATED"
            else "Recorded procedure date relative to first tumor sequencing"
            if kind == "RECORDED"
            else reason or date_source or status
        )
        rows.append({
            "patient_id": values[index["PATIENT_ID"]],
            "sample_id": values[index["SAMPLE_ID"]] or None,
            "match_level": values[index["MATCH_LEVEL"]],
            "image_id": image,
            "part_key": values[index["PART_KEY"]],
            "part_number": values[index["PART_NUMBER"]] or None,
            "block_key": values[index["BLOCK_KEY"]],
            "block_number": values[index["BLOCK_NUMBER"]] or None,
            "block_label": values[index["BLOCK_LABEL"]] or None,
            "part_description": values[index["PART_DESCRIPTION"]] or None,
            "stain_name": values[index["STAIN_NAME"]] or None,
            "stain_group": values[index["STAIN_GROUP"]] or None,
            "is_hne": values[index["IS_HNE"]] == "TRUE",
            "is_ihc": values[index["IS_IHC"]] == "TRUE",
            "slide_path": values[index["SOURCE_URL"]] or None,
            "can_serve_tiles": values[index["CAN_SERVE_TILES"]] == "TRUE",
            "specimen_key": values[index["SPECIMEN_KEY"]],
            "timeline_start_days": start or None,
            "timeline_date_status": status or None,
            "timeline_date_kind": kind or None,
            "timeline_date_source": date_source or None,
            "timeline_date_reason": reason or None,
            "timeline_coordinate_system": coordinate_system or None,
            "slide_timepoint_source": display_source or None,
        })
    return rows


def _timeline_from_slides(
    db: sqlite3.Connection,
    study_stable_by_id: dict[int, str],
    patient_targets: dict[str, list[tuple[int, int]]],
) -> list[tuple[int, int, int | None, str, list[tuple[str, str]]]]:
    """Build database events through the same generator used for study files."""
    columns = _TIMELINE_MODULE.PATHOLOGY_TIMELINE_COLUMNS
    events = []
    for study_internal, study_stable in sorted(study_stable_by_id.items()):
        patient_internal_by_stable = {
            stable_id: internal_id
            for stable_id, matches in patient_targets.items()
            for target_study, internal_id in matches
            if target_study == study_internal
        }
        grouped_rows = _TIMELINE_MODULE.build_pathology_timeline_rows(
            _timeline_association_rows(db, study_internal),
            study_stable,
        )
        for row in grouped_rows:
            record = dict(zip(columns, row))
            patient_internal = patient_internal_by_stable.get(record["PATIENT_ID"])
            if patient_internal is None:
                raise RuntimeError(
                    f"timeline patient is not present in the staged portal cohort: {record['PATIENT_ID']}"
                )
            data = [
                (name, record[name])
                for name in columns[4:]
                if record[name] != ""
            ]
            events.append(
                (study_internal, patient_internal, int(record["START_DATE"]), None,
                 record["EVENT_TYPE"], data)
            )
    return events


def _insert_timeline(
    args: argparse.Namespace,
    db: sqlite3.Connection,
    study_stable_by_id: dict[int, str],
    patient_targets: dict[str, list[tuple[int, int]]],
) -> int:
    events = _timeline_from_slides(db, study_stable_by_id, patient_targets)
    max_id_rows = _query_rows(args, "SELECT coalesce(max(clinical_event_id), 0) FROM clinical_event FORMAT TSV")
    next_id = int(max_id_rows[0][0]) if max_id_rows else 0
    event_rows = []
    data_rows = []
    for offset, (_study, patient, start, stop, event_type, data) in enumerate(events, start=1):
        event_id = next_id + offset
        event_rows.append((event_id, patient, start, stop, event_type))
        data_rows.extend((event_id, key, value) for key, value in data if value != "")
    _insert_stream(args, "clinical_event", ["clinical_event_id", "patient_id", "start_date", "stop_date", "event_type"], event_rows, {3})
    _insert_stream(args, "clinical_event_data", ["clinical_event_id", "key", "value"], data_rows)
    return len(events)


def main() -> int:
    args = _args()
    if args.study_identifier and (args.study_inventory or args.write_study_inventory):
        raise RuntimeError("--study-identifier cannot be combined with inventory mode")
    if args.study_inventory and args.write_study_inventory:
        raise RuntimeError("--study-inventory and --write-study-inventory are mutually exclusive")
    if not args.clickhouse_config.is_file() or (args.clickhouse_config.stat().st_mode & 0o077):
        raise RuntimeError("ClickHouse config must exist and be mode 0600")
    if shutil.which(args.clickhouse_bin) is None and not Path(args.clickhouse_bin).is_file():
        raise RuntimeError(f"ClickHouse client not found: {args.clickhouse_bin}")
    prefixes = exporter._source_prefixes(args.allowed_source_prefixes)
    study_rows = _query_rows(args, "SELECT cancer_study_id, cancer_study_identifier FROM cancer_study FORMAT TSV")
    study_stable_by_id = {int(row[0]): row[1] for row in study_rows if row[1]}
    if args.write_study_inventory:
        inventory = _inventory_from_source(args, study_stable_by_id)
        inventory["inventory_sha256"] = _inventory_sha256(inventory)
        args.write_study_inventory.parent.mkdir(parents=True, exist_ok=True)
        args.write_study_inventory.write_text(
            json.dumps(inventory, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        print(json.dumps(inventory, sort_keys=True), flush=True)
        return 0

    if args.study_inventory and not args.timeline_generator_sha:
        raise RuntimeError(
            "--timeline-generator-sha or WSI_TIMELINE_GENERATOR_SHA is required "
            "for production hydration"
        )

    if not args.derived_tables_sql:
        raise RuntimeError(
            "--derived-tables-sql or CBIOPORTAL_DERIVED_TABLES_SQL is required for hydration"
        )
    args.derived_tables_sql = Path(args.derived_tables_sql).resolve()
    if not args.derived_tables_sql.is_file():
        raise RuntimeError(f"derived-table SQL does not exist: {args.derived_tables_sql}")
    derived_tables_sql_sha256 = _sha256(args.derived_tables_sql)
    timeline_generator_sha = (
        _validate_timeline_generator(args.timeline_generator_sha)
        if args.study_inventory or args.timeline_generator_sha
        else ""
    )
    _check_import_permissions(args)

    if args.study_identifier:
        selected_study_ids = {
            study_id
            for study_id, identifier in study_stable_by_id.items()
            if identifier == args.study_identifier
        }
        if not selected_study_ids:
            raise RuntimeError(
                f"requested study does not exist in ClickHouse: {args.study_identifier}"
            )
    elif args.study_inventory:
        inventory_ids = _read_study_inventory(
            args.study_inventory,
            args.canonical_table,
            args.registry_table,
            args.warehouse_id,
        )
        selected_study_ids = {
            study_id
            for study_id, identifier in study_stable_by_id.items()
            if identifier in inventory_ids
        }
        missing = sorted(set(inventory_ids) - set(study_stable_by_id.values()))
        if missing:
            raise RuntimeError(
                "study inventory contains studies missing from the target portal: "
                + ",".join(missing)
            )
    else:
        raise RuntimeError(
            "production hydration requires --study-inventory; use --study-identifier "
            "for a targeted dev repair"
        )
    patient_targets, sample_targets, patient_stable_by_internal = _load_target_maps(
        args, selected_study_ids
    )
    staging_path = args.staging_db.resolve() if args.staging_db else Path(tempfile.mkstemp(prefix="wsi_hydration_", suffix=".sqlite")[1])
    owns_staging = args.staging_db is None
    try:
        db = sqlite3.connect(staging_path)
        if args.staging_db:
            # Validation must finish before _cleanup.  In particular, a
            # legacy four-field timing snapshot is rejected here, while the
            # target ClickHouse tables remain untouched.
            stats = _validate_staging(
                db,
                selected_study_ids=selected_study_ids,
                prefixes=prefixes,
                timeline_generator_sha=timeline_generator_sha,
                derived_tables_sql_sha256=derived_tables_sql_sha256,
                allow_incomplete_assets=args.allow_incomplete_assets,
            )
            stats["resumed_from_staging"] = 1
        else:
            db.execute("PRAGMA journal_mode=OFF")
            db.execute("PRAGMA synchronous=OFF")
            _create_staging_schema(db)
            stats = _stage(
                args,
                db,
                patient_targets,
                sample_targets,
                patient_stable_by_internal,
                prefixes,
                require_complete_assets=not args.allow_incomplete_assets,
            )
            if not args.allow_incomplete_assets and stats.get("incomplete", 0):
                raise RuntimeError(
                    "Databricks WSI hydration found incomplete asset bundles; "
                    "refusing to mutate ClickHouse"
                )
            _write_staging_metadata(
                db,
                selected_study_ids=selected_study_ids,
                prefixes=prefixes,
                timeline_generator_sha=timeline_generator_sha,
                derived_tables_sql_sha256=derived_tables_sql_sha256,
                stats=stats,
                diagnostic=args.allow_incomplete_assets,
            )
            _validate_staging(
                db,
                selected_study_ids=selected_study_ids,
                prefixes=prefixes,
                timeline_generator_sha=timeline_generator_sha,
                derived_tables_sql_sha256=derived_tables_sql_sha256,
                allow_incomplete_assets=args.allow_incomplete_assets,
            )
        if stats.get("selected_rows", 0) == 0:
            raise RuntimeError("Databricks WSI data did not overlap any target patient/sample; refusing to mutate ClickHouse")
        if args.allow_incomplete_assets:
            print(
                "WARNING: --allow-incomplete-assets is diagnostic only; this hydration "
                "must not be treated as an accepted release",
                file=sys.stderr,
                flush=True,
            )
        study_ids = [row[0] for row in db.execute("SELECT DISTINCT study_id FROM slides ORDER BY study_id")]
        expected_study_ids = set(selected_study_ids)
        staged_study_ids = set(study_ids)
        if staged_study_ids != expected_study_ids:
            missing = sorted(expected_study_ids - staged_study_ids)
            unexpected = sorted(staged_study_ids - expected_study_ids)
            details = []
            if missing:
                details.append("missing=" + ",".join(map(str, missing)))
            if unexpected:
                details.append("unexpected=" + ",".join(map(str, unexpected)))
            raise RuntimeError(
                "staging database study scope does not match the selected release inventory: "
                + " ".join(details)
            )
        print(json.dumps({"stage": stats, "study_ids": study_ids, "allowed_source_prefixes": prefixes}, sort_keys=True), flush=True)
        _cleanup(args, study_ids)
        _populate_tables(args, db, study_ids)
        timeline_count = _insert_timeline(args, db, study_stable_by_id, patient_targets)
        db.close()
        # Rebuild all derived tables after WSI clinical attributes/events.
        print(f"inserted {timeline_count:,} pathology timeline events; rebuilding derived tables", flush=True)
        # Send the SQL contents to the client instead of passing a host path.
        # The production client is often containerized, so a path visible to
        # this process is not necessarily visible inside the ClickHouse client
        # container.  ``--query`` also keeps this invocation compatible with
        # the migration client wrapper used by beta and release jobs.
        try:
            populate_sql = args.derived_tables_sql.read_text(encoding="utf-8")
        except OSError as exc:
            raise RuntimeError(
                f"cannot read derived-table rebuild SQL: {args.derived_tables_sql}"
            ) from exc
        command = [args.clickhouse_bin, "client", "--config-file", str(args.clickhouse_config),
                   "--database", args.database, "--mutations_sync", "2", "--multiquery",
                   "--query", populate_sql, "--param_optimize_backoff_secs", "0"]
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        if result.returncode:
            raise RuntimeError(f"derived-table rebuild failed: {result.stderr[-4000:]}")
        print(
            json.dumps(
                {
                    "hydrated": True,
                    "selected_rows": stats["selected_rows"],
                    "study_count": len(study_ids),
                    "timeline_event_count": timeline_count,
                    "derived_tables_sql_sha256": derived_tables_sql_sha256,
                    "timeline_generator_sha": timeline_generator_sha,
                },
                sort_keys=True,
            ),
            flush=True,
        )
    finally:
        db_path_exists = staging_path.exists()
        try:
            db.close()
        except Exception:
            pass
        if owns_staging and not args.keep_staging and db_path_exists:
            staging_path.unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"WSI hydration failed: {exc}", file=sys.stderr)
        raise
