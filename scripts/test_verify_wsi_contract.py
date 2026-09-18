#!/usr/bin/env python3
"""Dependency-free contract tests for the stack WSI release gates."""

from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from builtins import ValueError
from argparse import Namespace
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parent
E2E_SCRIPT = (ROOT / "verify-databricks-wsi-e2e.sh").read_text()
STACK_SCRIPT = (ROOT / "verify-stack-release.sh").read_text()
HYDRATE_SCRIPT = (ROOT / "hydrate_databricks_wsi_clickhouse.py").read_text()
MOLECULAR_HYDRATE_SCRIPT = (ROOT / "hydrate-study-molecular.sh").read_text()


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to import {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


VERIFY = _load("verify_study_load", ROOT / "verify-study-load.py")
STACK = _load("verify_stack_release", ROOT / "verify-stack-release.py")
EXPORT = _load("export_databricks_wsi_snapshot", ROOT / "export_databricks_wsi_snapshot.py")
RECONCILE = _load(
    "reconcile_pathology_timeline_capabilities",
    ROOT / "reconcile_pathology_timeline_capabilities.py",
)
HYDRATE = _load(
    "hydrate_databricks_wsi_clickhouse",
    ROOT / "hydrate_databricks_wsi_clickhouse.py",
)


def _write_timeline_pair(directory: Path) -> None:
    (directory / "meta_clinical_timeline_pathology_slides.txt").write_text(
        "cancer_study_identifier: study_a\n"
        "data_filename: data_clinical_timeline_pathology_slides.txt\n",
        encoding="utf-8",
    )
    (directory / "data_clinical_timeline_pathology_slides.txt").write_text(
        "PATIENT_ID\tSTART_DATE\tEVENT_TYPE\tIMAGE_IDS\n", encoding="utf-8"
    )


class PortalTileContractTests(unittest.TestCase):
    def test_release_verifiers_use_dev_tables_and_bound_event_requests(self):
        self.assertIn("WSI_TILE_SERVER_ROOT", HYDRATE_SCRIPT)
        self.assertIn("WSI_TILE_SERVER_ROOT", (ROOT / "export_databricks_wsi_snapshot.py").read_text())
        self.assertIn('databricks_target="${DATABRICKS_TARGET:-dev}"', E2E_SCRIPT)
        self.assertIn(
            'canonical_default="cdsi_prod.pathology_data_mining_dev.canonical_slide_associations"',
            E2E_SCRIPT,
        )
        self.assertIn("s3://ocra/", E2E_SCRIPT)
        self.assertIn(
            '--timeline-patient-sample "${TIMELINE_PATIENT_SAMPLE:-0}"',
            E2E_SCRIPT,
        )
        self.assertIn(
            'args+=(--timeline-patient-sample "${TIMELINE_PATIENT_SAMPLE:-0}")',
            STACK_SCRIPT,
        )

    def test_hydration_is_strict_by_default(self):
        self.assertIn('"--allow-incomplete-assets"', HYDRATE_SCRIPT)
        self.assertIn('"--study-identifier"', HYDRATE_SCRIPT)
        self.assertIn('"--study-inventory"', HYDRATE_SCRIPT)
        self.assertIn('"--write-study-inventory"', HYDRATE_SCRIPT)
        self.assertIn('"--derived-tables-sql"', HYDRATE_SCRIPT)
        self.assertIn('"--timeline-generator-sha"', HYDRATE_SCRIPT)
        self.assertIn(
            "require_complete_assets=not args.allow_incomplete_assets",
            HYDRATE_SCRIPT,
        )
        self.assertIn('VERIFY_ALL_ACCESS:-1', E2E_SCRIPT)

    def _valid_staging_row(self, *, can_serve: str = "FALSE"):
        values = [""] * len(HYDRATE.DATA_COLUMNS)
        for name, value in {
            "PATIENT_ID": "P-1",
            "IMAGE_ID": "slide-1",
            "MATCH_LEVEL": "UNMATCHED",
            "SPECIMEN_KEY": "unmatched::part:unknown::block:unknown",
            "CAN_SERVE_TILES": can_serve,
        }.items():
            values[HYDRATE.DATA_COLUMNS.index(name)] = value
        timing = [
            "0",
            "AVAILABLE",
            "RECORDED",
            "procedure_date",
            "",
            HYDRATE.TIMELINE_COORDINATE_SYSTEM,
            "Recorded procedure date relative to first tumor sequencing",
        ]
        return values, timing

    def test_staging_validation_rejects_legacy_snapshot_before_cleanup(self):
        with tempfile.TemporaryDirectory() as temporary:
            db = HYDRATE.sqlite3.connect(Path(temporary) / "legacy.sqlite")
            db.execute(
                "CREATE TABLE slides (study_id INTEGER, patient_internal INTEGER, image_id TEXT, "
                "sample_internal INTEGER, reference_internal INTEGER, rank_key TEXT, "
                "values_json TEXT, timing_json TEXT)"
            )
            values, timing = self._valid_staging_row()
            db.execute(
                "INSERT INTO slides VALUES(?,?,?,?,?,?,?,?)",
                (1, 1, "slide-1", None, None, "2|1|1|", json.dumps(values), json.dumps(timing[:4])),
            )
            db.commit()
            with self.assertRaisesRegex(RuntimeError, "missing v3 metadata"):
                HYDRATE._validate_staging(
                    db,
                    selected_study_ids={1},
                    prefixes=("s3://pathology/",),
                    timeline_generator_sha="",
                    derived_tables_sql_sha256="derived",
                    allow_incomplete_assets=False,
                )
            db.close()

    def test_staging_validation_checks_v3_rows_and_release_bindings(self):
        with tempfile.TemporaryDirectory() as temporary:
            db = HYDRATE.sqlite3.connect(Path(temporary) / "valid.sqlite")
            HYDRATE._create_staging_schema(db)
            values, timing = self._valid_staging_row()
            db.execute(
                "INSERT INTO slides VALUES(?,?,?,?,?,?,?,?)",
                (1, 1, "slide-1", None, None, "2|1|1|", json.dumps(values), json.dumps(timing)),
            )
            stats = {"selected_rows": 1, "study_count": 1, "incomplete": 0}
            prefixes = ("s3://pathology/", "s3://mskmind-bkt/", "s3://ocra/")
            HYDRATE._write_staging_metadata(
                db,
                selected_study_ids={1},
                prefixes=prefixes,
                timeline_generator_sha="",
                derived_tables_sql_sha256="derived",
                stats=stats,
                diagnostic=False,
            )
            validated = HYDRATE._validate_staging(
                db,
                selected_study_ids={1},
                prefixes=prefixes,
                timeline_generator_sha="",
                derived_tables_sql_sha256="derived",
                allow_incomplete_assets=False,
            )
            self.assertEqual(validated["selected_rows"], 1)
            with self.assertRaisesRegex(RuntimeError, "does not match"):
                HYDRATE._validate_staging(
                    db,
                    selected_study_ids={1},
                    prefixes=("s3://ocra/",),
                    timeline_generator_sha="",
                    derived_tables_sql_sha256="derived",
                    allow_incomplete_assets=False,
                )
            db.close()

    def test_staging_validation_rejects_incomplete_servable_bundle(self):
        with tempfile.TemporaryDirectory() as temporary:
            db = HYDRATE.sqlite3.connect(Path(temporary) / "incomplete.sqlite")
            HYDRATE._create_staging_schema(db)
            values, timing = self._valid_staging_row(can_serve="TRUE")
            db.execute(
                "INSERT INTO slides VALUES(?,?,?,?,?,?,?,?)",
                (1, 1, "slide-1", None, None, "2|1|0|", json.dumps(values), json.dumps(timing)),
            )
            HYDRATE._write_staging_metadata(
                db,
                selected_study_ids={1},
                prefixes=("s3://pathology/",),
                timeline_generator_sha="",
                derived_tables_sql_sha256="derived",
                stats={"selected_rows": 1, "study_count": 1, "incomplete": 1},
                diagnostic=True,
            )
            with self.assertRaisesRegex(RuntimeError, "incomplete asset bundle"):
                HYDRATE._validate_staging(
                    db,
                    selected_study_ids={1},
                    prefixes=("s3://pathology/",),
                    timeline_generator_sha="",
                    derived_tables_sql_sha256="derived",
                    allow_incomplete_assets=True,
                )
            db.close()

    def test_production_inventory_is_derived_from_canonical_patient_sample_overlap(self):
        args = Namespace(
            canonical_table="prod.canonical.associations",
            registry_table="prod.registry.slides",
            warehouse_id="warehouse",
        )
        targets = (
            {"P-1": [(10, 100)], "P-2": [(20, 200)]},
            {"S-1": [(10, 101, 100, "P-1")]},
            {},
        )
        records = [
            {"patient_id": "P-1", "sample_id": "S-1"},
            {"patient_id": "P-2", "sample_id": ""},
        ]
        with mock.patch.object(HYDRATE, "_load_target_maps", return_value=targets), mock.patch.object(
            HYDRATE.exporter, "_run_export_query", return_value=iter(records)
        ):
            inventory = HYDRATE._inventory_from_source(args, {10: "study_a", 20: "study_b"})

        self.assertEqual(inventory["kind"], "canonical_impact_sample_membership")
        self.assertEqual(
            [study["study_id"] for study in inventory["studies"]], ["study_a", "study_b"]
        )
        self.assertEqual(inventory["studies"][0]["canonical_row_count"], 1)

    def test_inventory_rejects_a_different_canonical_source(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "inventory.json"
            path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "kind": "canonical_impact_sample_membership",
                        "canonical_table": "dev.canonical.associations",
                        "studies": [{"study_id": "study_a"}],
                    }
                ),
                encoding="utf-8",
            )
            value = json.loads(path.read_text(encoding="utf-8"))
            value["inventory_sha256"] = HYDRATE._inventory_sha256(value)
            path.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "does not match"):
                HYDRATE._read_study_inventory(
                    path,
                    "prod.canonical.associations",
                    "prod.registry.slides",
                    "warehouse",
                )

    def test_molecular_hydration_is_bulk_replace_and_covers_all_categories(self):
        self.assertIn("ALTER TABLE mutation DELETE", MOLECULAR_HYDRATE_SCRIPT)
        self.assertIn("ALTER TABLE sample_cna_event DELETE", MOLECULAR_HYDRATE_SCRIPT)
        self.assertIn("ALTER TABLE structural_variant DELETE", MOLECULAR_HYDRATE_SCRIPT)
        self.assertIn("ImportCopyNumberSegmentData", MOLECULAR_HYDRATE_SCRIPT)
        self.assertIn("ImportGenePanelProfileMap", MOLECULAR_HYDRATE_SCRIPT)
        self.assertIn("build_pathology_timeline_rows", HYDRATE_SCRIPT)
        self.assertNotIn("groups.setdefault(key", HYDRATE_SCRIPT)
        self.assertIn('profile_id "$mutation_meta" MUTATION_EXTENDED', MOLECULAR_HYDRATE_SCRIPT)
        self.assertIn('stable_id = \'$stable_id\'', MOLECULAR_HYDRATE_SCRIPT)
        self.assertNotIn("VERIFY_AFTER_HYDRATION", MOLECULAR_HYDRATE_SCRIPT)
        self.assertNotIn("--overwrite-existing --meta", MOLECULAR_HYDRATE_SCRIPT)

    def test_gene_alias_map_preserves_ambiguous_aliases(self):
        with mock.patch.object(
            VERIFY,
            "_clickhouse_query",
            side_effect=[
                [["1", "CANONICAL"], ["2", "SHARED"]],
                [["3", "SHARED"], ["4", "ALIAS"], ["5", "ALIAS"]],
            ],
        ):
            symbols = VERIFY._gene_symbol_map(Namespace())
        self.assertEqual(symbols["SHARED"], {2})
        self.assertEqual(symbols["ALIAS"], {4, 5})

    def test_profile_lookup_uses_metadata_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            meta = Path(temporary) / "meta_mutations.txt"
            meta.write_text(
                "stable_id: mutations\n"
                "genetic_alteration_type: MUTATION_EXTENDED\n"
                "datatype: MAF\n",
                encoding="utf-8",
            )
            with mock.patch.object(
                VERIFY, "_clickhouse_query", return_value=[["25"]]
            ) as query:
                profile_id = VERIFY._profile_id(Namespace(), "study_a", meta)
        self.assertEqual(profile_id, 25)
        sql = query.call_args.args[1]
        self.assertIn("stable_id = 'study_a_mutations'", sql)
        self.assertIn("genetic_alteration_type = 'MUTATION_EXTENDED'", sql)
        self.assertIn("datatype = 'MAF'", sql)

    def test_hydration_checks_permissions_before_mutating(self):
        args = Namespace(database="cbioportal_msk_beta", import_role="beta_wsi_import_role")
        with mock.patch.object(
            HYDRATE,
            "_run_client",
            return_value=(
                "GRANT SELECT ON cbioportal_msk_beta.* TO beta_wsi_import_role\n"
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "complete WSI hydration"):
                HYDRATE._check_import_permissions(args)

    def test_release_gate_requests_every_access_bundle(self):
        with tempfile.TemporaryDirectory() as temporary:
            study_dir = Path(temporary)
            (study_dir / "meta_wsi.txt").write_text(
                "cancer_study_identifier: study_a\n", encoding="utf-8"
            )
            args = Namespace(
                portal_url="https://portal.example.test",
                clickhouse_container="clickhouse",
                clickhouse_user="cbio_user",
                clickhouse_database="cbioportal",
                timeline_patient_sample=24,
                wsi_sample_size=3,
                expected_tile_url="",
                cookie="",
                study_timeout_seconds=60,
            )
            completed = mock.Mock(stdout=json.dumps({"status": "accepted"}))
            study = {
                "study_id": "study_a",
                "study_dir": str(study_dir),
                "timeline_dir": str(study_dir),
                "declared_files": [],
            }
            with mock.patch.object(STACK.subprocess, "run", return_value=completed) as run:
                STACK._verify_study(args, study, check_all_tiles=False)
            command = run.call_args.args[0]
            self.assertIn("--check-all-access", command)
            self.assertNotIn("--check-access", command)

    def test_release_gate_requires_molecular_data_by_default(self):
        with tempfile.TemporaryDirectory() as temporary:
            completed = mock.Mock(stdout=json.dumps({"status": "accepted"}))
            args = Namespace(
                portal_url="https://portal.example.test",
                clickhouse_container="clickhouse",
                clickhouse_user="cbio_user",
                clickhouse_database="cbioportal",
                timeline_patient_sample=0,
                wsi_sample_size=1,
                expected_tile_url="",
                cookie="",
                study_timeout_seconds=60,
            )
            study = {
                "study_id": "study_a",
                "study_dir": temporary,
                "timeline_dir": temporary,
                "declared_files": [],
            }
            with mock.patch.object(STACK.subprocess, "run", return_value=completed) as run:
                STACK._verify_study(args, study, check_all_tiles=False)
            self.assertIn("--check-all-data", run.call_args.args[0])

    def test_portal_config_is_authoritative_and_cross_origin_is_preflighted(self):
        args = Namespace(cookie="", expected_tile_url="https://tiles.example.test/")
        with mock.patch.object(
            VERIFY, "_request_json", return_value={"msk_wsi_tile_server_url": "https://tiles.example.test"}
        ), mock.patch.object(VERIFY, "_request_cors_preflight") as preflight:
            tile_url, origin = VERIFY._portal_tile_server(args, "https://portal.example.test")

        self.assertEqual(tile_url, "https://tiles.example.test")
        self.assertEqual(origin, "https://portal.example.test")
        self.assertEqual(
            [call.args for call in preflight.call_args_list],
            [
                ("https://tiles.example.test/thumbnails", "https://portal.example.test"),
                ("https://tiles.example.test/tiles/zxy/0/0/0", "https://portal.example.test"),
            ],
        )

    def test_portal_config_mismatch_fails_even_when_legacy_tile_url_is_set(self):
        args = Namespace(
            cookie="", expected_tile_url="https://wrong.example.test", tile_url="https://wrong.example.test"
        )
        with mock.patch.object(
            VERIFY, "_request_json", return_value={"msk_wsi_tile_server_url": "https://tiles.example.test"}
        ):
            with self.assertRaisesRegex(VERIFY.VerificationError, "differs from the expected"):
                VERIFY._portal_tile_server(args, "https://portal.example.test")

    def test_sample_selection_spans_patients_and_is_stable(self):
        wsi = {
            "servable_image_ids": {
                "patient-b": {"slide-b2", "slide-b1"},
                "patient-a": {"slide-a1"},
            }
        }
        slides = [
            {"imageId": "slide-b2", "canServeTiles": True},
            {"imageId": "slide-a1", "canServeTiles": True},
            {"imageId": "slide-b1", "canServeTiles": True},
            {"imageId": "not-servable", "canServeTiles": False},
        ]
        sample = VERIFY._select_wsi_sample(wsi, slides, 2)
        self.assertEqual([slide["imageId"] for slide in sample], ["slide-a1", "slide-b1"])

    def test_timeline_patient_sampling_is_bounded_and_event_focused(self):
        patients = ["patient-a", "patient-b", "patient-c", "patient-d"]
        metrics = {
            "patient-a": {"events": 1},
            "patient-c": {"events": 2},
            "patient-d": {"events": 3},
        }
        self.assertEqual(
            VERIFY._select_timeline_patients(patients, metrics, 2),
            ["patient-a", "patient-d"],
        )
        self.assertEqual(
            VERIFY._select_timeline_patients(patients, metrics, 0),
            ["patient-a", "patient-c", "patient-d"],
        )

    def test_timeline_capability_check_rejects_non_servable_linkout(self):
        event = {
            "attributes": [
                {"key": "IMAGE_COUNT", "value": "2"},
                {"key": "NON_SERVABLE_IMAGE_COUNT", "value": "0"},
                {"key": "TOTAL_IMAGE_COUNT", "value": "2"},
                {
                    "key": "LINKOUT",
                    "value": (
                        "/patient/wsiHESlides?caseId=P-1&sampleId=S-1&"
                        "stainFilter=hne&matchLevel=BLOCK&specimenKey=block::part:1::block:A1"
                    ),
                },
            ]
        }
        slides = [
            {
                "imageId": "slide-1",
                "sampleId": "S-1",
                "matchLevel": "BLOCK",
                "specimenKey": "block::part:1::block:A1",
                "isHne": True,
                "isIhc": False,
                "canServeTiles": False,
            },
            {
                "imageId": "slide-2",
                "sampleId": "S-1",
                "matchLevel": "BLOCK",
                "specimenKey": "block::part:1::block:A1",
                "isHne": True,
                "isIhc": False,
                "canServeTiles": False,
            },
        ]
        with self.assertRaisesRegex(
            VERIFY.VerificationError, "non-servable WSI group"
        ):
            VERIFY._validate_pathology_timeline_capability(
                [event], {"P-1": slides}
            )

    def test_timeline_capability_check_accepts_non_servable_event_without_linkout(self):
        event = {
            "attributes": [
                {"key": "IMAGE_COUNT", "value": "0"},
                {"key": "NON_SERVABLE_IMAGE_COUNT", "value": "2"},
                {"key": "TOTAL_IMAGE_COUNT", "value": "2"},
            ]
        }
        self.assertEqual(
            VERIFY._validate_pathology_timeline_capability([event], {"P-1": []}),
            1,
        )

    def test_timeline_capability_check_rejects_unaccounted_slides_without_linkout(self):
        event = {
            "attributes": [
                {"key": "IMAGE_COUNT", "value": "0"},
                {"key": "NON_SERVABLE_IMAGE_COUNT", "value": "0"},
                {"key": "TOTAL_IMAGE_COUNT", "value": "1"},
            ]
        }
        with self.assertRaisesRegex(VERIFY.VerificationError, "inconsistent"):
            VERIFY._validate_pathology_timeline_capability([event], {"P-1": []})


class DatabricksExportContractTests(unittest.TestCase):
    @staticmethod
    def _study_fixture(root: Path) -> Path:
        study_dir = root / "study_a"
        study_dir.mkdir()
        (study_dir / "meta_study.txt").write_text(
            "cancer_study_identifier: study_a\n", encoding="utf-8"
        )
        (study_dir / "data_clinical_sample.txt").write_text(
            "PATIENT_ID\tSAMPLE_ID\nP-1\tS-1\n", encoding="utf-8"
        )
        return study_dir

    def test_export_rejects_a_cohort_row_that_would_be_filtered(self):
        with tempfile.TemporaryDirectory() as temporary:
            study_dir = self._study_fixture(Path(temporary))
            record = {
                "patient_id": "P-1",
                "sample_id": "",
                "match_level": "INVALID",
                "image_id": "slide-1",
                "can_serve_tiles": False,
            }
            with mock.patch.object(EXPORT, "_run_external_query", return_value=[record]):
                with self.assertRaisesRegex(ValueError, "filtered 1 rows"):
                    with mock.patch.object(
                        EXPORT.sys, "argv", ["export", "--study-dir", str(study_dir)]
                    ):
                        EXPORT.main()

    def test_export_rejects_a_study_with_no_servable_assets(self):
        with tempfile.TemporaryDirectory() as temporary:
            study_dir = self._study_fixture(Path(temporary))
            record = {
                "patient_id": "P-1",
                "match_level": "UNMATCHED",
                "image_id": "slide-1",
                "can_serve_tiles": False,
            }
            with mock.patch.object(EXPORT, "_run_external_query", return_value=[record]):
                with self.assertRaisesRegex(ValueError, "no servable WSI assets"):
                    with mock.patch.object(
                        EXPORT.sys, "argv", ["export", "--study-dir", str(study_dir)]
                    ):
                        EXPORT.main()

    def test_tile_metadata_accepts_date_like_sha256_fingerprint(self):
        metadata = {
            "dimensions": {"width": 100, "height": 80},
            "levels": 1,
            "level_dimensions": [{"width": 100, "height": 80}],
            "max_zoom": 0,
            "tile_size": 256,
            # Contains the date-like substring 20395333, which is valid
            # inside a digest and must not be treated as PHI.
            "source_fingerprint": "a" * 10 + "20395333" + "b" * 46,
        }
        self.assertTrue(EXPORT._metadata_is_safe_and_valid(metadata))

    def test_export_preserves_a_matched_row_marked_non_servable(self):
        with tempfile.TemporaryDirectory() as temporary:
            study_dir = self._study_fixture(Path(temporary))
            record = {
                "patient_id": "P-1",
                "sample_id": "S-1",
                "match_level": "PART",
                "image_id": "slide-1",
                "can_serve_tiles": False,
            }
            values = EXPORT._row(
                record,
                {"P-1"},
                {"S-1"},
                {"S-1": "P-1"},
                require_complete_assets=True,
                asset_stats={"canonical_servable": 0, "incomplete": 0},
            )
            self.assertIsNotNone(values)
            self.assertEqual(values[EXPORT.DATA_COLUMNS.index("CAN_SERVE_TILES")], "FALSE")
            self.assertEqual(values[EXPORT.DATA_COLUMNS.index("SOURCE_URL")], "")

    def test_export_downgrades_sample_outside_study_to_unmatched(self):
        record = {
            "patient_id": "P-1",
            "sample_id": "S-not-in-study",
            "match_level": "PART",
            "image_id": "slide-1",
            "can_serve_tiles": False,
        }
        values = EXPORT._row(
            record,
            {"P-1"},
            {"S-1"},
            {"S-1": "P-1"},
            require_complete_assets=True,
            asset_stats={"canonical_servable": 0, "incomplete": 0},
        )
        self.assertIsNotNone(values)
        self.assertEqual(values[EXPORT.DATA_COLUMNS.index("MATCH_LEVEL")], "UNMATCHED")
        self.assertEqual(values[EXPORT.DATA_COLUMNS.index("SAMPLE_ID")], "")

    def test_export_fails_closed_for_canonical_servable_row_without_metadata(self):
        record = {
            "patient_id": "P-1",
            "match_level": "UNMATCHED",
            "image_id": "slide-1",
            "can_serve_tiles": True,
            "slide_path": "s3://mskmind-bkt/reef-slides/slide-1.svs",
            "serving_artifact_uri": "s3://mskmind-bkt/wsi-thumbnails/slide-1.jpg",
            "tile_metadata_json": "",
        }
        with self.assertRaisesRegex(ValueError, "asset contract is incomplete"):
            EXPORT._row(
                record,
                {"P-1"},
                set(),
                {},
                require_complete_assets=True,
                asset_stats={"canonical_servable": 0, "incomplete": 0},
            )

    def test_export_keeps_known_failed_registry_row_as_non_servable(self):
        record = {
            "patient_id": "P-1",
            "match_level": "UNMATCHED",
            "image_id": "slide-failed",
            "can_serve_tiles": True,
            "registry_status": "failed",
        }
        stats = {"canonical_servable": 0, "incomplete": 0, "registry_failed": 0}
        values = EXPORT._row(
            record,
            {"P-1"},
            set(),
            {},
            require_complete_assets=True,
            asset_stats=stats,
        )
        self.assertIsNotNone(values)
        self.assertEqual(values[EXPORT.DATA_COLUMNS.index("CAN_SERVE_TILES")], "FALSE")
        self.assertEqual(stats["registry_failed"], 1)
        self.assertEqual(stats["incomplete"], 0)

    def test_export_keeps_registry_success_with_invalid_metadata_non_servable(self):
        record = {
            "patient_id": "P-1",
            "match_level": "UNMATCHED",
            "image_id": "slide-invalid-metadata",
            "can_serve_tiles": True,
            "registry_status": "success",
            "slide_path": "s3://mskmind-bkt/reef-slides/slide-invalid-metadata.svs",
            "artifact_uri": "s3://mskmind-bkt/wsi-thumbnails/masters/slide-invalid-metadata.jpg",
            "tile_metadata_json": "{}",
        }
        stats = {"canonical_servable": 0, "incomplete": 0, "registry_invalid": 0}
        values = EXPORT._row(
            record,
            {"P-1"},
            set(),
            {},
            require_complete_assets=True,
            asset_stats=stats,
        )
        self.assertIsNotNone(values)
        self.assertEqual(values[EXPORT.DATA_COLUMNS.index("CAN_SERVE_TILES")], "FALSE")
        self.assertEqual(stats["registry_invalid"], 1)
        self.assertEqual(stats["incomplete"], 0)

    def test_export_diagnostic_escape_hatch_keeps_the_association_non_servable(self):
        record = {
            "patient_id": "P-1",
            "match_level": "UNMATCHED",
            "image_id": "slide-1",
            "can_serve_tiles": True,
            "slide_path": "s3://mskmind-bkt/reef-slides/slide-1.svs",
            "serving_artifact_uri": "s3://mskmind-bkt/wsi-thumbnails/slide-1.jpg",
            "tile_metadata_json": "",
        }
        values = EXPORT._row(
            record,
            {"P-1"},
            set(),
            {},
            require_complete_assets=False,
            asset_stats={"canonical_servable": 0, "incomplete": 0},
        )
        self.assertIsNotNone(values)
        self.assertEqual(values[EXPORT.DATA_COLUMNS.index("CAN_SERVE_TILES")], "FALSE")

    def test_export_accepts_all_configured_s3_source_prefixes(self):
        record = {
            "patient_id": "P-1",
            "match_level": "UNMATCHED",
            "image_id": "slide-pathology",
            "can_serve_tiles": True,
            "slide_path": "s3://pathology/CRC_21-167/slides/slide-pathology.svs",
            "artifact_uri": "s3://mskmind-bkt/wsi-thumbnails/slide-pathology.jpg",
            "tile_metadata_json": json.dumps(
                {
                    "dimensions": {"width": 1024, "height": 1024},
                    "levels": 1,
                    "level_dimensions": [{"width": 1024, "height": 1024}],
                    "max_zoom": 0,
                    "tile_size": 256,
                }
            ),
            "width": 1024,
            "height": 1024,
            "content_type": "image/jpeg",
        }
        prefixes = EXPORT._source_prefixes(
            [
                "s3://mskmind-bkt/reef-slides/",
                "s3://pathology/CRC_21-167/slides/",
            ]
        )
        values = EXPORT._row(
            record,
            {"P-1"},
            set(),
            {},
            require_complete_assets=True,
            asset_stats={"canonical_servable": 0, "incomplete": 0},
            allowed_source_prefixes=prefixes,
        )
        self.assertIsNotNone(values)
        self.assertEqual(values[EXPORT.DATA_COLUMNS.index("CAN_SERVE_TILES")], "TRUE")

    def test_source_prefixes_reject_invalid_s3_prefixes(self):
        with self.assertRaisesRegex(ValueError, "invalid S3 prefix"):
            EXPORT._source_prefixes(["s3://"])

    def test_default_source_prefixes_match_tile_server_policy(self):
        with mock.patch.dict(EXPORT.os.environ, {EXPORT._SOURCE_PREFIX_ENV: ""}, clear=False):
            self.assertEqual(
                EXPORT._source_prefixes(),
                ("s3://pathology/", "s3://mskmind-bkt/", "s3://ocra/"),
            )

    def test_timeline_record_uses_final_wsi_capability(self):
        values = [""] * len(EXPORT.DATA_COLUMNS)
        for name, value in {
            "PATIENT_ID": "P-1",
            "SAMPLE_ID": "S-1",
            "IMAGE_ID": "slide-1",
            "MATCH_LEVEL": "BLOCK",
            "SPECIMEN_KEY": "block::part:1::block:A1",
            "IS_HNE": "TRUE",
            "IS_IHC": "FALSE",
            "CAN_SERVE_TILES": "FALSE",
        }.items():
            values[EXPORT.DATA_COLUMNS.index(name)] = value
        values.extend(("-5", "AVAILABLE", "patient_first_tumor_sequencing_day_zero", "source"))

        record = EXPORT._timeline_record(values)

        self.assertFalse(record["can_serve_tiles"])


class ReleaseManifestTests(unittest.TestCase):
    def test_contains_manifest_allows_unrelated_catalog_studies_and_zero_wsi_entry(self):
        with tempfile.TemporaryDirectory() as temporary:
            study_dir = Path(temporary) / "study"
            study_dir.mkdir()
            (study_dir / "meta_study.txt").write_text(
                "cancer_study_identifier: study_a\n", encoding="utf-8"
            )
            manifest = Path(temporary) / "manifest.json"
            manifest.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "catalog_policy": "contains",
                        "inventory": {
                            "kind": "canonical_impact_sample_membership",
                            "studies": [{"study_id": "study_a"}],
                        },
                        "studies": [
                            {
                                "study_id": "study_a",
                                "study_dir": str(study_dir),
                                "require_wsi": False,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            value = json.loads(manifest.read_text(encoding="utf-8"))
            value["inventory"]["inventory_sha256"] = STACK._inventory_sha256(value["inventory"])
            manifest.write_text(json.dumps(value), encoding="utf-8")
            policy, studies = STACK._read_manifest(manifest)
            self.assertEqual(policy, "contains")
            self.assertFalse(studies[0]["require_wsi"])

    def test_contains_manifest_rejects_inventory_coverage_gap(self):
        with tempfile.TemporaryDirectory() as temporary:
            study_dir = Path(temporary) / "study"
            study_dir.mkdir()
            (study_dir / "meta_study.txt").write_text(
                "cancer_study_identifier: study_a\n", encoding="utf-8"
            )
            manifest = Path(temporary) / "manifest.json"
            manifest.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "catalog_policy": "contains",
                        "inventory": {
                            "kind": "canonical_impact_sample_membership",
                            "studies": [
                                {"study_id": "study_a"},
                                {"study_id": "study_b"},
                            ],
                        },
                        "studies": [
                            {
                                "study_id": "study_a",
                                "study_dir": str(study_dir),
                                "require_wsi": False,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            value = json.loads(manifest.read_text(encoding="utf-8"))
            value["inventory"]["inventory_sha256"] = STACK._inventory_sha256(value["inventory"])
            manifest.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(STACK.VerificationError, "coverage does not match"):
                STACK._read_manifest(manifest)

    def test_manifest_requires_pixel_snapshot_and_complete_timeline_pair(self):
        with tempfile.TemporaryDirectory() as temporary:
            study_dir = Path(temporary) / "study"
            study_dir.mkdir()
            (study_dir / "meta_wsi.txt").write_text(
                "cancer_study_identifier: study_a\ndata_filename: data_wsi.txt\n", encoding="utf-8"
            )
            (study_dir / "data_wsi.txt").write_text("PATIENT_ID\tIMAGE_ID\nP\tI\n", encoding="utf-8")
            (study_dir / "wsi_snapshot_manifest.json").write_text(
                json.dumps(
                    {
                        "study_id": "study_a",
                        "association_row_count": 1,
                        "servable_row_count": 1,
                        "patient_count": 1,
                        "incomplete_asset_count": 0,
                        "filtered_row_count": 0,
                    }
                ),
                encoding="utf-8",
            )
            manifest = Path(temporary) / "manifest.json"
            manifest.write_text(
                json.dumps({"version": 1, "studies": [{"study_id": "study_a", "study_dir": str(study_dir)}]}),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(STACK.VerificationError, "missing the pathology timeline"):
                STACK._read_manifest(manifest)
            _write_timeline_pair(study_dir)
            policy, studies = STACK._read_manifest(manifest)
            self.assertEqual(policy, "exact")
            self.assertEqual(studies[0]["study_id"], "study_a")

            timeline_dir = Path(temporary) / "timeline"
            timeline_dir.mkdir()
            (timeline_dir / "meta_clinical_timeline_pathology_slides.txt").write_text("", encoding="utf-8")
            manifest.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "studies": [
                            {
                                "study_id": "study_a",
                                "study_dir": str(study_dir),
                                "timeline_dir": str(timeline_dir),
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(STACK.VerificationError, "incomplete pathology timeline"):
                STACK._read_manifest(manifest)

    def test_manifest_rejects_incomplete_wsi_assets(self):
        with tempfile.TemporaryDirectory() as temporary:
            study_dir = Path(temporary) / "study"
            study_dir.mkdir()
            (study_dir / "meta_wsi.txt").write_text(
                "cancer_study_identifier: study_a\ndata_filename: data_wsi.txt\n",
                encoding="utf-8",
            )
            (study_dir / "data_wsi.txt").write_text("PATIENT_ID\tIMAGE_ID\nP\tI\n", encoding="utf-8")
            _write_timeline_pair(study_dir)
            (study_dir / "wsi_snapshot_manifest.json").write_text(
                json.dumps(
                    {
                        "study_id": "study_a",
                        "association_row_count": 1,
                        "servable_row_count": 1,
                        "patient_count": 1,
                        "incomplete_asset_count": 1,
                        "filtered_row_count": 0,
                    }
                ),
                encoding="utf-8",
            )
            manifest = Path(temporary) / "manifest.json"
            manifest.write_text(
                json.dumps(
                    {"version": 1, "studies": [{"study_id": "study_a", "study_dir": str(study_dir)}]}
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(STACK.VerificationError, "incomplete WSI assets"):
                STACK._read_manifest(manifest)

    def test_manifest_rejects_duplicate_studies(self):
        with tempfile.TemporaryDirectory() as temporary:
            study_dir = Path(temporary) / "study"
            study_dir.mkdir()
            (study_dir / "meta_wsi.txt").write_text(
                "cancer_study_identifier: study_a\ndata_filename: data_wsi.txt\n", encoding="utf-8"
            )
            (study_dir / "data_wsi.txt").write_text("PATIENT_ID\tIMAGE_ID\nP\tI\n", encoding="utf-8")
            _write_timeline_pair(study_dir)
            (study_dir / "wsi_snapshot_manifest.json").write_text(
                json.dumps(
                    {
                        "study_id": "study_a",
                        "association_row_count": 1,
                        "servable_row_count": 1,
                        "patient_count": 1,
                        "incomplete_asset_count": 0,
                        "filtered_row_count": 0,
                    }
                ),
                encoding="utf-8",
            )
            manifest = Path(temporary) / "manifest.json"
            entry = {"study_id": "study_a", "study_dir": str(study_dir)}
            manifest.write_text(json.dumps({"version": 1, "studies": [entry, entry]}), encoding="utf-8")
            with self.assertRaisesRegex(STACK.VerificationError, "duplicates study"):
                STACK._read_manifest(manifest)

    def test_manifest_rejects_a_study_id_mismatch_in_meta_wsi(self):
        with tempfile.TemporaryDirectory() as temporary:
            study_dir = Path(temporary) / "study"
            study_dir.mkdir()
            (study_dir / "meta_wsi.txt").write_text(
                "cancer_study_identifier: source_id\ndata_filename: data_wsi.txt\n",
                encoding="utf-8",
            )
            (study_dir / "data_wsi.txt").write_text("PATIENT_ID\tIMAGE_ID\nP\tI\n", encoding="utf-8")
            _write_timeline_pair(study_dir)
            (study_dir / "wsi_snapshot_manifest.json").write_text(
                json.dumps(
                    {
                        "study_id": "source_id",
                        "association_row_count": 1,
                        "servable_row_count": 1,
                        "patient_count": 1,
                        "incomplete_asset_count": 0,
                        "filtered_row_count": 0,
                    }
                ),
                encoding="utf-8",
            )
            manifest = Path(temporary) / "manifest.json"
            manifest.write_text(
                json.dumps({"version": 1, "studies": [{"study_id": "portal_id", "study_dir": str(study_dir)}]}),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(STACK.VerificationError, "different cancer_study_identifier"):
                STACK._read_manifest(manifest)

    def test_manifest_rejects_a_declared_source_without_data(self):
        with tempfile.TemporaryDirectory() as temporary:
            study_dir = Path(temporary) / "study"
            study_dir.mkdir()
            (study_dir / "meta_wsi.txt").write_text(
                "cancer_study_identifier: study_a\ndata_filename: data_wsi.txt\n", encoding="utf-8"
            )
            (study_dir / "data_wsi.txt").write_text("PATIENT_ID\tIMAGE_ID\nP\tI\n", encoding="utf-8")
            _write_timeline_pair(study_dir)
            (study_dir / "meta_mutations.txt").write_text(
                "cancer_study_identifier: study_a\ndata_filename: data_mutations.txt\n", encoding="utf-8"
            )
            (study_dir / "wsi_snapshot_manifest.json").write_text(
                json.dumps(
                    {
                        "study_id": "study_a",
                        "association_row_count": 1,
                        "servable_row_count": 1,
                        "patient_count": 1,
                        "incomplete_asset_count": 0,
                        "filtered_row_count": 0,
                    }
                ),
                encoding="utf-8",
            )
            manifest = Path(temporary) / "manifest.json"
            manifest.write_text(
                json.dumps({"version": 1, "studies": [{"study_id": "study_a", "study_dir": str(study_dir)}]}),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(STACK.VerificationError, "references missing or unsafe data"):
                STACK._read_manifest(manifest)


class TimelineRepairTests(unittest.TestCase):
    def test_reconcile_removes_linkout_for_a_non_servable_slide(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            wsi = root / "data_wsi.txt"
            wsi.write_text(
                "PATIENT_ID\tSAMPLE_ID\tIMAGE_ID\tMATCH_LEVEL\tSPECIMEN_KEY\tIS_HNE\tIS_IHC\tCAN_SERVE_TILES\n"
                "P-1\tS-1\tslide-1\tBLOCK\tblock::part:1::block:A1\tTRUE\tFALSE\tFALSE\n",
                encoding="utf-8",
            )
            timeline = root / "data_clinical_timeline_pathology_slides.txt"
            timeline.write_text(
                "PATIENT_ID\tSTART_DATE\tEVENT_TYPE\tIMAGE_COUNT\tNON_SERVABLE_IMAGE_COUNT\tTOTAL_IMAGE_COUNT\tIMAGE_IDS\tLINKOUT\n"
                "P-1\t0\tPATHOLOGY SLIDES\t1\t0\t1\t[\"slide-1\"]\t"
                "/patient/wsiHESlides?caseId=P-1&sampleId=S-1&stainFilter=hne&"
                "matchLevel=BLOCK&specimenKey=block%3A%3Apart%3A1%3A%3Ablock%3AA1\n",
                encoding="utf-8",
            )

            result = RECONCILE.reconcile(wsi, timeline)

            self.assertEqual(result["rows_changed"], 1)
            row = next(iter(RECONCILE._rows(timeline)))
            self.assertEqual(row["IMAGE_COUNT"], "0")
            self.assertEqual(row["NON_SERVABLE_IMAGE_COUNT"], "1")
            self.assertEqual(row["TOTAL_IMAGE_COUNT"], "1")
            self.assertEqual(row["LINKOUT"], "")

    def test_reconcile_can_drop_an_association_absent_from_the_wsi_snapshot(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            wsi = root / "data_wsi.txt"
            wsi.write_text(
                "PATIENT_ID\tSAMPLE_ID\tIMAGE_ID\tMATCH_LEVEL\tSPECIMEN_KEY\tIS_HNE\tIS_IHC\tCAN_SERVE_TILES\n",
                encoding="utf-8",
            )
            timeline = root / "data_clinical_timeline_pathology_slides.txt"
            timeline.write_text(
                "PATIENT_ID\tSTART_DATE\tEVENT_TYPE\tIMAGE_COUNT\tNON_SERVABLE_IMAGE_COUNT\tTOTAL_IMAGE_COUNT\tIMAGE_IDS\tLINKOUT\n"
                "P-1\t0\tPATHOLOGY SLIDES\t1\t0\t1\t[\"slide-1\"]\t"
                "/patient/wsiHESlides?caseId=P-1&stainFilter=hne&matchLevel=BLOCK&"
                "specimenKey=block%3A%3Apart%3A1%3A%3Ablock%3AA1\n",
                encoding="utf-8",
            )

            result = RECONCILE.reconcile(wsi, timeline, drop_unresolved=True)

            self.assertEqual(result["rows_changed"], 1)
            self.assertEqual(list(RECONCILE._rows(timeline)), [])


if __name__ == "__main__":
    unittest.main()
