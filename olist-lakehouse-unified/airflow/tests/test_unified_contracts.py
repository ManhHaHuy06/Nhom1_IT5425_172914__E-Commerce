from __future__ import annotations

import ast
import sys
import types
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "airflow" / "include"))

from state_commit import build_ingestion_state_updates

# local_cleanup only needs the deterministic run-id sanitizer from ingestion.py.
ingestion_stub = types.ModuleType("ingestion")
ingestion_stub.safe_run_id = lambda value: "".join(c if c.isalnum() or c in "_.-" else "_" for c in value)[:160]
sys.modules.setdefault("ingestion", ingestion_stub)
from local_cleanup import cleanup_run_extracts


class StateCommitTests(unittest.TestCase):
    def setUp(self):
        self.plan = {
            "transaction_windows": {
                "orders": {"from": "2026-01-01 00:00:00", "to": "2026-01-01 00:15:00"},
                "customers": {"from": "2026-01-01 00:00:00", "to": "2026-01-01 00:15:00"},
            }
        }
        self.tx = {"rows": 12, "batch_id": "txn_demo"}
        self.ref = {"loaded": False, "rows": 0, "snapshot_id": None}

    def test_transactional_batch_commits_only_after_clickhouse_success(self):
        updates = build_ingestion_state_updates(
            self.plan, self.tx, self.ref, {"due": False}, [], True, "2026-01-01T00:16:00Z"
        )
        self.assertEqual(updates["wm__orders"], "2026-01-01 00:15:00")
        self.assertEqual(updates["last_transaction_batch_id"], "txn_demo")
        self.assertNotIn("last_reference_snapshot_date", updates)

    def test_reference_snapshot_commits_fingerprint(self):
        ref = {"loaded": True, "rows": 100, "snapshot_id": "2026-01-01"}
        extract = {"geolocation_sha256": "200:abcd:ef01"}
        updates = build_ingestion_state_updates(
            self.plan, {"rows": 0, "batch_id": "txn_empty"}, ref, extract, [], True, "ok"
        )
        self.assertEqual(updates["last_reference_snapshot_date"], "2026-01-01")
        self.assertEqual(updates["geolocation_snapshot_sha256"], "200:abcd:ef01")

    def test_failed_task_blocks_all_state_updates(self):
        with self.assertRaisesRegex(ValueError, "refusing to commit"):
            build_ingestion_state_updates(
                self.plan, self.tx, self.ref, None, ["build_gold_marts"], False, "ok"
            )

    def test_clickhouse_failure_blocks_when_rows_exist(self):
        with self.assertRaisesRegex(ValueError, "ClickHouse"):
            build_ingestion_state_updates(
                self.plan, self.tx, self.ref, None, [], False, "ok"
            )

    def test_no_data_run_can_advance_checked_watermark(self):
        updates = build_ingestion_state_updates(
            self.plan, {"rows": 0, "batch_id": "txn_empty"}, self.ref, None, [], False, "ok"
        )
        self.assertIn("wm__orders", updates)
        self.assertNotIn("geolocation_snapshot_sha256", updates)


class ArchitectureContractTests(unittest.TestCase):
    def test_dag_uses_safe_state_commit_and_success_gated_cleanup(self):
        dag = (ROOT / "airflow/dags/olist_end_to_end_pipeline.py").read_text()
        self.assertIn("from state_commit import build_ingestion_state_updates", dag)
        self.assertIn("trigger_rule=TriggerRule.ALL_SUCCESS", dag)
        self.assertIn("cleanup_run_extracts", dag)

    def test_clickhouse_loader_does_not_truncate_live_tables(self):
        loader = (ROOT / "airflow/include/clickhouse_loader.py").read_text().upper()
        self.assertNotIn("TRUNCATE TABLE", loader)
        self.assertIn("EXCHANGE TABLES", loader)

    def test_cleanup_removes_only_the_exact_current_run_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            run_dir = root / "manual__2026-01-01"
            (run_dir / "transactions").mkdir(parents=True)
            (run_dir / "reference").mkdir()
            sibling = root / "another_run"
            sibling.mkdir()
            result = cleanup_run_extracts(
                temp, "manual__2026-01-01",
                {"work_dir": str(run_dir / "transactions")},
                {"work_dir": str(run_dir / "reference")},
            )
            self.assertIn("cleaned", result)
            self.assertFalse(run_dir.exists())
            self.assertTrue(sibling.exists())

    def test_cleanup_rejects_symlink_run_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            outside = root / "outside"
            outside.mkdir()
            link = root / "run_1"
            try:
                link.symlink_to(outside, target_is_directory=True)
            except OSError:
                self.skipTest("Symlink creation is not permitted by this Windows environment")
            with self.assertRaisesRegex(ValueError, "symlink"):
                cleanup_run_extracts(temp, "run_1", None, None)
            self.assertTrue(outside.exists())

    def test_cleanup_rejects_xcom_path_outside_run(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            run_dir = root / "run_1"
            run_dir.mkdir()
            outside = root / "other"
            outside.mkdir()
            with self.assertRaisesRegex(ValueError, "outside"):
                cleanup_run_extracts(
                    temp, "run_1", {"work_dir": str(outside)}, None
                )

    def test_oltp_adapter_maps_all_nine_tables_to_existing_bronze_ddl(self):
        adapter_path = ROOT / "layers/bronze/load_oltp_bronze.py"
        tree = ast.parse(adapter_path.read_text())
        mapping = None
        for node in tree.body:
            if isinstance(node, ast.Assign) and any(
                isinstance(target, ast.Name) and target.id == "TABLES" for target in node.targets
            ):
                mapping = ast.literal_eval(node.value)
                break
        self.assertIsNotNone(mapping)
        self.assertEqual(len(mapping), 9)
        ddl = (ROOT / "layers/bronze/create_bronze_tables.sql").read_text()
        for source, (target, columns) in mapping.items():
            match = __import__("re").search(
                rf"CREATE TABLE IF NOT EXISTS iceberg\.bronze\.{__import__('re').escape(target)}\s*\((.*?)\)\s*USING iceberg",
                ddl, __import__("re").S | __import__("re").I,
            )
            self.assertIsNotNone(match, target)
            declared = set()
            for line in match.group(1).splitlines():
                col = __import__("re").match(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s+", line)
                if col:
                    declared.add(col.group(1))
            declared -= {"loaded_at", "source_file", "batch_id"}
            self.assertEqual(set(columns), declared, source)

    def test_original_lakehouse_transformations_are_present(self):
        for rel in (
            "layers/silver/dimensions.py",
            "layers/silver/facts.py",
            "layers/gold/build_marts.py",
            "layers/bronze/create_bronze_tables.sql",
            "layers/silver/create_silver_tables.sql",
            "layers/gold/create_gold_tables.sql",
        ):
            self.assertTrue((ROOT / rel).is_file(), rel)


if __name__ == "__main__":
    unittest.main()
