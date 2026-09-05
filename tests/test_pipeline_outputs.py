from __future__ import annotations

import sys
from pathlib import Path
from unittest import TestCase, mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import export_reports_to_postgres
import run_pipeline


class PipelineOutputTests(TestCase):
    def test_postgres_output_uses_final_reports_after_fundamentals(self) -> None:
        events: list[str] = []

        def record_step(name, command, index, total, env=None):
            events.append(name)

        def record_postgres(database_url: str, run_label: str) -> str:
            self.assertEqual(database_url, "postgresql://example")
            self.assertEqual(run_label, "market-close portal update")
            events.append("Postgres portal snapshot")
            return "update-id"

        with (
            mock.patch.object(run_pipeline, "run_step", side_effect=record_step),
            mock.patch.object(run_pipeline, "_database_url", return_value="postgresql://example"),
            mock.patch.object(run_pipeline, "persist_to_postgres", side_effect=record_postgres),
        ):
            result = run_pipeline.main(
                [
                    "--with-fundamentals",
                    "--postgres-mode",
                    "required",
                    "--run-label",
                    "market-close",
                ]
            )

        self.assertEqual(result, 0)
        self.assertLess(events.index("Global fundamentals"), events.index("Dashboard (JSON/CSV)"))
        self.assertEqual(events[-1], "Postgres portal snapshot")

    def test_required_postgres_stops_before_calculation_without_database_url(self) -> None:
        with (
            mock.patch.object(run_pipeline, "_database_url", return_value=None),
            mock.patch.object(run_pipeline, "run_step") as run_step,
        ):
            result = run_pipeline.main(["--postgres-mode", "required"])

        self.assertEqual(result, 2)
        run_step.assert_not_called()

    def test_default_mode_keeps_artifact_only_updates_available(self) -> None:
        with (
            mock.patch.object(run_pipeline, "run_step"),
            mock.patch.object(run_pipeline, "persist_to_postgres") as persist,
        ):
            result = run_pipeline.main([])

        self.assertEqual(result, 0)
        persist.assert_not_called()


class PostgresWriterTests(TestCase):
    def test_writer_persists_pipeline_provenance(self) -> None:
        executions = []

        class Result:
            def mappings(self):
                return self

            def one(self):
                return {"id": "00000000-0000-0000-0000-000000000001"}

        class Connection:
            def execute(self, statement, params=None):
                executions.append((str(statement), params or {}))
                return Result()

        class Transaction:
            def __enter__(self):
                return Connection()

            def __exit__(self, exc_type, exc, traceback):
                return False

        class Engine:
            def begin(self):
                return Transaction()

        empty_report = {"market_data": {"latest_price_date": "2026-09-05"}}
        with (
            mock.patch.object(
                export_reports_to_postgres,
                "_load_json",
                side_effect=[{**empty_report, "sectors": []}, {**empty_report, "regions": []}],
            ),
            mock.patch.object(export_reports_to_postgres, "create_engine", return_value=Engine()),
            mock.patch.object(export_reports_to_postgres, "_source_commit", return_value="abc1234"),
        ):
            update_id = export_reports_to_postgres.export_reports(
                database_url="postgresql://example",
                sector_report_path=Path("sector.json"),
                region_report_path=Path("region.json"),
                run_label="market-close portal update",
                triggered_by="market_pipeline",
            )

        self.assertEqual(update_id, "00000000-0000-0000-0000-000000000001")
        self.assertEqual(executions[0][1]["triggered_by"], "market_pipeline")
        self.assertEqual(executions[0][1]["latest_price_date"], "2026-09-05")
        self.assertIn("update public.update_runs", executions[-1][0])


class WorkflowContractTests(TestCase):
    def test_workflow_uses_unified_required_postgres_output(self) -> None:
        workflow = (ROOT / ".github/workflows/pipeline_weekdays.yml").read_text(
            encoding="utf-8"
        )

        self.assertIn("STARE_POSTGRES_MODE: required", workflow)
        self.assertIn("DATABASE_URL: ${{ secrets.DATABASE_URL }}", workflow)
        self.assertNotIn("Import update into portal database", workflow)
