from __future__ import annotations

import copy
import json
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

    def test_calculation_failure_is_recorded_for_postgres_updates(self) -> None:
        call_count = 0

        def fail_second_step(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise run_pipeline.subprocess.CalledProcessError(7, args[1])

        with (
            mock.patch.object(run_pipeline, "_database_url", return_value="postgresql://example"),
            mock.patch.object(run_pipeline, "run_step", side_effect=fail_second_step),
            mock.patch.object(run_pipeline, "log_pipeline_failure", return_value="failure-id") as log,
            mock.patch.object(run_pipeline, "persist_to_postgres") as persist,
        ):
            result = run_pipeline.main(["--postgres-mode", "required", "--run-label", "open"])

        self.assertEqual(result, 7)
        persist.assert_not_called()
        log.assert_called_once()
        self.assertEqual(log.call_args.args[1], "open portal update")
        self.assertEqual(log.call_args.args[3], ["S&P 500 universe"])
        self.assertEqual(
            log.call_args.args[4],
            [{"name": "Global universe", "returncode": 7}],
        )


class ReportValidationTests(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.sector_report = json.loads(
            (ROOT / "reports/sector_dashboard.json").read_text(encoding="utf-8")
        )
        cls.region_report = json.loads(
            (ROOT / "reports/region_dashboard.json").read_text(encoding="utf-8")
        )

    def test_current_reports_pass_structural_and_coverage_validation(self) -> None:
        result = export_reports_to_postgres.validate_reports(
            copy.deepcopy(self.sector_report),
            copy.deepcopy(self.region_report),
            max_data_age_days=7,
        )

        self.assertEqual(result["sector_count"], 11)
        self.assertEqual(result["region_count"], 3)
        self.assertGreaterEqual(result["price_coverage"], 0.9)
        self.assertEqual(result["stock_rows"], result["recommendation_rows"])

    def test_missing_sector_prevents_success(self) -> None:
        sector_report = copy.deepcopy(self.sector_report)
        sector_report["sectors"] = sector_report["sectors"][1:]

        with self.assertRaises(export_reports_to_postgres.ReportValidationError) as raised:
            export_reports_to_postgres.validate_reports(
                sector_report,
                copy.deepcopy(self.region_report),
            )

        self.assertIn("missing sectors", str(raised.exception))

    def test_stale_market_data_prevents_scheduled_success(self) -> None:
        sector_report = copy.deepcopy(self.sector_report)
        region_report = copy.deepcopy(self.region_report)
        sector_report["market_data"]["latest_price_date"] = "2000-01-01"
        region_report["market_data"]["latest_price_date"] = "2000-01-01"

        with self.assertRaises(export_reports_to_postgres.ReportValidationError) as raised:
            export_reports_to_postgres.validate_reports(
                sector_report,
                region_report,
                max_data_age_days=7,
            )

        self.assertIn("latest market data is", str(raised.exception))

    def test_database_row_count_mismatch_prevents_success(self) -> None:
        with self.assertRaises(export_reports_to_postgres.ReportValidationError):
            export_reports_to_postgres._validate_database_counts(
                {
                    "sector_count": 11,
                    "region_count": 3,
                    "stock_rows": 181,
                    "recommendation_rows": 182,
                },
                {
                    "sector_count": 11,
                    "region_count": 3,
                    "stock_rows": 182,
                    "recommendation_rows": 182,
                },
            )


class PostgresWriterTests(TestCase):
    def test_writer_persists_pipeline_provenance(self) -> None:
        executions = []

        class Result:
            def __init__(self, row):
                self.row = row

            def mappings(self):
                return self

            def one(self):
                return self.row

        class Connection:
            def execute(self, statement, params=None):
                sql = str(statement)
                executions.append((sql, params or {}))
                if "returning id" in sql:
                    return Result({"id": "00000000-0000-0000-0000-000000000001"})
                if "sector_count" in sql and "select count" in sql:
                    return Result(
                        {
                            "sector_count": 11,
                            "region_count": 3,
                            "stock_rows": 182,
                            "recommendation_rows": 182,
                        }
                    )
                return Result({})

        class Transaction:
            def __enter__(self):
                return Connection()

            def __exit__(self, exc_type, exc, traceback):
                return False

        class Engine:
            def begin(self):
                return Transaction()

            def dispose(self):
                pass

        with (
            mock.patch.object(export_reports_to_postgres, "create_engine", return_value=Engine()),
            mock.patch.object(export_reports_to_postgres, "_source_commit", return_value="abc1234"),
        ):
            update_id = export_reports_to_postgres.export_reports(
                database_url="postgresql://example",
                sector_report_path=ROOT / "reports/sector_dashboard.json",
                region_report_path=ROOT / "reports/region_dashboard.json",
                run_label="market-close portal update",
                triggered_by="market_pipeline",
            )

        self.assertEqual(update_id, "00000000-0000-0000-0000-000000000001")
        self.assertEqual(executions[0][1]["triggered_by"], "market_pipeline")
        success_updates = [params for sql, params in executions if "status = 'success'" in sql]
        self.assertEqual(success_updates[0]["latest_price_date"], "2026-09-04")
        self.assertIn("update public.update_runs", executions[-1][0])

    def test_validation_failure_is_written_to_update_audit(self) -> None:
        executions = []

        class Result:
            def mappings(self):
                return self

            def one(self):
                return {"id": "00000000-0000-0000-0000-000000000002"}

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

            def dispose(self):
                pass

        incomplete_sector_report = {
            "market_data": {"latest_price_date": "2026-09-05"},
            "sectors": [],
        }
        incomplete_region_report = {
            "market_data": {"latest_price_date": "2026-09-05"},
            "regions": [],
        }
        with (
            mock.patch.object(
                export_reports_to_postgres,
                "_load_json",
                side_effect=[incomplete_sector_report, incomplete_region_report],
            ),
            mock.patch.object(export_reports_to_postgres, "create_engine", return_value=Engine()),
        ):
            with self.assertRaises(export_reports_to_postgres.ReportValidationError):
                export_reports_to_postgres.export_reports(
                    database_url="postgresql://example",
                    sector_report_path=Path("sector.json"),
                    region_report_path=Path("region.json"),
                    run_label="failed update",
                )

        failure_updates = [params for sql, params in executions if "status = 'failed'" in sql]
        self.assertEqual(len(failure_updates), 1)
        diagnostics = json.loads(failure_updates[0]["diagnostics"])
        self.assertEqual(diagnostics["failure"]["stage"], "report_validation")
        self.assertIn("missing sectors", diagnostics["failure"]["message"])


class WorkflowContractTests(TestCase):
    def test_manual_workflow_uses_required_postgres_output(self) -> None:
        workflow = (ROOT / ".github/workflows/pipeline_weekdays.yml").read_text(
            encoding="utf-8"
        )

        self.assertIn(
            "STARE_POSTGRES_MODE: ${{ github.event_name == 'workflow_dispatch' && 'required' || 'disabled' }}",
            workflow,
        )
        self.assertIn("DATABASE_URL: ${{ secrets.DATABASE_URL }}", workflow)
        self.assertNotIn("Import update into portal database", workflow)

    def test_render_blueprint_defines_both_market_update_jobs(self) -> None:
        blueprint = (ROOT / "render.yaml").read_text(encoding="utf-8")

        self.assertIn("name: stare-market-open", blueprint)
        self.assertIn('schedule: "35 13,14 * * 1-5"', blueprint)
        self.assertIn("--window market-open", blueprint)
        self.assertIn("name: stare-market-close", blueprint)
        self.assertIn('schedule: "10 20,21 * * 1-5"', blueprint)
        self.assertIn("--window market-close", blueprint)
