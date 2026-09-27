import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
import sys
from unittest import TestCase, mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import backfill_historical_reports as backfill
import export_reports_to_postgres as exporter


class HistoricalBackfillTests(TestCase):
    def snapshot(self, day=8, hour=22, commit="abc"):
        return backfill.Snapshot(commit, datetime(2026, 9, day, hour, tzinfo=UTC),
                                 date(2026, 9, day), {"sectors": []}, {"regions": []})

    def test_one_final_snapshot_per_day_within_retention(self):
        now = datetime(2026, 9, 27, tzinfo=UTC)
        latest = self.snapshot(commit="last")
        old = backfill.Snapshot("expired", now - timedelta(days=31), date(2026, 8, 27), {}, {})
        future = backfill.Snapshot("future", now + timedelta(days=1), date(2026, 9, 28), {}, {})
        stale_market = backfill.Snapshot("stale", now, date(2026, 8, 1), {}, {})
        result = backfill.select_daily([latest, self.snapshot(hour=13), old, future, stale_market], now, 30)
        self.assertEqual(result, [latest])
        with self.assertRaises(ValueError):
            backfill.select_daily([], now, 31)

    def test_dry_run_never_writes(self):
        engine = mock.MagicMock()
        with mock.patch.object(backfill, "create_engine", return_value=engine), mock.patch.object(
            backfill, "existing_dates", return_value=set()
        ), mock.patch.object(backfill, "export_reports") as write:
            result = backfill.backfill("postgresql://test", [self.snapshot()])
        self.assertEqual(result["planned"], ["2026-09-08"])
        write.assert_not_called()
        engine.dispose.assert_called_once()

    def test_apply_then_retry_skips_existing_market_date(self):
        engine = mock.MagicMock()
        saved = set()
        snapshot = self.snapshot()

        def write(url, sector_path, region_path, **kwargs):
            self.assertEqual(kwargs["historical_at"], snapshot.observed_at)
            self.assertEqual(kwargs["source_commit"], "abc")
            self.assertEqual(json.loads(sector_path.read_text()), snapshot.sector)
            saved.add(snapshot.market_date)
            return "imported-id"

        with mock.patch.object(backfill, "create_engine", return_value=engine), mock.patch.object(
            backfill, "existing_dates", side_effect=lambda conn: saved
        ), mock.patch.object(backfill, "export_reports", side_effect=write) as export:
            first = backfill.backfill("postgresql://test", [snapshot], apply=True)
            retry = backfill.backfill("postgresql://test", [snapshot], apply=True)
        self.assertEqual(len(first["imported"]), 1)
        self.assertEqual(retry["imported"], [])
        self.assertEqual(retry["skipped"], ["2026-09-08"])
        export.assert_called_once()
        lock_sql = str(engine.begin.return_value.__enter__.return_value.execute.call_args.args[0])
        self.assertIn("pg_advisory_xact_lock", lock_sql)

    def test_export_preserves_observation_timestamp_and_provenance(self):
        engine = mock.MagicMock()
        connection = engine.begin.return_value.__enter__.return_value
        connection.execute.return_value.mappings.return_value.one.return_value = {"id": "historical-id"}
        observed = datetime(2026, 9, 8, 22, tzinfo=UTC)
        with mock.patch.object(exporter, "create_engine", return_value=engine) as create, mock.patch.object(
            exporter, "_validate_database_counts"
        ), mock.patch.object(exporter, "_database_counts", return_value={}), mock.patch.object(
            exporter, "_source_commit", return_value="current-code"
        ):
            exporter.export_reports("postgresql://test", ROOT / "reports/sector_dashboard.json",
                                    ROOT / "reports/region_dashboard.json", "history",
                                    historical_at=observed, source_commit="original-artifact")
        self.assertIsNone(create.call_args.kwargs["connect_args"]["prepare_threshold"])
        inserts = [call.args[1] for call in connection.execute.call_args_list
                   if "returning id" in str(call.args[0])]
        success = [call.args[1] for call in connection.execute.call_args_list
                   if "status = 'success'" in str(call.args[0])]
        self.assertEqual(inserts[0]["source_commit"], "original-artifact")
        self.assertEqual(inserts[0]["started_at"], observed)
        self.assertEqual(success[0]["completed_at"], observed)
        diagnostics = json.loads(success[0]["diagnostics"])
        self.assertEqual(diagnostics["historical_import"]["importer_commit"], "current-code")

    def test_invalid_historical_timestamp_rejected_before_db_access(self):
        with mock.patch.object(exporter, "create_engine") as engine:
            for timestamp in (datetime(2026, 1, 1), datetime.now(UTC) + timedelta(days=1)):
                with self.assertRaises(ValueError):
                    exporter.export_reports("postgresql://test", Path("a"), Path("b"), "history",
                                            historical_at=timestamp)
            engine.assert_not_called()
