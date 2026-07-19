from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from nifty_span.span.phase1_finalizer import finalize_span_phase1


class SpanPhase1FinalizerTests(unittest.TestCase):
    def test_pass_ready_and_outputs_are_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = _fixture(Path(temp))
            audit_fn = _audit_factory()
            with patch("nifty_span.span.phase1_finalizer.audit_span_backfill", audit_fn):
                first = _finalize(fixture)
                json_bytes = Path(first.completion_json).read_bytes()
                markdown_bytes = Path(first.completion_markdown).read_bytes()
                second = _finalize(fixture)

            self.assertTrue(first.ok)
            self.assertEqual(first.outcome, "PASS_READY")
            self.assertEqual(Path(second.completion_json).read_bytes(), json_bytes)
            self.assertEqual(
                Path(second.completion_markdown).read_bytes(), markdown_bytes
            )
            payload = json.loads(json_bytes)
            self.assertEqual(payload["audit"]["expected_cells"], 12_132)
            self.assertEqual(payload["audit"]["accounted_cells"], 12_132)
            self.assertEqual(payload["audit"]["terminal_cells"], 12_132)
            self.assertTrue(payload["source_stability"]["stable"])
            self.assertTrue(payload["evidence_complete"])
            self.assertTrue(
                payload["artifacts"]["exports"]["download_parquet"]["sha256"]
            )
            self.assertTrue(payload["artifacts"]["audit"]["matrix_parquet"]["sha256"])

    def test_manifest_change_during_finalization_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = _fixture(Path(temp))
            stable_audit = _audit_factory()

            def mutate_after_audit(**kwargs):
                report = stable_audit(**kwargs)
                with fixture.download.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(_download_event("2021-01-02")) + "\n")
                return report

            with patch(
                "nifty_span.span.phase1_finalizer.audit_span_backfill",
                mutate_after_audit,
            ):
                report = _finalize(fixture)

            self.assertFalse(report.ok)
            self.assertEqual(report.outcome, "FAIL_INCOMPLETE")
            self.assertFalse(report.payload["source_stability"]["stable"])
            self.assertFalse(
                report.payload["source_stability"]["checks"]["download_unchanged"]
            )

    def test_incomplete_audit_never_becomes_success(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = _fixture(Path(temp))
            with patch(
                "nifty_span.span.phase1_finalizer.audit_span_backfill",
                _audit_factory(
                    outcome="FAIL_INCOMPLETE",
                    ok=False,
                    accounted_cells=12_126,
                    terminal_cells=12_126,
                    failed_or_incomplete_cells=6,
                    matrix_complete=False,
                ),
            ):
                report = _finalize(fixture)

            self.assertEqual(report.outcome, "FAIL_INCOMPLETE")
            self.assertFalse(report.ok)
            self.assertFalse(report.payload["matrix_checks"]["accounted_cells_exact"])
            self.assertFalse(report.payload["matrix_checks"]["matrix_complete"])

    def test_missing_benchmark_or_pilot_evidence_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = _fixture(Path(temp))
            missing = fixture.root / "missing.json"
            cases = (
                ((), (fixture.pilot,)),
                ((fixture.benchmark,), ()),
                ((missing,), (fixture.pilot,)),
                ((fixture.benchmark,), (missing,)),
            )
            with patch(
                "nifty_span.span.phase1_finalizer.audit_span_backfill", _audit_factory()
            ):
                for benchmarks, pilots in cases:
                    with self.subTest(benchmarks=benchmarks, pilots=pilots):
                        report = _finalize(
                            fixture,
                            benchmark_artifacts=benchmarks,
                            pilot_artifacts=pilots,
                        )
                        self.assertEqual(report.outcome, "FAIL_INCOMPLETE")
                        self.assertFalse(report.payload["evidence_complete"])

    def test_missing_recovery_evidence_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = _fixture(Path(temp))
            with patch(
                "nifty_span.span.phase1_finalizer.audit_span_backfill", _audit_factory()
            ):
                report = _finalize(fixture, recovery_artifacts=())

            self.assertEqual(report.outcome, "FAIL_INCOMPLETE")
            self.assertFalse(report.payload["evidence_complete"])
            self.assertEqual(report.payload["artifacts"]["recovery"], [])

    def test_failed_benchmark_or_pilot_evidence_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = _fixture(Path(temp))
            with patch(
                "nifty_span.span.phase1_finalizer.audit_span_backfill", _audit_factory()
            ):
                fixture.benchmark.write_text(
                    '{"overall_outcome":"FAIL_FRESH_SPEEDUP"}\n', encoding="utf-8"
                )
                benchmark_fail = _finalize(fixture)
                fixture.benchmark.write_text(
                    '{"overall_outcome":"PASS"}\n', encoding="utf-8"
                )
                fixture.pilot.write_text(
                    '{"overall_status":"WAITING"}\n', encoding="utf-8"
                )
                pilot_fail = _finalize(fixture)

            self.assertEqual(benchmark_fail.outcome, "FAIL_INCOMPLETE")
            self.assertEqual(
                benchmark_fail.payload["artifacts"]["benchmarks"][0]["acceptance"],
                "FAIL",
            )
            self.assertEqual(pilot_fail.outcome, "FAIL_INCOMPLETE")
            self.assertEqual(
                pilot_fail.payload["artifacts"]["pilots"][0]["acceptance"], "FAIL"
            )

    def test_pilot_report_from_another_run_root_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = _fixture(Path(temp))
            payload = json.loads(fixture.pilot.read_text(encoding="utf-8"))
            payload["run_root"] = str(fixture.root / "another-run")
            fixture.pilot.write_text(json.dumps(payload) + "\n", encoding="utf-8")
            with patch(
                "nifty_span.span.phase1_finalizer.audit_span_backfill",
                _audit_factory(),
            ):
                report = _finalize(fixture)

            self.assertEqual(report.outcome, "FAIL_INCOMPLETE")
            self.assertEqual(
                report.payload["artifacts"]["pilots"][0]["acceptance"], "FAIL"
            )

    def test_conflicting_pass_and_fail_outcomes_are_rejected(self) -> None:
        self._assert_conflicting_benchmark_rejected(
            {"overall_status": "PASS", "outcome": "FAIL"}
        )

    def test_pass_with_ok_false_is_rejected(self) -> None:
        self._assert_conflicting_benchmark_rejected(
            {"overall_status": "PASS", "ok": False}
        )

    def test_pass_with_failed_gate_is_rejected(self) -> None:
        self._assert_conflicting_benchmark_rejected(
            {
                "overall_status": "PASS",
                "gates": {
                    "semantic_equivalence": {"passed": True},
                    "fresh_extraction_speedup": {"passed": False},
                },
            }
        )

    def test_top_level_pilot_pass_cannot_hide_nested_waiting_or_failure(self) -> None:
        for nested_status in ("WAITING", "FAIL"):
            with (
                self.subTest(nested_status=nested_status),
                tempfile.TemporaryDirectory() as temp,
            ):
                fixture = _fixture(Path(temp))
                fixture.pilot.write_text(
                    json.dumps(
                        {
                            "overall_status": "PASS",
                            "pilot_count": 2,
                            "status_counts": {"PASS": 1, nested_status: 1},
                            "pilots": [{"status": "PASS"}, {"status": nested_status}],
                        }
                    )
                    + "\n",
                    encoding="utf-8",
                )
                with patch(
                    "nifty_span.span.phase1_finalizer.audit_span_backfill",
                    _audit_factory(),
                ):
                    report = _finalize(fixture)
                self.assertEqual(report.outcome, "FAIL_INCOMPLETE")
                self.assertEqual(
                    report.payload["artifacts"]["pilots"][0]["acceptance"],
                    "FAIL",
                )

    def _assert_conflicting_benchmark_rejected(
        self, payload: dict[str, object]
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = _fixture(Path(temp))
            fixture.benchmark.write_text(json.dumps(payload) + "\n", encoding="utf-8")
            with patch(
                "nifty_span.span.phase1_finalizer.audit_span_backfill", _audit_factory()
            ):
                report = _finalize(fixture)
        self.assertEqual(report.outcome, "FAIL_INCOMPLETE")
        self.assertFalse(report.payload["evidence_complete"])
        self.assertEqual(
            report.payload["artifacts"]["benchmarks"][0]["acceptance"],
            "FAIL",
        )

    def test_source_boundary_is_preserved_as_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = _fixture(Path(temp))
            with patch(
                "nifty_span.span.phase1_finalizer.audit_span_backfill",
                _audit_factory(
                    outcome="BLOCKED_SOURCE",
                    ok=False,
                    blocked_matrix_complete=True,
                    source_boundary_cells=1,
                ),
            ):
                report = _finalize(fixture)

            self.assertEqual(report.outcome, "BLOCKED_SOURCE")
            self.assertFalse(report.ok)

    def test_source_boundary_survives_missing_and_waiting_acceptance_evidence(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = _fixture(Path(temp))
            fixture.pilot.write_text(
                '{"overall_status":"WAITING","pilots":[{"status":"WAITING"}]}\n',
                encoding="utf-8",
            )
            with patch(
                "nifty_span.span.phase1_finalizer.audit_span_backfill",
                _audit_factory(
                    outcome="BLOCKED_SOURCE",
                    ok=False,
                    blocked_matrix_complete=True,
                    source_boundary_cells=1,
                ),
            ):
                report = _finalize(
                    fixture,
                    benchmark_artifacts=(),
                    pilot_artifacts=(fixture.pilot,),
                )

            self.assertEqual(report.outcome, "BLOCKED_SOURCE")
            self.assertFalse(report.ok)
            self.assertFalse(report.payload["evidence_complete"])
            self.assertEqual(report.payload["artifacts"]["benchmarks"], [])
            self.assertEqual(
                report.payload["artifacts"]["pilots"][0]["acceptance"], "FAIL"
            )


class _Fixture(SimpleNamespace):
    root: Path
    download: Path
    extraction: Path
    availability: Path
    benchmark: Path
    pilot: Path
    pilot_expected: dict[str, object]
    recovery: Path


def _fixture(root: Path) -> _Fixture:
    manifests = root / "manifests"
    manifests.mkdir(parents=True)
    download = manifests / "download.jsonl"
    extraction = manifests / "extraction.jsonl"
    availability = manifests / "availability.effective.jsonl"
    download.write_text(
        json.dumps(_download_event("2021-01-01")) + "\n", encoding="utf-8"
    )
    extraction.write_text(json.dumps(_extraction_event()) + "\n", encoding="utf-8")
    availability.write_text('{"classification":"official_holiday"}\n', encoding="utf-8")
    benchmark = root / "benchmark.json"
    pilot = root / "pilot.json"
    benchmark.write_text(json.dumps(_benchmark_payload(root)) + "\n", encoding="utf-8")
    pilot_expected = _pilot_payload(root)
    pilot.write_text(json.dumps(pilot_expected) + "\n", encoding="utf-8")
    for directory in (root / "raw", root / "fragments", root / "compacted"):
        directory.mkdir()
    recovery = _write_recovery_evidence(root, download, availability)
    return _Fixture(
        root=root,
        download=download,
        extraction=extraction,
        availability=availability,
        benchmark=benchmark,
        pilot=pilot,
        pilot_expected=pilot_expected,
        recovery=recovery,
    )


def _benchmark_payload(root: Path) -> dict[str, object]:
    sources: dict[str, object] = {}
    for label in ("legacy", "streaming", "comparison"):
        path = root / f"{label}.json"
        path.write_text(json.dumps({"label": label}) + "\n", encoding="utf-8")
        sources[f"{label}_json"] = str(path)
        sources[f"{label}_sha256"] = _sha256(path)
    return {
        "schema_version": "span-benchmark-binding-limit/v1",
        "overall_outcome": "PASS_BINDING_LIMIT",
        "ok": True,
        "repository_commit": "a" * 40,
        "benchmark_scope": {"month": "2021-01", "raw_archives": 120},
        "source_evidence": sources,
        "semantic_equivalence": {
            "status": "PASS",
            "canonical_semantic_sha256": "b" * 64,
            "row_count": 1,
            "duplicate_natural_keys": 0,
        },
    }


def _pilot_payload(root: Path) -> dict[str, object]:
    pilot_ids = (
        "ordinary_early_2021_01",
        "special_session_2024_03",
        "expiry_regime_2025_09",
        "ordinary_recent_2026_06",
    )
    pilots = []
    source_root = root / "pilot_sources"
    source_root.mkdir()
    for pilot_id in pilot_ids:
        artifacts = {}
        for name in ("audit_summary", "date_slot_matrix", "compacted_parquet"):
            path = source_root / f"{pilot_id}.{name}"
            path.write_text(f"{pilot_id}:{name}\n", encoding="utf-8")
            artifacts[name] = {
                "path": path.relative_to(root).as_posix(),
                "exists": True,
                "size_bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
        pilots.append({"pilot_id": pilot_id, "status": "PASS", "artifacts": artifacts})
    return {
        "schema_version": "span-required-pilots/v1",
        "run_root": str(root),
        "overall_status": "PASS",
        "pilot_count": len(pilots),
        "status_counts": {"PASS": len(pilots), "WAITING": 0, "FAIL": 0},
        "pilots": pilots,
    }


def _finalize(
    fixture: _Fixture,
    *,
    benchmark_artifacts=None,
    pilot_artifacts=None,
    recovery_artifacts=None,
):
    with patch(
        "nifty_span.span.phase1_finalizer.inspect_required_span_pilots",
        return_value=fixture.pilot_expected,
    ):
        return finalize_span_phase1(
            run_root=fixture.root,
            start_date=date(2021, 1, 1),
            end_date=date(2026, 7, 15),
            availability_manifest=fixture.availability,
            benchmark_artifacts=(fixture.benchmark,)
            if benchmark_artifacts is None
            else benchmark_artifacts,
            pilot_artifacts=(fixture.pilot,)
            if pilot_artifacts is None
            else pilot_artifacts,
            recovery_artifacts=(fixture.recovery,)
            if recovery_artifacts is None
            else recovery_artifacts,
            commit_sha="a" * 40,
            test_result="98 tests passed",
            tool_versions={"pyarrow": "20.0.0", "python": "3.11.9"},
        )


def _write_recovery_evidence(root: Path, download: Path, availability: Path) -> Path:
    evidence_root = root / "recovery"
    snapshots = evidence_root / "manifest_snapshots"
    snapshots.mkdir(parents=True)
    snapshot_bytes = download.read_bytes()
    snapshot_hash = hashlib.sha256(snapshot_bytes).hexdigest()
    snapshot = snapshots / f"{snapshot_hash}.jsonl"
    snapshot.write_bytes(snapshot_bytes)
    markdown = evidence_root / "SPAN_CORRUPT_RECOVERY_fixture.md"
    markdown.write_text(
        "# NSE SPAN exact-static recovery and source evidence\n\n"
        "No alternative-source decision is required by this recovery run.\n",
        encoding="utf-8",
    )
    report = evidence_root / "span_corrupt_recovery_fixture.json"
    payload = {
        "schema_version": "span-corrupt-static-recovery/v1",
        "run_id": "fixture",
        "start_date": "2021-01-01",
        "end_date": "2026-07-15",
        "raw_root": str((root / "raw").resolve()),
        "source_manifest": str(download.resolve()),
        "availability_manifest": str(availability.resolve()),
        "source_snapshot": str(snapshot.resolve()),
        "source_snapshot_sha256": snapshot_hash,
        "source_snapshot_size_bytes": len(snapshot_bytes),
        "source_snapshot_events": 1,
        "selected_cells": 0,
        "network_calls": 0,
        "recovered_cells": 0,
        "classified_source_corrupt_cells": 0,
        "already_classified_cells": 0,
        "unresolved_cells": 0,
        "unresolved_corrupt_cells": 0,
        "unresolved_missing_cells": 0,
        "ok": True,
        "json_report": str(report.resolve()),
        "markdown_report": str(markdown.resolve()),
        "cells": [],
    }
    report.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    return report


def _audit_factory(**overrides):
    def audit(**kwargs):
        report_root = Path(kwargs["report_root"])
        report_root.mkdir(parents=True, exist_ok=True)
        matrix = report_root / "span_date_slot_matrix.parquet"
        summary = report_root / "span_backfill_summary.json"
        markdown = report_root / "SPAN_BACKFILL_AUDIT.md"
        matrix.write_bytes(b"deterministic-matrix")
        summary.write_text('{"outcome":"PASS_READY"}\n', encoding="utf-8")
        markdown.write_text("# deterministic audit\n", encoding="utf-8")
        values = {
            "accepted_unavailable_cells": 4_000,
            "accounted_cells": 12_132,
            "availability_manifest_sha256": _sha256(
                Path(kwargs["availability_manifest"])
            ),
            "compacted_months": 67,
            "compacted_rows": 1_000_000,
            "compacted_unique": True,
            "download_manifest_sha256": _sha256(Path(kwargs["download_manifest"])),
            "downloaded_cells": 8_132,
            "earliest_proven_download_date": "2021-01-04",
            "expected_cells": 12_132,
            "extraction_complete": True,
            "extraction_manifest_sha256": _sha256(Path(kwargs["extraction_manifest"])),
            "failed_or_incomplete_cells": 0,
            "latest_proven_download_date": "2026-07-15",
            "matrix_complete": True,
            "blocked_matrix_complete": False,
            "matrix_parquet": str(matrix),
            "ok": True,
            "outcome": "PASS_READY",
            "raw_integrity_ok": True,
            "requested_dates": 2_022,
            "resolved_or_blocked_cells": 12_132,
            "slot_year_counts": (_slot_year(),),
            "summary_json": str(summary),
            "audit_markdown": str(markdown),
            "terminal_cells": 12_132,
            "source_boundary_cells": 0,
            "unresolved_missing_cells": 0,
            "unresolved_non_boundary_cells": 0,
        }
        values.update(overrides)
        return SimpleNamespace(**values)

    return audit


def _slot_year() -> dict[str, object]:
    return {
        "accepted_unavailable_cells": 0,
        "download_state_counts": {"downloaded": 365},
        "downloaded_valid_cells": 365,
        "extracted_valid_cells": 365,
        "extraction_state_counts": {"fragment_created": 365},
        "manifest_missing_cells": 0,
        "nonterminal_or_failed_cells": 0,
        "raw_missing_response_cells": 0,
        "slot": "BOD",
        "suffix": "i1",
        "terminal_cells": 365,
        "total_cells": 365,
        "unresolved_missing_cells": 0,
        "year": 2021,
    }


def _download_event(day: str) -> dict[str, object]:
    return {
        "attempt": 1,
        "event_id": f"event-{day}",
        "observed_at_utc": "2021-01-01T00:00:00Z",
        "run_id": "run",
        "schema_version": 1,
        "slot": "BOD",
        "state": "not_returned_http_404",
        "suffix": "i1",
        "terminal": True,
        "trading_date": day,
    }


def _extraction_event() -> dict[str, object]:
    return {
        "date": "2021-01-01",
        "event": "fragment_created",
        "extraction_identity": "identity",
        "fragment_path": "2021/01/01/fragment.parquet",
        "fragment_sha256": "b" * 64,
        "fragment_size_bytes": 100,
        "ingested_at_utc": "2021-01-01T00:00:00Z",
        "instrument_counts": {"CE": 1, "FUT": 1, "PE": 1},
        "parser_version": "parser",
        "row_count": 3,
        "schema_version": "span-arrow-schema-v1",
        "slot": "BOD",
        "source_file": "nsccl.20210101.i1.zip",
        "source_sha256": "a" * 64,
        "symbols_filter": ["NIFTY"],
    }


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


if __name__ == "__main__":
    unittest.main()
