from __future__ import annotations

from datetime import date
from hashlib import sha256
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
import json
import sys

from nifty_span.span.availability import IMPORT_SCHEMA


SCRIPT = Path(__file__).parents[1] / "scripts" / "follow_span_completed_months.py"
SPEC = spec_from_file_location("follow_span_completed_months", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
subject = module_from_spec(SPEC)
sys.modules[SPEC.name] = subject
SPEC.loader.exec_module(subject)


class CompletedMonthFollowerTests(TestCase):
    def test_requires_every_terminal_cell_and_never_selects_open_month(self) -> None:
        january = _month_events(2025, 1)
        february = _month_events(2025, 2)
        february[-1] = {**february[-1], "terminal": False, "state": "retryable_error"}
        march = _month_events(2025, 3)
        latest = subject._latest_cells(january + february + march)

        self.assertEqual(subject._eligible_months(latest, date(2025, 3, 15)), [(2025, 1)])

    def test_pass_ready_uses_snapshot_parse_workers_and_skips_valid_rerun(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "download.jsonl"
            original = _write_manifest(manifest, _month_events(2025, 1))
            config = _config(root, manifest, parse_workers=7)
            calls: list[tuple[str, dict[str, object]]] = []
            compacted = root / "parquet" / "span_2025_01.parquet"
            compacted.parent.mkdir(parents=True)
            compacted.write_bytes(b"valid compacted month")
            compacted_sha = sha256(compacted.read_bytes()).hexdigest()

            def extractor(**kwargs: object) -> SimpleNamespace:
                calls.append(("extract", kwargs))
                return SimpleNamespace(ok=True)

            def auditor(**kwargs: object) -> SimpleNamespace:
                calls.append(("audit", kwargs))
                return _audit(
                    "PASS_READY",
                    downloaded_cells=186,
                    months=(
                        SimpleNamespace(compacted_path=str(compacted), sha256=compacted_sha),
                    ),
                )

            first = subject.follow_once(
                config,
                today=date(2025, 2, 1),
                extractor=extractor,
                auditor=auditor,
            )
            second = subject.follow_once(
                config,
                today=date(2025, 2, 1),
                extractor=extractor,
                auditor=auditor,
            )

            self.assertEqual(first.processed_months, 1)
            self.assertEqual(first.skipped_months, 0)
            self.assertEqual(second.processed_months, 0)
            self.assertEqual(second.skipped_months, 1)
            self.assertEqual([name for name, _kwargs in calls], ["extract", "audit"])
            extract_kwargs = calls[0][1]
            audit_kwargs = calls[1][1]
            self.assertEqual(extract_kwargs["parse_workers"], 7)
            self.assertEqual(extract_kwargs["download_manifest"], Path(first.snapshot_path))
            self.assertEqual(audit_kwargs["download_manifest"], Path(first.snapshot_path))
            self.assertEqual(Path(first.snapshot_path).read_bytes(), original)
            self.assertEqual(manifest.read_bytes(), original)
            state_events = _state_events(config.state_root, "2025-01")
            self.assertEqual([event["event"] for event in state_events], ["started", "completed"])

    def test_fail_incomplete_is_recorded_and_deferred_without_becoming_fatal(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "download.jsonl"
            _write_manifest(manifest, _month_events(2025, 1))
            config = _config(root, manifest)
            calls = {"extract": 0, "audit": 0}

            def extractor(**_kwargs: object) -> SimpleNamespace:
                calls["extract"] += 1
                return SimpleNamespace(ok=True)

            def auditor(**_kwargs: object) -> SimpleNamespace:
                calls["audit"] += 1
                return _audit("FAIL_INCOMPLETE", downloaded_cells=1)

            first = subject.follow_once(
                config,
                today=date(2025, 2, 1),
                extractor=extractor,
                auditor=auditor,
            )
            second = subject.follow_once(
                config,
                today=date(2025, 2, 1),
                extractor=extractor,
                auditor=auditor,
            )

            self.assertEqual(first.incomplete_months, 1)
            self.assertEqual(dict(first.outcomes), {"2025-01": "FAIL_INCOMPLETE"})
            self.assertEqual(second.skipped_months, 1)
            self.assertEqual(calls, {"extract": 1, "audit": 1})
            self.assertEqual(
                [event["event"] for event in _state_events(config.state_root, "2025-01")],
                ["started", "audit_incomplete"],
            )

    def test_fail_integrity_and_failed_extraction_stop_the_cycle(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "download.jsonl"
            _write_manifest(manifest, _month_events(2025, 1))
            config = _config(root, manifest)

            with self.assertRaises(subject.FollowerFatalError):
                subject.follow_once(
                    config,
                    today=date(2025, 2, 1),
                    extractor=lambda **_kwargs: SimpleNamespace(ok=False),
                    auditor=lambda **_kwargs: _audit("PASS_READY"),
                )
            self.assertEqual(
                _state_events(config.state_root, "2025-01")[-1]["event"],
                "fatal_extraction",
            )

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "download.jsonl"
            _write_manifest(manifest, _month_events(2025, 1))
            config = _config(root, manifest)
            with self.assertRaises(subject.FollowerFatalError):
                subject.follow_once(
                    config,
                    today=date(2025, 2, 1),
                    extractor=lambda **_kwargs: SimpleNamespace(ok=True),
                    auditor=lambda **_kwargs: _audit("FAIL_INTEGRITY"),
                )
            self.assertEqual(
                _state_events(config.state_root, "2025-01")[-1]["event"],
                "fatal_integrity",
            )

    def test_partial_last_line_is_not_snapshotted_or_processed(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "download.jsonl"
            manifest.write_bytes(b'{"trading_date":"2025-01-01"')
            config = _config(root, manifest)
            called = False

            def extractor(**_kwargs: object) -> SimpleNamespace:
                nonlocal called
                called = True
                return SimpleNamespace(ok=True)

            with self.assertRaises(subject.ManifestNotStableError):
                subject.follow_once(
                    config,
                    today=date(2025, 2, 1),
                    extractor=extractor,
                    auditor=lambda **_kwargs: _audit("PASS_READY"),
                )

            self.assertFalse(called)
            self.assertFalse((config.state_root / "snapshots").exists())

    def test_optional_availability_manifest_is_forwarded_and_changes_state_key(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "download.jsonl"
            availability = root / "availability.jsonl"
            _write_manifest(manifest, _month_events(2025, 1))
            _write_manifest(
                availability,
                [{"trading_date": "2025-01-01", "slot": "BOD", "outcome": "confirmed"}],
            )
            config = _config(root, manifest, availability_manifest=availability)
            received: list[Path | None] = []

            def auditor(**kwargs: object) -> SimpleNamespace:
                received.append(kwargs["availability_manifest"])
                return _audit("FAIL_INCOMPLETE")

            subject.follow_once(
                config,
                today=date(2025, 2, 1),
                extractor=lambda **_kwargs: SimpleNamespace(ok=True),
                auditor=auditor,
            )
            _write_manifest(
                availability,
                [
                    {"trading_date": "2025-01-01", "slot": "BOD", "outcome": "confirmed"},
                    {"trading_date": "2025-01-02", "slot": "ID1", "outcome": "confirmed"},
                ],
            )
            second = subject.follow_once(
                config,
                today=date(2025, 2, 1),
                extractor=lambda **_kwargs: SimpleNamespace(ok=True),
                auditor=auditor,
            )

            self.assertEqual(received, [availability.resolve(), availability.resolve()])
            self.assertEqual(second.processed_months, 1)
            self.assertEqual(second.skipped_months, 0)

    def test_availability_refresh_options_are_paired(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "download.jsonl"
            _write_manifest(manifest, _month_events(2025, 1))
            with self.assertRaisesRegex(ValueError, "supplied together"):
                subject.FollowerConfig(
                    **_config_kwargs(root, manifest),
                    availability_import=root / "reviewed.json",
                ).validated()
            with self.assertRaisesRegex(ValueError, "require availability_manifest"):
                subject.FollowerConfig(
                    **_config_kwargs(root, manifest),
                    availability_import=root / "reviewed.json",
                    provenance_root=root / "provenance",
                ).validated()

    def test_refresh_classifies_exact_snapshot_once_when_coverage_is_complete(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "download.jsonl"
            original = _write_manifest(manifest, _month_events(2025, 1, missing=True))
            availability_import = _availability_import_for_month(root, "closed")
            availability_manifest = root / "availability.jsonl"
            provenance_root = root / "provenance"
            config = _config(
                root,
                manifest,
                availability_manifest=availability_manifest,
                availability_import=availability_import,
                provenance_root=provenance_root,
            )
            classifier_calls: list[dict[str, object]] = []

            def classifier(**kwargs: object) -> object:
                classifier_calls.append(kwargs)
                return subject.import_and_classify_availability(**kwargs)

            first = subject.follow_once(
                config,
                today=date(2025, 2, 1),
                extractor=lambda **_kwargs: SimpleNamespace(ok=True),
                auditor=lambda **_kwargs: _audit("FAIL_INCOMPLETE"),
                availability_classifier=classifier,
            )
            second = subject.follow_once(
                config,
                today=date(2025, 2, 1),
                extractor=lambda **_kwargs: SimpleNamespace(ok=True),
                auditor=lambda **_kwargs: _audit("FAIL_INCOMPLETE"),
                availability_classifier=classifier,
            )

            self.assertEqual(first.incomplete_months, 1)
            self.assertEqual(second.skipped_months, 1)
            self.assertEqual(len(classifier_calls), 1)
            call = classifier_calls[0]
            self.assertEqual(call["start_date"], date(2025, 1, 1))
            self.assertEqual(call["end_date"], date(2025, 1, 31))
            self.assertEqual(Path(call["download_manifest"]).read_bytes(), original)
            self.assertNotEqual(Path(call["download_manifest"]), manifest)
            self.assertEqual(call["provenance_root"], provenance_root.resolve())
            effective_manifest = Path(call["availability_manifest"])
            self.assertNotEqual(effective_manifest, availability_manifest)
            self.assertEqual(len(subject.load_availability_events(effective_manifest)), 186)
            self.assertEqual(manifest.read_bytes(), original)

    def test_uncovered_no_entry_cells_have_one_durable_classification_attempt(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "download.jsonl"
            _write_manifest(manifest, _month_events(2025, 1, missing=True))
            availability_manifest = root / "availability.jsonl"
            config = _config(
                root,
                manifest,
                availability_manifest=availability_manifest,
                availability_import=_availability_import_for_month(
                    root, "closed", omit_day=15
                ),
                provenance_root=root / "provenance",
            )
            calls = {"classifier": 0, "audit": 0}

            def classifier(**kwargs: object) -> object:
                calls["classifier"] += 1
                return subject.import_and_classify_availability(**kwargs)

            def auditor(**_kwargs: object) -> SimpleNamespace:
                calls["audit"] += 1
                return _audit("FAIL_INCOMPLETE")

            first = subject.follow_once(
                config,
                today=date(2025, 2, 1),
                extractor=lambda **_kwargs: SimpleNamespace(ok=True),
                auditor=auditor,
                availability_classifier=classifier,
            )
            first_journal_path = _effective_manifest(config)
            first_journal = first_journal_path.read_bytes()
            later = [
                subject.follow_once(
                    config,
                    today=date(2025, 2, 1),
                    extractor=lambda **_kwargs: SimpleNamespace(ok=True),
                    auditor=auditor,
                    availability_classifier=classifier,
                )
                for _ in range(3)
            ]

            self.assertEqual(calls, {"classifier": 1, "audit": 1})
            self.assertEqual(first.incomplete_months, 1)
            self.assertEqual([report.skipped_months for report in later], [1, 1, 1])
            self.assertEqual(first_journal_path.read_bytes(), first_journal)
            self.assertEqual(len(subject.load_availability_events(first_journal_path)), 180)
            attempts = [
                event
                for event in _state_events(config.state_root, "2025-01")
                if event["event"] == "availability_classification_attempted"
            ]
            self.assertEqual(len(attempts), 1)
            self.assertEqual(len(attempts[0]["uncovered_cells"]), 6)

            import_payload = json.loads(config.availability_import.read_text(encoding="utf-8"))
            import_payload["review_revision"] = "changed-evidence-content"
            config.availability_import.write_text(
                json.dumps(import_payload), encoding="utf-8"
            )
            changed = subject.follow_once(
                config,
                today=date(2025, 2, 1),
                extractor=lambda **_kwargs: SimpleNamespace(ok=True),
                auditor=auditor,
                availability_classifier=classifier,
            )
            self.assertEqual(calls, {"classifier": 2, "audit": 2})
            self.assertEqual(changed.incomplete_months, 1)

    def test_unresolved_and_source_boundary_classifications_remain_nonfatal(self) -> None:
        scenarios = (
            ("regular_trading_day", "FAIL_INCOMPLETE", "incomplete_months"),
            ("trading_source_boundary", "BLOCKED_SOURCE", "blocked_months"),
        )
        for market_state, outcome, count_field in scenarios:
            with self.subTest(market_state=market_state), TemporaryDirectory() as tmp:
                root = Path(tmp)
                manifest = root / "download.jsonl"
                _write_manifest(manifest, _month_events(2025, 1, missing=True))
                config = _config(
                    root,
                    manifest,
                    availability_manifest=root / "availability.jsonl",
                    availability_import=_availability_import_for_month(root, market_state),
                    provenance_root=root / "provenance",
                )
                report = subject.follow_once(
                    config,
                    today=date(2025, 2, 1),
                    extractor=lambda **_kwargs: SimpleNamespace(ok=True),
                    auditor=lambda **_kwargs: _audit(outcome),
                )

                self.assertEqual(getattr(report, count_field), 1)
                self.assertEqual(dict(report.outcomes), {"2025-01": outcome})
                events = subject.load_availability_events(_effective_manifest(config))
                expected_classification = (
                    "unresolved"
                    if market_state == "regular_trading_day"
                    else "source_boundary"
                )
                self.assertEqual(
                    {event["classification_outcome"] for event in events.values()},
                    {expected_classification},
                )

    def test_changed_import_uses_fresh_journal_without_stale_acceptance(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "download.jsonl"
            _write_manifest(manifest, _month_events(2025, 1, missing=True))
            config = _config(
                root,
                manifest,
                availability_manifest=root / "availability.jsonl",
                availability_import=_availability_import_for_month(root, "closed"),
                provenance_root=root / "provenance",
            )
            calls = {"classifier": 0, "audit": 0}
            audited_paths: list[Path] = []

            def classifier(**kwargs: object) -> object:
                calls["classifier"] += 1
                return subject.import_and_classify_availability(**kwargs)

            def auditor(**kwargs: object) -> SimpleNamespace:
                calls["audit"] += 1
                path = Path(kwargs["availability_manifest"])
                audited_paths.append(path)
                event_count = len(subject.load_availability_events(path))
                return _audit("PASS_READY" if event_count == 186 else "FAIL_INCOMPLETE")

            first = subject.follow_once(
                config,
                today=date(2025, 2, 1),
                extractor=lambda **_kwargs: SimpleNamespace(ok=True),
                auditor=auditor,
                availability_classifier=classifier,
            )
            journal_a = audited_paths[-1]
            import_payload = json.loads(config.availability_import.read_text(encoding="utf-8"))
            import_payload["dates"] = []
            config.availability_import.write_text(
                json.dumps(import_payload), encoding="utf-8"
            )
            second = subject.follow_once(
                config,
                today=date(2025, 2, 1),
                extractor=lambda **_kwargs: SimpleNamespace(ok=True),
                auditor=auditor,
                availability_classifier=classifier,
            )
            journal_b = audited_paths[-1]
            third = subject.follow_once(
                config,
                today=date(2025, 2, 1),
                extractor=lambda **_kwargs: SimpleNamespace(ok=True),
                auditor=auditor,
                availability_classifier=classifier,
            )

            self.assertEqual(dict(first.outcomes), {"2025-01": "PASS_READY"})
            self.assertEqual(second.incomplete_months, 1)
            self.assertEqual(third.skipped_months, 1)
            self.assertEqual(calls, {"classifier": 2, "audit": 2})
            self.assertNotEqual(journal_a, journal_b)
            self.assertEqual(len(subject.load_availability_events(journal_a)), 186)
            self.assertEqual(len(subject.load_availability_events(journal_b)), 0)
            self.assertFalse(config.availability_manifest.exists())

    def test_invalid_existing_availability_evidence_stops_as_fatal(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "download.jsonl"
            _write_manifest(manifest, _month_events(2025, 1, missing=True))
            availability_manifest = root / "availability.jsonl"
            config = _config(
                root,
                manifest,
                availability_manifest=availability_manifest,
                availability_import=_availability_import_for_month(root, "closed"),
                provenance_root=root / "provenance",
            )
            _effective_manifest(config).write_text("{not-json}\n", encoding="utf-8")

            with self.assertRaisesRegex(
                subject.FollowerFatalError, "availability integrity check failed"
            ):
                subject.follow_once(
                    config,
                    today=date(2025, 2, 1),
                    extractor=lambda **_kwargs: SimpleNamespace(ok=True),
                    auditor=lambda **_kwargs: _audit("PASS_READY"),
                )

            self.assertEqual(
                _state_events(config.state_root, "2025-01")[-1]["event"],
                "fatal_availability",
            )


def _config(
    root: Path,
    manifest: Path,
    *,
    parse_workers: int = 4,
    availability_manifest: Path | None = None,
    availability_import: Path | None = None,
    provenance_root: Path | None = None,
) -> subject.FollowerConfig:
    return subject.FollowerConfig(
        **_config_kwargs(root, manifest),
        availability_manifest=availability_manifest,
        availability_import=availability_import,
        provenance_root=provenance_root,
        parse_workers=parse_workers,
    ).validated()


def _config_kwargs(root: Path, manifest: Path) -> dict[str, Path]:
    return {
        "download_manifest": manifest,
        "raw_root": root / "raw",
        "fragment_root": root / "fragments",
        "extraction_manifest": root / "extraction.jsonl",
        "compacted_root": root / "parquet",
        "quarantine_root": root / "quarantine",
        "report_root": root / "reports",
        "state_root": root / "follower-state",
    }


def _effective_manifest(config: subject.FollowerConfig) -> Path:
    journal = subject._effective_availability_journal(
        config.availability_manifest, config.availability_import
    )
    assert journal.path is not None
    return journal.path


def _month_events(
    year: int, month: int, *, missing: bool = False
) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    day = date(year, month, 1)
    while day.month == month:
        for slot, suffix in subject.SLOT_SPECS:
            events.append(
                {
                    "observed_at_utc": f"{day.isoformat()}T03:30:00Z",
                    "trading_date": day.isoformat(),
                    "slot": slot,
                    "suffix": suffix,
                    "state": "not_returned_http_404" if missing else "downloaded",
                    "terminal": True,
                    "path": f"{year:04d}/{month:02d}/{day.day:02d}/nsccl.{day:%Y%m%d}.{suffix}.zip",
                    "sha256": f"{day:%Y%m%d}{suffix}".encode().hex().ljust(64, "0")[:64],
                    "size_bytes": 100,
                }
            )
        day = date.fromordinal(day.toordinal() + 1)
    return events


def _availability_import_for_month(
    root: Path, market_state: str, *, omit_day: int | None = None
) -> Path:
    source = root / "official-source.pdf"
    source.write_bytes(b"reviewed official availability source fixture")
    dates = []
    day = date(2025, 1, 1)
    while day.month == 1:
        if day.day == omit_day:
            day = date.fromordinal(day.toordinal() + 1)
            continue
        entry: dict[str, object] = {
            "date": day.isoformat(),
            "market_state": market_state,
            "reason": "Reviewed official classification for follower integration test.",
            "source_ids": ["official-source"],
        }
        if market_state == "closed":
            entry["classification"] = "official_holiday"
        dates.append(entry)
        day = date.fromordinal(day.toordinal() + 1)
    payload = {
        "schema_version": IMPORT_SCHEMA,
        "sources": [
            {
                "id": "official-source",
                "url": "https://nsearchives.nseindia.com/content/circulars/fixture.pdf",
                "path": str(source),
                "fetched_at_utc": "2026-07-15T10:00:00+00:00",
            }
        ],
        "dates": dates,
    }
    path = root / "availability-import.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _write_manifest(path: Path, events: list[dict[str, object]]) -> bytes:
    content = "".join(json.dumps(event, sort_keys=True) + "\n" for event in events).encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return content


def _audit(
    outcome: str,
    *,
    downloaded_cells: int = 0,
    months: tuple[SimpleNamespace, ...] = (),
) -> SimpleNamespace:
    return SimpleNamespace(
        outcome=outcome,
        downloaded_cells=downloaded_cells,
        months=months,
        expected_cells=186,
        terminal_cells=186,
        unavailable_cells=0,
        failed_or_incomplete_cells=0,
        raw_integrity_failures=0,
        downloaded_without_valid_extraction=0,
        duplicate_natural_keys=0,
    )


def _state_events(state_root: Path, month: str) -> list[dict[str, object]]:
    return [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((state_root / "events" / month).glob("*.json"))
    ]
