from __future__ import annotations

from datetime import date
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
import json
import subprocess
import sys
import zipfile

from nifty_span.span.availability import (
    import_and_classify_availability,
    load_availability_events,
)
from nifty_span.span.backfill_audit import audit_span_backfill
from nifty_span.span.backfill_downloader import SLOT_SPECS
from nifty_span.span.corrupt_recovery import (
    CorruptRecoveryConfig,
    _acquire_lock,
    _release_lock,
    build_corrupt_recovery_command,
    recover_corrupt_span_cells,
    static_archive_url,
    validate_corrupt_recovery_report,
)
from nifty_span.span.postrun_orchestrator import ProcessRecord


DAY = date(2021, 10, 11)
SLOT = "EOD"
SUFFIX = "s"


class _Response:
    def __init__(self, content: bytes, *, status: int = 200) -> None:
        self.status_code = status
        self.content = content
        self.headers = {
            "Content-Type": "application/zip",
            "Content-Length": str(len(content)),
            "ETag": '"safe-etag"',
            "Set-Cookie": "session=must-not-appear",
            "Authorization": "Bearer must-not-appear",
        }


class _Client:
    def __init__(self, responses: list[_Response]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, float]] = []
        self.closed = False

    async def get(self, url: str, **kwargs: object) -> _Response:
        self.calls.append((url, float(kwargs["timeout"])))
        if len(self.responses) == 1:
            return self.responses[0]
        return self.responses.pop(0)

    async def aclose(self) -> None:
        self.closed = True


def _inner_zip(day: date, suffix: str) -> bytes:
    stream = BytesIO()
    inner_suffix = f"i0{suffix[1]}" if suffix.startswith("i") else "s"
    with zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            f"nsccl.{day:%Y%m%d}.{inner_suffix}.spn",
            b"<spanFile><point/></spanFile>",
        )
    return stream.getvalue()


def _write_corrupt_manifest(path: Path, rejected: bytes) -> dict[str, object]:
    event: dict[str, object] = {
        "schema_version": 1,
        "event_id": "reports-api-event-1",
        "run_id": "reports-api-run-1",
        "observed_at_utc": "2026-07-15T12:00:00Z",
        "trading_date": DAY.isoformat(),
        "slot": SLOT,
        "suffix": SUFFIX,
        "state": "corrupt_inner_zip",
        "terminal": False,
        "attempt": 4,
        "http_status": 200,
        "outer_member": {
            "name": f"nsccl.{DAY:%Y%m%d}.{SUFFIX}.zip",
            "crc32": "00000000",
        },
        "rejected_inner": {
            "sha256": sha256(rejected).hexdigest(),
            "size_bytes": len(rejected),
        },
        "returned_suffixes": ["i1", "i2", "i3", "i4", "i5", "s"],
        "response": {
            "body_sha256": "a" * 64,
            "body_size_bytes": 123,
            "content_type": "application/zip",
        },
    }
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(event) + "\n", encoding="utf-8")
    return event


def _write_corrupt_bundle_manifest(path: Path, rejected: bytes) -> None:
    corrupt = _write_corrupt_manifest(path, rejected)
    blocked = {
        "schema_version": 1,
        "event_id": "reports-api-blocked-bod-1",
        "run_id": corrupt["run_id"],
        "observed_at_utc": corrupt["observed_at_utc"],
        "trading_date": DAY.isoformat(),
        "slot": "BOD",
        "suffix": "i1",
        "state": "bundle_validation_blocked",
        "terminal": False,
        "attempt": corrupt["attempt"],
        "http_status": 200,
        "response": corrupt["response"],
    }
    path.write_text(
        json.dumps(blocked) + "\n" + json.dumps(corrupt) + "\n",
        encoding="utf-8",
    )


def _write_missing_manifest(path: Path) -> dict[str, object]:
    event: dict[str, object] = {
        "schema_version": 1,
        "event_id": "reports-api-missing-1",
        "run_id": "reports-api-run-1",
        "observed_at_utc": "2026-07-15T12:00:00Z",
        "trading_date": DAY.isoformat(),
        "slot": SLOT,
        "suffix": SUFFIX,
        "state": "slot_not_returned",
        "terminal": True,
        "attempt": 1,
        "http_status": 200,
        "returned_suffixes": ["i1", "i2", "i3", "i4", "i5"],
    }
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(event) + "\n", encoding="utf-8")
    return event


def _classify_other_slots_as_boundaries(root: Path) -> None:
    manifest = root / "manifests" / "download.jsonl"
    with manifest.open("a", encoding="utf-8", newline="\n") as handle:
        for slot, suffix in SLOT_SPECS:
            if slot == SLOT:
                continue
            handle.write(
                json.dumps(
                    {
                        "schema_version": 1,
                        "event_id": f"missing-{slot}",
                        "trading_date": DAY.isoformat(),
                        "slot": slot,
                        "suffix": suffix,
                        "state": "slot_not_returned",
                        "terminal": True,
                    }
                )
                + "\n"
            )
    source = root / "official-boundary.txt"
    source.write_text("official source boundary fixture", encoding="utf-8")
    import_path = root / "availability-import.json"
    import_path.write_text(
        json.dumps(
            {
                "schema_version": "span-availability-import/v1",
                "sources": [
                    {
                        "id": "official-boundary",
                        "url": "https://nsearchives.nseindia.com/official-boundary",
                        "path": source.name,
                        "sha256": sha256(source.read_bytes()).hexdigest(),
                        "fetched_at_utc": "2026-07-15T12:00:00Z",
                    }
                ],
                "dates": [
                    {
                        "date": DAY.isoformat(),
                        "market_state": "trading_source_boundary",
                        "reason": "Official fixture proves the test boundary.",
                        "source_ids": ["official-boundary"],
                    }
                ],
                "weekly_rules": [],
            }
        ),
        encoding="utf-8",
    )
    import_and_classify_availability(
        start_date=DAY,
        end_date=DAY,
        import_path=import_path,
        download_manifest=manifest,
        availability_manifest=root / "manifests" / "availability.jsonl",
        provenance_root=root / "availability_sources",
    )


def _run(
    root: Path,
    client: _Client,
    *,
    rejected: bytes,
    process_provider=lambda: (),
    max_attempts: int = 3,
):
    manifest = root / "manifests" / "download.jsonl"
    _write_corrupt_manifest(manifest, rejected)
    return _recover(
        root, client, process_provider=process_provider, max_attempts=max_attempts
    )


def _recover(
    root: Path,
    client: _Client,
    *,
    process_provider=lambda: (),
    max_attempts: int = 3,
):
    manifest = root / "manifests" / "download.jsonl"
    return recover_corrupt_span_cells(
        start_date=DAY,
        end_date=DAY,
        raw_root=root / "raw",
        download_manifest=manifest,
        availability_manifest=root / "manifests" / "availability.jsonl",
        report_root=root / "reports" / "corrupt_recovery",
        config=CorruptRecoveryConfig(
            max_attempts=max_attempts,
            timeout_seconds=600,
            backoff_base_seconds=0,
            backoff_max_seconds=0,
            jitter_seconds=0,
        ),
        client_factory=lambda _worker: client,
        process_provider=process_provider,
    )


class SpanCorruptRecoveryTests(TestCase):
    def test_exact_static_recovery_includes_only_paired_blocked_bundle_companions(
        self,
    ) -> None:
        rejected = b"PK\x03\x04same-official-corruption"
        valid_bod = _inner_zip(DAY, "i1")
        client = _Client([_Response(valid_bod), _Response(rejected)])
        with TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = root / "manifests" / "download.jsonl"
            _write_corrupt_bundle_manifest(manifest, rejected)

            report = _recover(root, client, max_attempts=1)

            self.assertTrue(report.ok)
            self.assertEqual(report.selected_cells, 2)
            self.assertEqual(report.recovered_cells, 1)
            self.assertEqual(report.classified_source_corrupt_cells, 1)
            self.assertEqual(report.unresolved_cells, 0)
            self.assertEqual(
                client.calls,
                [
                    (static_archive_url(DAY, "i1"), 600.0),
                    (static_archive_url(DAY, SUFFIX), 600.0),
                ],
            )
            self.assertEqual(
                [cell.source_state for cell in report.cells],
                ["bundle_validation_blocked", "corrupt_inner_zip"],
            )
            evidence = validate_corrupt_recovery_report(
                report.json_report,
                start_date=DAY,
                end_date=DAY,
                raw_root=root / "raw",
                download_manifest=manifest,
                availability_manifest=root / "manifests" / "availability.jsonl",
            )
            self.assertTrue(evidence["ok"])

    def test_unpaired_blocked_bundle_cell_is_fatal_and_never_downloaded(self) -> None:
        rejected = b"PK\x03\x04same-official-corruption"
        with TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = root / "manifests" / "download.jsonl"
            _write_corrupt_bundle_manifest(manifest, rejected)
            events = [json.loads(line) for line in manifest.read_text().splitlines()]
            events[0]["run_id"] = "different-reports-api-run"
            manifest.write_text(
                "".join(json.dumps(event) + "\n" for event in events),
                encoding="utf-8",
            )
            client = _Client([_Response(rejected)])

            report = _recover(root, client, max_attempts=1)

            self.assertFalse(report.ok)
            self.assertEqual(report.selected_cells, 2)
            self.assertEqual(report.unresolved_cells, 1)
            self.assertEqual(report.unresolved_missing_cells, 1)
            self.assertEqual(report.cells[0].network_attempts, 0)
            self.assertEqual(
                report.cells[0].disposition, "unresolved_manifest_schema_error"
            )
            self.assertEqual(client.calls, [(static_archive_url(DAY, SUFFIX), 600.0)])

    def test_nonterminal_corrupt_boundary_blocks_only_when_every_other_cell_is_resolved(
        self,
    ) -> None:
        corrupt = b"PK\x03\x04same-official-corruption"
        with TemporaryDirectory() as temp:
            root = Path(temp)
            report = _run(root, _Client([_Response(corrupt)]), rejected=corrupt)
            self.assertTrue(report.ok)
            evidence = validate_corrupt_recovery_report(
                report.json_report,
                start_date=DAY,
                end_date=DAY,
                raw_root=root / "raw",
                download_manifest=root / "manifests" / "download.jsonl",
                availability_manifest=root / "manifests" / "availability.jsonl",
            )
            self.assertTrue(evidence["ok"])
            self.assertTrue(evidence["artifact_sha256"])
            _classify_other_slots_as_boundaries(root)
            audit = audit_span_backfill(
                start_date=DAY,
                end_date=DAY,
                raw_root=root / "raw",
                download_manifest=root / "manifests" / "download.jsonl",
                extraction_manifest=root / "manifests" / "extraction.jsonl",
                fragment_root=root / "fragments",
                compacted_root=root / "compacted",
                report_root=root / "reports" / "audit-complete-boundary",
                availability_manifest=root / "manifests" / "availability.jsonl",
            )
            self.assertEqual(audit.outcome, "BLOCKED_SOURCE")
            self.assertTrue(audit.blocked_matrix_complete)
            self.assertEqual(audit.accounted_cells, 6)
            self.assertEqual(audit.resolved_or_blocked_cells, 6)
            self.assertEqual(audit.source_boundary_cells, 6)
            self.assertEqual(audit.unresolved_non_boundary_cells, 0)

    def test_stale_boundary_is_not_counted_after_latest_download_recovery(self) -> None:
        corrupt = b"PK\x03\x04same-official-corruption"
        with TemporaryDirectory() as temp:
            root = Path(temp)
            _run(root, _Client([_Response(corrupt)]), rejected=corrupt)
            _classify_other_slots_as_boundaries(root)
            manifest = root / "manifests" / "download.jsonl"
            with manifest.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(
                    json.dumps(
                        {
                            "schema_version": 1,
                            "event_id": "latest-recovery",
                            "trading_date": DAY.isoformat(),
                            "slot": SLOT,
                            "suffix": SUFFIX,
                            "state": "downloaded",
                            "terminal": True,
                            "path": "2021/10/11/nsccl.20211011.s.zip",
                            "sha256": "a" * 64,
                            "size_bytes": 1,
                        }
                    )
                    + "\n"
                )
            audit = audit_span_backfill(
                start_date=DAY,
                end_date=DAY,
                raw_root=root / "raw",
                download_manifest=manifest,
                extraction_manifest=root / "manifests" / "extraction.jsonl",
                fragment_root=root / "fragments",
                compacted_root=root / "compacted",
                report_root=root / "reports" / "audit-stale-boundary",
                availability_manifest=root / "manifests" / "availability.jsonl",
            )
            self.assertEqual(audit.source_boundary_cells, 5)
            self.assertTrue(audit.blocked_matrix_complete)
            self.assertEqual(audit.outcome, "FAIL_INTEGRITY")

    def test_stale_corrupt_boundary_is_not_counted_for_changed_latest_bytes(
        self,
    ) -> None:
        corrupt = b"PK\x03\x04same-official-corruption"
        with TemporaryDirectory() as temp:
            root = Path(temp)
            _run(root, _Client([_Response(corrupt)]), rejected=corrupt)
            _classify_other_slots_as_boundaries(root)
            changed = b"PK\x03\x04different-corruption"
            manifest = root / "manifests" / "download.jsonl"
            with manifest.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(
                    json.dumps(
                        {
                            "schema_version": 1,
                            "event_id": "new-reports-api-event",
                            "trading_date": DAY.isoformat(),
                            "slot": SLOT,
                            "suffix": SUFFIX,
                            "state": "corrupt_inner_zip",
                            "terminal": False,
                            "rejected_inner": {
                                "sha256": sha256(changed).hexdigest(),
                                "size_bytes": len(changed),
                            },
                        }
                    )
                    + "\n"
                )
            audit = audit_span_backfill(
                start_date=DAY,
                end_date=DAY,
                raw_root=root / "raw",
                download_manifest=manifest,
                extraction_manifest=root / "manifests" / "extraction.jsonl",
                fragment_root=root / "fragments",
                compacted_root=root / "compacted",
                report_root=root / "reports" / "audit-stale-corrupt-boundary",
                availability_manifest=root / "manifests" / "availability.jsonl",
            )
            self.assertEqual(audit.source_boundary_cells, 5)
            self.assertFalse(audit.blocked_matrix_complete)
            self.assertEqual(audit.unresolved_non_boundary_cells, 1)
            self.assertEqual(audit.outcome, "FAIL_INCOMPLETE")

    def test_valid_static_archive_recovers_missing_reports_api_slot(self) -> None:
        valid = _inner_zip(DAY, SUFFIX)
        client = _Client([_Response(valid)])
        with TemporaryDirectory() as temp:
            root = Path(temp)
            _write_missing_manifest(root / "manifests" / "download.jsonl")
            report = _recover(root, client)

            self.assertTrue(report.ok)
            self.assertEqual(report.recovered_cells, 1)
            self.assertEqual(report.unresolved_missing_cells, 0)
            self.assertEqual(report.cells[0].source_state, "slot_not_returned")
            self.assertEqual(client.calls, [(static_archive_url(DAY, SUFFIX), 600.0)])

            second_client = _Client([])
            second = _recover(root, second_client)
            self.assertTrue(second.ok)
            self.assertEqual(second.selected_cells, 0)
            self.assertEqual(second_client.calls, [])

    def test_three_identical_static_404s_prove_source_boundary_not_absence(
        self,
    ) -> None:
        body = b"official archive not found"
        client = _Client([_Response(body, status=404)])
        with TemporaryDirectory() as temp:
            root = Path(temp)
            _write_missing_manifest(root / "manifests" / "download.jsonl")
            report = _recover(root, client)

            self.assertTrue(report.ok)
            self.assertEqual(report.unresolved_corrupt_cells, 0)
            self.assertEqual(report.unresolved_missing_cells, 0)
            self.assertEqual(report.classified_source_absent_cells, 1)
            self.assertEqual(report.network_calls, 3)
            self.assertEqual(
                report.cells[0].disposition, "official_source_archive_absent"
            )
            self.assertEqual(report.cells[0].static_status, 404)
            self.assertEqual(report.cells[0].static_sha256, sha256(body).hexdigest())
            self.assertEqual(report.cells[0].static_size_bytes, len(body))
            availability = load_availability_events(
                root / "manifests" / "availability.jsonl"
            )[(DAY.isoformat(), SLOT)]
            self.assertEqual(availability["classification_outcome"], "source_boundary")
            self.assertFalse(availability["historical_nonpublication_proven"])
            self.assertFalse(availability["exact_payload_match"])
            self.assertEqual(len(availability["static_archive_observations"]), 3)

            second = _recover(root, _Client([]))
            self.assertTrue(second.ok)
            self.assertEqual(second.selected_cells, 0)

    def test_static_404_requires_three_identical_completed_observations(self) -> None:
        body = b"official archive not found"
        cases = (
            ([_Response(body, status=404)], 2),
            (
                [
                    _Response(body, status=404),
                    _Response(b"temporary", status=500),
                    _Response(body, status=404),
                ],
                3,
            ),
            (
                [
                    _Response(body, status=404),
                    _Response(body, status=404),
                    _Response(b"different", status=404),
                ],
                3,
            ),
        )
        for responses, max_attempts in cases:
            with self.subTest(max_attempts=max_attempts), TemporaryDirectory() as temp:
                root = Path(temp)
                _write_missing_manifest(root / "manifests" / "download.jsonl")
                report = _recover(root, _Client(responses), max_attempts=max_attempts)

                self.assertFalse(report.ok)
                self.assertEqual(report.unresolved_missing_cells, 1)
                self.assertEqual(report.classified_source_absent_cells, 0)
                self.assertFalse((root / "manifests" / "availability.jsonl").exists())

    def test_valid_static_archive_recovers_through_immutable_raw_path(self) -> None:
        rejected = b"PK\x03\x04reports-api-corrupt"
        valid = _inner_zip(DAY, SUFFIX)
        client = _Client([_Response(valid)])
        with TemporaryDirectory() as temp:
            root = Path(temp)
            report = _run(root, client, rejected=rejected)

            self.assertTrue(report.ok)
            self.assertEqual(report.recovered_cells, 1)
            self.assertEqual(report.classified_source_corrupt_cells, 0)
            self.assertEqual(report.network_calls, 1)
            self.assertEqual(client.calls, [(static_archive_url(DAY, SUFFIX), 600.0)])
            canonical = root / "raw" / "2021" / "10" / "11" / "nsccl.20211011.s.zip"
            self.assertEqual(canonical.read_bytes(), valid)
            with zipfile.ZipFile(canonical) as archive:
                self.assertIsNone(archive.testzip())
            events = [
                json.loads(line)
                for line in (root / "manifests" / "download.jsonl")
                .read_text()
                .splitlines()
            ]
            recovered = events[-1]
            self.assertEqual(recovered["state"], "downloaded")
            self.assertTrue(recovered["terminal"])
            self.assertEqual(recovered["sha256"], sha256(valid).hexdigest())
            self.assertEqual(recovered["members"], ["nsccl.20211011.s.spn"])
            self.assertFalse((root / "manifests" / "availability.jsonl").exists())
            self.assertTrue(Path(report.json_report).is_file())
            self.assertTrue(Path(report.markdown_report).is_file())

    def test_exact_corrupt_payload_proves_boundary_and_excludes_raw_bytes(self) -> None:
        corrupt = b"PK\x03\x04truncated-official-object"
        client = _Client([_Response(corrupt)])
        with TemporaryDirectory() as temp:
            root = Path(temp)
            report = _run(root, client, rejected=corrupt)

            self.assertTrue(report.ok)
            self.assertEqual(report.classified_source_corrupt_cells, 1)
            self.assertEqual(report.recovered_cells, 0)
            self.assertFalse((root / "raw").exists())
            availability_path = root / "manifests" / "availability.jsonl"
            latest = load_availability_events(availability_path)[
                (DAY.isoformat(), SLOT)
            ]
            self.assertEqual(
                latest["calendar_classification"], "official_source_corrupt_archive"
            )
            self.assertTrue(latest["source_availability_boundary_proven"])
            self.assertFalse(latest["raw_persisted"])
            self.assertIsNone(latest["canonical_archive_path"])
            self.assertEqual(
                latest["static_archive_evidence"]["body_sha256"],
                latest["reports_api_evidence"]["rejected_inner"]["sha256"],
            )
            all_evidence = (
                Path(report.json_report).read_text(encoding="utf-8")
                + Path(report.markdown_report).read_text(encoding="utf-8")
                + availability_path.read_text(encoding="utf-8")
            ).lower()
            self.assertNotIn("must-not-appear", all_evidence)
            self.assertNotIn("authorization", all_evidence)
            self.assertNotIn("set-cookie", all_evidence)

    def test_different_corrupt_payload_stays_unresolved_and_retries_bounded(
        self,
    ) -> None:
        reports_corrupt = b"PK\x03\x04reports-api-version"
        static_corrupt = b"PK\x03\x04different-static-version"
        client = _Client([_Response(static_corrupt)])
        with TemporaryDirectory() as temp:
            root = Path(temp)
            report = _run(root, client, rejected=reports_corrupt, max_attempts=3)

            self.assertFalse(report.ok)
            self.assertEqual(report.unresolved_cells, 1)
            self.assertEqual(report.network_calls, 3)
            self.assertEqual(
                report.cells[0].disposition, "unresolved_evidence_mismatch"
            )
            self.assertFalse((root / "raw").exists())
            self.assertFalse((root / "manifests" / "availability.jsonl").exists())

    def test_exact_payload_with_non_corrupt_validation_state_never_proves_boundary(
        self,
    ) -> None:
        stream = BytesIO()
        with zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("not-a-span-member.txt", b"valid zip, wrong contract")
        missing_spn = stream.getvalue()
        with TemporaryDirectory() as temp:
            root = Path(temp)
            report = _run(root, _Client([_Response(missing_spn)]), rejected=missing_spn)

            self.assertFalse(report.ok)
            self.assertEqual(report.selected_cells, 1)
            self.assertEqual(report.unresolved_cells, 1)
            self.assertEqual(report.classified_source_corrupt_cells, 0)
            self.assertEqual(
                report.cells[0].disposition,
                "unresolved_non_corrupt_validation_state",
            )
            self.assertEqual(report.cells[0].validation_state, "missing_spn")
            self.assertFalse((root / "manifests" / "availability.jsonl").exists())
            self.assertFalse((root / "raw").exists())

    def test_incomplete_rejected_inner_evidence_is_selected_and_unresolved(
        self,
    ) -> None:
        corrupt = b"PK\x03\x04invalid-static-zip"
        mutations = (
            lambda event: event.pop("rejected_inner"),
            lambda event: event.__setitem__(
                "rejected_inner", {"sha256": "malformed", "size_bytes": "unknown"}
            ),
        )
        for mutate in mutations:
            with self.subTest(mutate=mutate), TemporaryDirectory() as temp:
                root = Path(temp)
                manifest = root / "manifests" / "download.jsonl"
                event = _write_corrupt_manifest(manifest, corrupt)
                mutate(event)
                manifest.write_text(json.dumps(event) + "\n", encoding="utf-8")
                report = _recover(
                    root,
                    _Client([_Response(corrupt)]),
                    max_attempts=1,
                )

                self.assertFalse(report.ok)
                self.assertEqual(report.selected_cells, 1)
                self.assertEqual(report.unresolved_cells, 1)
                self.assertEqual(
                    report.cells[0].disposition,
                    "unresolved_insufficient_reports_api_evidence",
                )
                self.assertFalse((root / "manifests" / "availability.jsonl").exists())

    def test_three_identical_static_corruptions_prove_legacy_source_boundary(
        self,
    ) -> None:
        corrupt = b"PK\x03\x04legacy-invalid-static-zip"
        with TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = root / "manifests" / "download.jsonl"
            event = _write_corrupt_manifest(manifest, corrupt)
            event.pop("rejected_inner")
            manifest.write_text(json.dumps(event) + "\n", encoding="utf-8")

            report = _recover(root, _Client([_Response(corrupt)]), max_attempts=3)

            self.assertTrue(report.ok)
            self.assertEqual(report.network_calls, 3)
            self.assertEqual(report.classified_source_corrupt_cells, 1)
            self.assertEqual(
                report.cells[0].evidence_basis,
                "repeated_http200_corrupt_inner_zip",
            )
            availability = load_availability_events(
                root / "manifests" / "availability.jsonl"
            )[(DAY.isoformat(), SLOT)]
            self.assertEqual(availability["classification_outcome"], "source_boundary")
            self.assertFalse(availability["exact_payload_match"])
            self.assertFalse(availability["historical_nonpublication_proven"])
            self.assertEqual(len(availability["static_archive_observations"]), 3)
            self.assertFalse((root / "raw").exists())

            evidence = validate_corrupt_recovery_report(
                report.json_report,
                start_date=DAY,
                end_date=DAY,
                raw_root=root / "raw",
                download_manifest=manifest,
                availability_manifest=root / "manifests" / "availability.jsonl",
            )
            self.assertTrue(evidence["ok"])

            _classify_other_slots_as_boundaries(root)
            audit = audit_span_backfill(
                start_date=DAY,
                end_date=DAY,
                raw_root=root / "raw",
                download_manifest=manifest,
                extraction_manifest=root / "manifests" / "extraction.jsonl",
                fragment_root=root / "fragments",
                compacted_root=root / "compacted",
                report_root=root / "reports" / "audit-repeated-static-boundary",
                availability_manifest=root / "manifests" / "availability.jsonl",
            )
            self.assertEqual(audit.outcome, "BLOCKED_SOURCE")
            self.assertTrue(audit.blocked_matrix_complete)

            with manifest.open("a", encoding="utf-8", newline="\n") as handle:
                changed = dict(event)
                changed["event_id"] = "later-same-state-event"
                handle.write(json.dumps(changed) + "\n")
            stale = audit_span_backfill(
                start_date=DAY,
                end_date=DAY,
                raw_root=root / "raw",
                download_manifest=manifest,
                extraction_manifest=root / "manifests" / "extraction.jsonl",
                fragment_root=root / "fragments",
                compacted_root=root / "compacted",
                report_root=root / "reports" / "audit-stale-repeated-boundary",
                availability_manifest=root / "manifests" / "availability.jsonl",
            )
            self.assertEqual(stale.outcome, "FAIL_INCOMPLETE")
            self.assertFalse(stale.blocked_matrix_complete)

    def test_repeated_static_corruption_requires_identical_third_payload(self) -> None:
        corrupt = b"PK\x03\x04legacy-invalid-static-zip"
        changed = b"PK\x03\x04changed-invalid-static-zip"
        with TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = root / "manifests" / "download.jsonl"
            event = _write_corrupt_manifest(manifest, corrupt)
            event.pop("rejected_inner")
            manifest.write_text(json.dumps(event) + "\n", encoding="utf-8")

            report = _recover(
                root,
                _Client([_Response(corrupt), _Response(corrupt), _Response(changed)]),
                max_attempts=3,
            )

            self.assertFalse(report.ok)
            self.assertEqual(report.unresolved_corrupt_cells, 1)
            self.assertEqual(report.classified_source_corrupt_cells, 0)
            self.assertFalse((root / "manifests" / "availability.jsonl").exists())

    def test_malformed_slot_suffix_corrupt_candidate_is_fatal_not_skipped(self) -> None:
        corrupt = b"PK\x03\x04invalid-static-zip"
        with TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = root / "manifests" / "download.jsonl"
            event = _write_corrupt_manifest(manifest, corrupt)
            event["suffix"] = "i5"
            manifest.write_text(json.dumps(event) + "\n", encoding="utf-8")
            client = _Client([])

            report = _recover(root, client)

            self.assertFalse(report.ok)
            self.assertEqual(report.selected_cells, 1)
            self.assertEqual(report.unresolved_cells, 1)
            self.assertEqual(report.network_calls, 0)
            self.assertEqual(client.calls, [])
            self.assertEqual(
                report.cells[0].disposition, "unresolved_manifest_schema_error"
            )
            self.assertIn("slot/suffix", report.cells[0].error or "")

    def test_invalid_date_corrupt_candidate_is_fatal_not_silently_dropped(self) -> None:
        corrupt = b"PK\x03\x04invalid-static-zip"
        with TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = root / "manifests" / "download.jsonl"
            event = _write_corrupt_manifest(manifest, corrupt)
            event["trading_date"] = "not-a-date"
            manifest.write_text(json.dumps(event) + "\n", encoding="utf-8")
            client = _Client([])

            report = _recover(root, client)

            self.assertFalse(report.ok)
            self.assertEqual(report.selected_cells, 1)
            self.assertEqual(report.unresolved_cells, 1)
            self.assertEqual(report.network_calls, 0)
            self.assertEqual(client.calls, [])
            self.assertEqual(report.cells[0].validation_state, "manifest_schema_error")
            self.assertIn("trading_date", report.cells[0].error or "")

    def test_valid_existing_canonical_resumes_without_network_after_publish_crash(
        self,
    ) -> None:
        rejected = b"PK\x03\x04reports-api-corrupt"
        valid = _inner_zip(DAY, SUFFIX)
        with TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = root / "manifests" / "download.jsonl"
            _write_corrupt_manifest(manifest, rejected)
            canonical = root / "raw" / "2021" / "10" / "11" / "nsccl.20211011.s.zip"
            canonical.parent.mkdir(parents=True)
            canonical.write_bytes(valid)
            client = _Client([])

            report = _recover(root, client)

            self.assertTrue(report.ok)
            self.assertEqual(report.network_calls, 0)
            self.assertEqual(client.calls, [])
            self.assertEqual(report.recovered_cells, 1)
            self.assertEqual(report.cells[0].disposition, "downloaded_existing")
            events = [json.loads(line) for line in manifest.read_text().splitlines()]
            resumed = events[-1]
            self.assertEqual(resumed["state"], "downloaded_existing")
            self.assertEqual(resumed["recovery_mode"], "existing_canonical_revalidated")
            self.assertTrue(
                resumed["existing_canonical"]["revalidated_without_network"]
            )
            self.assertEqual(resumed["sha256"], sha256(valid).hexdigest())

    def test_active_downloader_for_exact_manifest_refuses_before_network(self) -> None:
        rejected = b"PK\x03\x04corrupt"
        with TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = root / "manifests" / "download.jsonl"
            _write_corrupt_manifest(manifest, rejected)
            client = _Client([_Response(rejected)])
            writer = ProcessRecord(
                pid=123,
                parent_pid=1,
                name="python.exe",
                command_line=(
                    "python -m nifty_span.cli span-backfill download "
                    f'--download-manifest "{manifest}"'
                ),
            )
            with self.assertRaisesRegex(
                RuntimeError, "downloader targets this manifest"
            ):
                recover_corrupt_span_cells(
                    start_date=DAY,
                    end_date=DAY,
                    raw_root=root / "raw",
                    download_manifest=manifest,
                    availability_manifest=root / "manifests" / "availability.jsonl",
                    report_root=root / "reports",
                    client_factory=lambda _worker: client,
                    process_provider=lambda: (writer,),
                )
            self.assertEqual(client.calls, [])
            lock_path = manifest.parent / f".{manifest.name}.corrupt-recovery.lock"
            self.assertTrue(lock_path.exists())
            descriptor = _acquire_lock(lock_path, "proof-released")
            _release_lock(descriptor)

    def test_process_crash_releases_os_lock_and_stale_file_is_resumable(self) -> None:
        with TemporaryDirectory() as temp:
            lock_path = Path(temp) / "recovery.lock"
            code = (
                "import os; "
                "from nifty_span.span.corrupt_recovery import _acquire_lock; "
                f"_acquire_lock({str(lock_path)!r}, 'crashing-child'); "
                "os._exit(0)"
            )
            completed = subprocess.run([sys.executable, "-c", code], check=False)
            self.assertEqual(completed.returncode, 0)
            self.assertTrue(lock_path.exists())
            descriptor = _acquire_lock(lock_path, "resumed-parent")
            _release_lock(descriptor)

    def test_recovery_and_postrun_modules_import_in_both_orders(self) -> None:
        for imports in (
            "import nifty_span.span.corrupt_recovery; import nifty_span.span.postrun_orchestrator",
            "import nifty_span.span.postrun_orchestrator; import nifty_span.span.corrupt_recovery",
        ):
            with self.subTest(imports=imports):
                completed = subprocess.run(
                    [sys.executable, "-c", imports],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_corrupt_boundary_does_not_mask_unrelated_missing_cells(self) -> None:
        corrupt = b"PK\x03\x04same-official-corruption"
        with TemporaryDirectory() as temp:
            root = Path(temp)
            report = _run(root, _Client([_Response(corrupt)]), rejected=corrupt)
            self.assertTrue(report.ok)
            audit = audit_span_backfill(
                start_date=DAY,
                end_date=DAY,
                raw_root=root / "raw",
                download_manifest=root / "manifests" / "download.jsonl",
                extraction_manifest=root / "manifests" / "extraction.jsonl",
                fragment_root=root / "fragments",
                compacted_root=root / "compacted",
                report_root=root / "reports" / "audit",
                availability_manifest=root / "manifests" / "availability.jsonl",
            )
            self.assertEqual(audit.outcome, "FAIL_INCOMPLETE")
            self.assertFalse(audit.ok)
            self.assertEqual(audit.source_boundary_cells, 1)
            self.assertEqual(audit.unresolved_non_boundary_cells, 5)
            event_text = (root / "manifests" / "availability.jsonl").read_text(
                encoding="utf-8"
            )
            self.assertNotIn("historical_nonpublication_proven", event_text)

    def test_classified_cell_resume_is_idempotent_and_makes_zero_network_calls(
        self,
    ) -> None:
        corrupt = b"PK\x03\x04same-official-corruption"
        with TemporaryDirectory() as temp:
            root = Path(temp)
            first_client = _Client([_Response(corrupt)])
            first = _run(root, first_client, rejected=corrupt)
            self.assertTrue(first.ok)

            second_client = _Client([])
            second = recover_corrupt_span_cells(
                start_date=DAY,
                end_date=DAY,
                raw_root=root / "raw",
                download_manifest=root / "manifests" / "download.jsonl",
                availability_manifest=root / "manifests" / "availability.jsonl",
                report_root=root / "reports" / "corrupt_recovery",
                client_factory=lambda _worker: second_client,
                process_provider=lambda: (),
            )
            self.assertTrue(second.ok)
            self.assertEqual(second.selected_cells, 0)
            self.assertEqual(second.already_classified_cells, 0)
            self.assertEqual(second.network_calls, 0)
            self.assertEqual(second_client.calls, [])
            self.assertEqual(
                len(
                    (root / "manifests" / "availability.jsonl").read_text().splitlines()
                ),
                1,
            )

    def test_pure_postrun_command_has_fixed_safe_defaults_and_no_secrets(self) -> None:
        command = build_corrupt_recovery_command(
            python_executable="python.exe",
            start_date=DAY,
            end_date=DAY,
            raw_root="raw",
            download_manifest="download.jsonl",
            availability_manifest="availability.jsonl",
            report_root="reports",
        )
        self.assertEqual(command[4:6], ("span-backfill", "recover-corrupt"))
        self.assertEqual(command[command.index("--corrupt-timeout-seconds") + 1], "600")
        self.assertEqual(command[command.index("--corrupt-max-attempts") + 1], "3")
        self.assertNotIn("--download-concurrency", command)
        command_text = " ".join(command).lower()
        for secret_name in ("access-token", "authorization", "password", "secret"):
            self.assertNotIn(secret_name, command_text)
