from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import date, timedelta
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import nifty_span.span.postrun_orchestrator as subject

from nifty_span.span.postrun_orchestrator import (
    apply_benchmark_wait,
    build_availability_classification_command,
    build_corrupt_recovery_command,
    build_extract_command,
    MatrixSummary,
    PostrunConfig,
    ProcessRecord,
    REPAIR_CONCURRENCY,
    REPAIR_INCOMPLETE_PASSES,
    REPAIR_MAX_ATTEMPTS,
    REPAIR_QUEUE_SIZE,
    REPAIR_TIMEOUT_SECONDS,
    build_repair_command,
    classify_orchestration_outcome,
    decide_repair,
    eligible_terminal_extraction_gap,
    extraction_gap,
    find_manifest_writers,
    missing_benchmark_artifacts,
    post_repair_matrix_status,
    process_targets_follower,
    process_tree,
    publish_download_manifest_snapshot,
    redact_command,
    retire_followers_at_boundary,
    summarize_download_matrix,
    validate_availability_classification_result,
    validate_corrupt_recovery_artifact,
    validate_post_extract_boundary,
    validated_subprocess_outcome,
    verify_canonical_manifest_unchanged,
)


class SpanPostrunOrchestratorTests(unittest.TestCase):
    def test_windows_process_inventory_retries_transient_cim_failure(self) -> None:
        failed = subprocess.CalledProcessError(1, ["powershell.exe"])
        recovered = subprocess.CompletedProcess(
            ["powershell.exe"],
            0,
            stdout=json.dumps(
                {
                    "ProcessId": 123,
                    "ParentProcessId": 10,
                    "Name": "python.exe",
                    "CommandLine": "python -m nifty_span.cli",
                    "CreationDate": "creation",
                }
            ),
            stderr="",
        )

        with (
            patch.object(
                subject.subprocess, "run", side_effect=[failed, recovered]
            ) as run,
            patch.object(subject.time, "sleep") as sleep,
        ):
            records = subject.list_windows_processes()

        self.assertEqual(
            records,
            (
                ProcessRecord(
                    123, 10, "python.exe", "python -m nifty_span.cli", "creation"
                ),
            ),
        )
        self.assertEqual(run.call_count, 2)
        self.assertEqual(
            run.call_args.kwargs["timeout"], subject.PROCESS_SNAPSHOT_TIMEOUT_SECONDS
        )
        sleep.assert_called_once_with(subject.PROCESS_SNAPSHOT_RETRY_SECONDS)

    def test_windows_process_inventory_fails_closed_after_bounded_retries(self) -> None:
        failure = subprocess.CalledProcessError(1, ["powershell.exe"])
        with (
            patch.object(subject.subprocess, "run", side_effect=failure) as run,
            patch.object(subject.time, "sleep") as sleep,
        ):
            with self.assertRaisesRegex(RuntimeError, "failed after 5 attempts"):
                subject.list_windows_processes()

        self.assertEqual(run.call_count, subject.PROCESS_SNAPSHOT_MAX_ATTEMPTS)
        self.assertEqual(sleep.call_count, subject.PROCESS_SNAPSHOT_MAX_ATTEMPTS - 1)

    def test_full_12132_latest_cell_matrix_skips_repair(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            manifest = Path(temp) / "download.jsonl"
            events = []
            day = date(2021, 1, 1)
            while day <= date(2026, 7, 15):
                for slot in ("BOD", "ID1", "ID2", "ID3", "ID4", "EOD"):
                    events.append(
                        {
                            "trading_date": day.isoformat(),
                            "slot": slot,
                            "state": "downloaded",
                            "terminal": True,
                        }
                    )
                day += timedelta(days=1)
            _write_jsonl(manifest, events)

            matrix = summarize_download_matrix(manifest)

        self.assertEqual(matrix.expected_cells, 12_132)
        self.assertEqual(matrix.accounted_cells, 12_132)
        self.assertTrue(matrix.fully_terminal)
        self.assertEqual(
            decide_repair(matrix, (), prior_launch_recorded=False),
            "SKIP_MATRIX_FULLY_TERMINAL",
        )

    def test_nonterminal_matrix_refuses_writer_and_prior_launch_before_launch(
        self,
    ) -> None:
        matrix = _matrix(terminal=12_131)
        writer = ProcessRecord(10, 1, "python.exe", "span-backfill download")
        self.assertEqual(
            decide_repair(matrix, (writer,), prior_launch_recorded=False),
            "REFUSE_ACTIVE_MANIFEST_WRITER",
        )
        self.assertEqual(
            decide_repair(matrix, (), prior_launch_recorded=True),
            "REFUSE_PRIOR_REPAIR_LAUNCH",
        )
        self.assertEqual(
            decide_repair(matrix, (), prior_launch_recorded=False),
            "LAUNCH_ONE_REPAIR",
        )
        self.assertEqual(
            decide_repair(
                matrix,
                (),
                prior_launch_recorded=False,
                skip_generic_repair=True,
            ),
            "SKIP_GENERIC_REPAIR_EXPLICIT_REARM",
        )
        self.assertEqual(
            decide_repair(
                matrix,
                (writer,),
                prior_launch_recorded=False,
                skip_generic_repair=True,
            ),
            "REFUSE_ACTIVE_MANIFEST_WRITER",
        )
        self.assertEqual(
            decide_repair(
                _matrix(terminal=12_132), (writer,), prior_launch_recorded=False
            ),
            "REFUSE_ACTIVE_MANIFEST_WRITER",
        )

    def test_writer_detection_requires_same_resolved_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            repo = Path(temp) / "repo"
            target = repo / "run" / "manifests" / "download.jsonl"
            other = repo / "other" / "download.jsonl"
            target.parent.mkdir(parents=True)
            other.parent.mkdir(parents=True)
            target.touch()
            other.touch()
            processes = (
                ProcessRecord(
                    11,
                    1,
                    "python.exe",
                    f'python -m nifty_span.cli span-backfill download --download-manifest "{target}"',
                ),
                ProcessRecord(
                    12,
                    1,
                    "python.exe",
                    f"python -m nifty_span.cli span-backfill download --download-manifest {other}",
                ),
                ProcessRecord(13, 1, "python.exe", f"python audit.py {target}"),
            )

            writers = find_manifest_writers(processes, target, repo)

        self.assertEqual([item.pid for item in writers], [11])

    def test_follower_detection_requires_script_and_same_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            repo = Path(temp) / "repo"
            manifest = repo / "run" / "manifests" / "download.jsonl"
            manifest.parent.mkdir(parents=True)
            manifest.touch()
            follower = ProcessRecord(
                20,
                1,
                "python.exe",
                (
                    "python scripts/follow_span_completed_months.py "
                    f'--download-manifest "{manifest}"'
                ),
            )
            wrong = ProcessRecord(
                21,
                1,
                "python.exe",
                f"python other.py --download-manifest {manifest}",
            )
            self.assertTrue(process_targets_follower(follower, manifest, repo))
            self.assertFalse(process_targets_follower(wrong, manifest, repo))

    def test_process_tree_keeps_only_root_descendants(self) -> None:
        records = (
            ProcessRecord(10, 1, "python.exe", "root"),
            ProcessRecord(11, 10, "python.exe", "child"),
            ProcessRecord(12, 11, "python.exe", "grandchild"),
            ProcessRecord(20, 1, "python.exe", "unrelated"),
        )
        self.assertEqual([item.pid for item in process_tree(records, 10)], [10, 11, 12])

    def test_opt_in_retirement_revalidates_and_retires_only_locked_follower_tree(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = replace(
                _config(root),
                follower_pids=(200, 201),
                retire_followers_before_full_extract=True,
                follower_retirement_timeout_seconds=2,
            )
            manifest = config.run_root / "manifests" / "download.jsonl"
            launcher = _follower(200, 1, manifest, creation="launch-time")
            worker = _follower(201, 200, manifest, creation="worker-time")
            console = _system_conhost(202, 200, creation="console-time")
            inventories = iter(
                ((launcher, worker, console), (launcher, worker, console), ())
            )
            state = {"locked": False}
            terminated: list[int] = []

            @contextmanager
            def fake_lock(*_args: object, **_kwargs: object):
                state["locked"] = True
                try:
                    yield manifest.parent / ".span-extract-compact.lock"
                finally:
                    state["locked"] = False

            def terminate(pid: int, _timeout: float) -> dict[str, object]:
                self.assertTrue(state["locked"])
                terminated.append(pid)
                return {"root_pid": pid, "state": "termination_requested"}

            evidence = retire_followers_at_boundary(
                config,
                manifest,
                {200: launcher, 201: worker},
                root / "retirement.jsonl",
                process_inventory=lambda: next(inventories),
                terminate_tree=terminate,
                lock_factory=fake_lock,
            )

        self.assertEqual(evidence["outcome"], "RETIRED")
        self.assertEqual(evidence["validated_roots"], [200])
        self.assertEqual(evidence["captured_tree_pids"], [200, 201, 202])
        self.assertEqual(
            [item["pid"] for item in evidence["validated_console_descendants"]],
            [202],
        )
        self.assertEqual(terminated, [200])
        self.assertFalse(state["locked"])

    def test_retirement_refuses_non_system_console_descendant(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = replace(
                _config(root),
                retire_followers_before_full_extract=True,
                follower_retirement_timeout_seconds=2,
            )
            manifest = config.run_root / "manifests" / "download.jsonl"
            follower = _follower(200, 1, manifest, creation="follower-time")
            fake_console = ProcessRecord(
                202,
                200,
                "conhost.exe",
                r"C:\Temp\conhost.exe 0x4",
                "console-time",
            )

            @contextmanager
            def fake_lock(*_args: object, **_kwargs: object):
                yield manifest.parent / ".span-extract-compact.lock"

            with self.assertRaisesRegex(
                subject.FollowerRetirementError, "non-explicit descendants"
            ):
                retire_followers_at_boundary(
                    config,
                    manifest,
                    {200: follower},
                    root / "retirement.jsonl",
                    process_inventory=lambda: (follower, fake_console),
                    terminate_tree=lambda pid, _timeout: {"root_pid": pid},
                    lock_factory=fake_lock,
                )

    def test_retirement_revalidates_console_identity_before_termination(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = replace(
                _config(root),
                retire_followers_before_full_extract=True,
                follower_retirement_timeout_seconds=2,
            )
            manifest = config.run_root / "manifests" / "download.jsonl"
            follower = _follower(200, 1, manifest, creation="follower-time")
            console = _system_conhost(202, 200, creation="console-time")
            reused = replace(console, creation_date="reused-time")
            inventories = iter(((follower, console), (follower, reused)))
            terminated: list[int] = []

            @contextmanager
            def fake_lock(*_args: object, **_kwargs: object):
                yield manifest.parent / ".span-extract-compact.lock"

            with self.assertRaisesRegex(
                subject.FollowerRetirementError, "identity changed"
            ):
                retire_followers_at_boundary(
                    config,
                    manifest,
                    {200: follower},
                    root / "retirement.jsonl",
                    process_inventory=lambda: next(inventories),
                    terminate_tree=lambda pid, _timeout: (
                        terminated.append(pid) or {"root_pid": pid}
                    ),
                    lock_factory=fake_lock,
                )
            self.assertEqual(terminated, [])

    def test_retirement_refuses_pid_reuse_and_wrong_command(self) -> None:
        scenarios = (
            (
                "reused",
                lambda original, manifest: replace(
                    original, creation_date="different-time"
                ),
                "was reused",
            ),
            (
                "wrong-command",
                lambda original, manifest: replace(
                    original,
                    command_line=(
                        "python scripts/follow_span_completed_months.py "
                        f'--download-manifest "{manifest}" --once'
                    ),
                ),
                "command identity changed",
            ),
        )
        for label, mutate, expected_error in scenarios:
            with self.subTest(label), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                config = replace(
                    _config(root),
                    retire_followers_before_full_extract=True,
                    follower_retirement_timeout_seconds=2,
                )
                manifest = config.run_root / "manifests" / "download.jsonl"
                original = _follower(200, 1, manifest, creation="original-time")
                current = mutate(original, manifest)
                terminated: list[int] = []

                @contextmanager
                def fake_lock(*_args: object, **_kwargs: object):
                    yield manifest.parent / ".span-extract-compact.lock"

                with self.assertRaisesRegex(RuntimeError, expected_error):
                    retire_followers_at_boundary(
                        config,
                        manifest,
                        {200: original},
                        root / "retirement.jsonl",
                        process_inventory=lambda: (current,),
                        terminate_tree=lambda pid, _timeout: (
                            terminated.append(pid) or {"root_pid": pid}
                        ),
                        lock_factory=fake_lock,
                    )
                self.assertEqual(terminated, [])

    def test_retirement_fails_closed_when_explicit_follower_survives(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = replace(
                _config(root),
                retire_followers_before_full_extract=True,
                follower_retirement_timeout_seconds=2,
            )
            manifest = config.run_root / "manifests" / "download.jsonl"
            follower = _follower(200, 1, manifest, creation="original-time")
            clock = iter((0.0, 0.0, 3.0))

            @contextmanager
            def fake_lock(*_args: object, **_kwargs: object):
                yield manifest.parent / ".span-extract-compact.lock"

            with self.assertRaisesRegex(
                subject.FollowerRetirementError, "retirement timed out"
            ) as raised:
                retire_followers_at_boundary(
                    config,
                    manifest,
                    {200: follower},
                    root / "retirement.jsonl",
                    process_inventory=lambda: (follower,),
                    terminate_tree=lambda pid, _timeout: {"root_pid": pid},
                    lock_factory=fake_lock,
                    clock=lambda: next(clock),
                    sleep=lambda _seconds: None,
                )
            self.assertEqual(raised.exception.evidence["outcome"], "FAIL")
            self.assertEqual(raised.exception.evidence["captured_tree_pids"], [200])

    def test_repair_command_is_pinned_and_uses_checkout_virtualenv(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config = _config(Path(temp))
            command = build_repair_command(config)

        self.assertTrue(command[0].endswith(r".venv\Scripts\python.exe"))
        self.assertEqual(
            _value(command, "--download-concurrency"), str(REPAIR_CONCURRENCY)
        )
        self.assertEqual(_value(command, "--queue-size"), str(REPAIR_QUEUE_SIZE))
        self.assertEqual(_value(command, "--max-attempts"), str(REPAIR_MAX_ATTEMPTS))
        self.assertEqual(
            _value(command, "--retry-incomplete-passes"),
            str(REPAIR_INCOMPLETE_PASSES),
        )
        self.assertEqual(
            _value(command, "--timeout-seconds"), str(int(REPAIR_TIMEOUT_SECONDS))
        )
        self.assertEqual(_value(command, "--start-date"), "2021-01-01")
        self.assertEqual(_value(command, "--end-date"), "2026-07-15")

    def test_extraction_gap_requires_success_for_latest_download_hash(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            download = root / "download.jsonl"
            extraction = root / "extraction.jsonl"
            _write_jsonl(
                download,
                [
                    _download("2025-01-02", "BOD", "a" * 64),
                    _download(
                        "2025-01-02", "ID1", "b" * 64, state="downloaded_existing"
                    ),
                ],
            )
            _write_jsonl(
                extraction,
                [
                    _extraction("2025-01-02", "BOD", "a" * 64, "fragment_created"),
                    _extraction("2025-01-02", "ID1", "b" * 64, "fragment_failed"),
                ],
            )

            gap = extraction_gap(download, extraction)

        self.assertEqual(gap.downloaded_sources, 2)
        self.assertEqual(gap.extracted_sources, 1)
        self.assertEqual(gap.missing_sources, (f"2025-01-02|ID1|{'b' * 64}",))
        self.assertFalse(gap.caught_up)

    def test_nonterminal_corrupt_cell_does_not_deadlock_postrun_sibling_extraction(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            download = root / "download.jsonl"
            extraction = root / "extraction.jsonl"
            events = _complete_month_events(2025, 1)
            sibling_hashes: dict[str, str] = {}
            for slot in ("BOD", "ID1", "ID2", "ID3", "ID4"):
                digest = (slot.lower() * 64)[:64]
                sibling_hashes[slot] = digest
                events.append(_download("2025-01-02", slot, digest))
            events.append(
                {
                    "trading_date": "2025-01-02",
                    "slot": "EOD",
                    "state": "corrupt_inner_zip",
                    "terminal": False,
                }
            )
            _write_jsonl(download, events)
            _write_jsonl(extraction, [])

            follower_gap = eligible_terminal_extraction_gap(
                download, extraction, current_day=date(2025, 2, 1)
            )
            full_gap_before = extraction_gap(download, extraction)
            _write_jsonl(
                extraction,
                [
                    _extraction("2025-01-02", slot, digest, "fragment_created")
                    for slot, digest in sibling_hashes.items()
                ],
            )
            full_gap_after = extraction_gap(download, extraction)

        self.assertTrue(follower_gap.caught_up)
        self.assertEqual(follower_gap.downloaded_sources, 0)
        self.assertEqual(full_gap_before.downloaded_sources, 5)
        self.assertEqual(len(full_gap_before.missing_sources), 5)
        self.assertTrue(full_gap_after.caught_up)
        self.assertEqual(full_gap_after.extracted_sources, 5)

    def test_download_snapshot_is_content_addressed_and_integrity_checked(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            canonical = root / "download.jsonl"
            _write_jsonl(canonical, [_download("2025-01-02", "BOD", "a" * 64)])

            snapshot = publish_download_manifest_snapshot(canonical, root / "snapshots")
            repeated = publish_download_manifest_snapshot(canonical, root / "snapshots")

            self.assertEqual(snapshot, repeated)
            self.assertEqual(Path(snapshot.snapshot_path).stem, snapshot.sha256)
            self.assertEqual(
                Path(snapshot.snapshot_path).read_bytes(), canonical.read_bytes()
            )
            Path(snapshot.snapshot_path).write_bytes(b"tampered\n")
            with self.assertRaisesRegex(RuntimeError, "snapshot failed integrity"):
                publish_download_manifest_snapshot(canonical, root / "snapshots")

    def test_frozen_boundary_refuses_active_canonical_writer(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            canonical = root / "download.jsonl"
            _write_jsonl(canonical, [_download("2025-01-02", "BOD", "a" * 64)])
            snapshot = publish_download_manifest_snapshot(canonical, root / "snapshots")
            writer = ProcessRecord(
                77,
                1,
                "python.exe",
                (
                    "python -m nifty_span.cli span-backfill download "
                    f'--download-manifest "{canonical}"'
                ),
            )

            with self.assertRaisesRegex(RuntimeError, "writer active"):
                verify_canonical_manifest_unchanged(snapshot, (writer,), root)

    def test_post_extract_boundary_fails_on_subprocess_error_or_manifest_mutation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            canonical = root / "download.jsonl"
            extraction = root / "extraction.jsonl"
            event = _download("2025-01-02", "BOD", "a" * 64)
            _write_jsonl(canonical, [event])
            _write_jsonl(
                extraction,
                [_extraction("2025-01-02", "BOD", "a" * 64, "fragment_created")],
            )
            snapshot = publish_download_manifest_snapshot(canonical, root / "snapshots")

            with self.assertRaisesRegex(RuntimeError, "exited 9"):
                validate_post_extract_boundary(
                    snapshot,
                    extraction,
                    extract_exit_code=9,
                    processes=(),
                    repo_root=root,
                )

            with canonical.open("a", encoding="utf-8") as stream:
                stream.write(
                    json.dumps({**event, "state": "downloaded_existing"}) + "\n"
                )
            with self.assertRaisesRegex(
                RuntimeError, "canonical download journal changed"
            ):
                validate_post_extract_boundary(
                    snapshot,
                    extraction,
                    extract_exit_code=0,
                    processes=(),
                    repo_root=root,
                )

    def test_post_extract_boundary_requires_zero_full_snapshot_gap(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            canonical = root / "download.jsonl"
            extraction = root / "extraction.jsonl"
            _write_jsonl(canonical, [_download("2025-01-02", "BOD", "a" * 64)])
            _write_jsonl(extraction, [])
            snapshot = publish_download_manifest_snapshot(canonical, root / "snapshots")

            with self.assertRaisesRegex(
                RuntimeError, "gap remains for 1 source hashes"
            ):
                validate_post_extract_boundary(
                    snapshot,
                    extraction,
                    extract_exit_code=0,
                    processes=(),
                    repo_root=root,
                )

    def test_full_extract_command_uses_frozen_snapshot_and_pinned_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config = _config(Path(temp))
            canonical = config.run_root / "manifests" / "download.jsonl"
            _write_jsonl(canonical, [_download("2025-01-02", "BOD", "a" * 64)])
            snapshot = publish_download_manifest_snapshot(
                canonical,
                config.run_root / "reports" / "postrun" / "download_snapshots",
            )

            command = build_extract_command(config, snapshot)

        self.assertEqual(_value(command, "--download-manifest"), snapshot.snapshot_path)
        self.assertEqual(_value(command, "--start-date"), "2021-01-01")
        self.assertEqual(_value(command, "--end-date"), "2026-07-15")
        self.assertEqual(
            _value(command, "--extraction-manifest"),
            str(config.run_root / "manifests" / "extraction.jsonl"),
        )

    def test_corrupt_recovery_command_is_pinned_safe_and_follower_catchup_is_first(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config = _config(Path(temp))
            command = build_corrupt_recovery_command(config)

            self.assertEqual(command[4:6], ("span-backfill", "recover-corrupt"))
            self.assertEqual(_value(command, "--start-date"), "2021-01-01")
            self.assertEqual(_value(command, "--end-date"), "2026-07-15")
            self.assertEqual(_value(command, "--corrupt-timeout-seconds"), "600")
            self.assertEqual(_value(command, "--corrupt-max-attempts"), "3")
            self.assertEqual(
                _value(command, "--availability-manifest"),
                str(config.availability_manifest.resolve()),
            )
            self.assertNotIn("--download-concurrency", command)

            order: list[str] = []

            def fake_run(
                argv: tuple[str, ...],
                *,
                cwd: Path,
                stdout_path: Path,
                stderr_path: Path,
                journal: Path,
                event_prefix: str,
            ) -> dict[str, object]:
                self.assertEqual(event_prefix, "corrupt_recovery")
                order.append(event_prefix)
                report_root = Path(_value(tuple(argv), "--report-root"))
                report_root.mkdir(parents=True, exist_ok=True)
                markdown = report_root / "SPAN_CORRUPT_RECOVERY_test.md"
                markdown.write_text("# proof\n", encoding="utf-8")
                report = report_root / "span_corrupt_recovery_test.json"
                report.write_text(
                    json.dumps(
                        {
                            "schema_version": "span-corrupt-static-recovery/v1",
                            "run_id": "test",
                            "ok": True,
                            "selected_cells": 0,
                            "recovered_cells": 0,
                            "classified_source_corrupt_cells": 0,
                            "already_classified_cells": 0,
                            "unresolved_cells": 0,
                            "unresolved_corrupt_cells": 0,
                            "unresolved_missing_cells": 0,
                            "json_report": str(report.resolve()),
                            "markdown_report": str(markdown.resolve()),
                        }
                    )
                    + "\n",
                    encoding="utf-8",
                )
                return {
                    "exit_code": 0,
                    "stdout": str(stdout_path),
                    "stderr": str(stderr_path),
                }

            def fake_catchup(*_args: object, **_kwargs: object) -> dict[str, object]:
                order.append("follower_catchup")
                return {"complete": False, "reason": "bounded-test-stop"}

            with (
                patch.object(subject, "_checkout_evidence", return_value={}),
                patch.object(subject, "_publish_status"),
                patch.object(subject, "_validate_and_wait_exact_downloaders"),
                patch.object(subject, "list_windows_processes", return_value=()),
                patch.object(
                    subject,
                    "summarize_download_matrix",
                    return_value=_matrix(terminal=12_132),
                ),
                patch.object(subject, "_run_logged_process", side_effect=fake_run),
                patch.object(
                    subject, "_wait_for_follower_catchup", side_effect=fake_catchup
                ),
                patch.object(
                    subject, "publish_download_manifest_snapshot"
                ) as frozen_snapshot,
            ):
                result = subject.run_postrun(config)

            self.assertEqual(result["outcome"], "WAITING")
            self.assertEqual(order, ["follower_catchup"])
            frozen_snapshot.assert_not_called()
            self.assertNotIn("corrupt_recovery", result)

    def test_explicit_rearm_skip_is_durable_and_bypasses_generic_repair(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = replace(_config(root), skip_generic_repair=True)
            incomplete = _matrix(terminal=10_414)

            with (
                patch.object(subject, "_checkout_evidence", return_value={}),
                patch.object(subject, "_publish_status"),
                patch.object(
                    subject, "_validate_and_wait_exact_downloaders", return_value={}
                ),
                patch.object(subject, "list_windows_processes", return_value=()),
                patch.object(
                    subject, "summarize_download_matrix", return_value=incomplete
                ),
                patch.object(subject, "_run_logged_process") as run_process,
                patch.object(
                    subject,
                    "_wait_for_follower_catchup",
                    return_value={"complete": False, "reason": "bounded-test-stop"},
                ),
            ):
                result = subject.run_postrun(config)

            journal = (
                config.run_root
                / "reports"
                / "postrun"
                / f"{config.log_prefix}.events.jsonl"
            )
            events = [json.loads(line) for line in journal.read_text().splitlines()]
            repair_decision = next(
                event for event in events if event["event"] == "repair_decision"
            )

        self.assertEqual(
            result["repair_decision"], "SKIP_GENERIC_REPAIR_EXPLICIT_REARM"
        )
        self.assertEqual(result["outcome"], "WAITING")
        self.assertTrue(result["config"]["skip_generic_repair"])
        self.assertTrue(repair_decision["explicit_skip_generic_repair"])
        self.assertEqual(repair_decision["wait_for_pids"], [100])
        self.assertEqual(repair_decision["matrix_terminal_cells"], 10_414)
        self.assertEqual(repair_decision["matrix_nonterminal_cells"], 1_718)
        run_process.assert_not_called()

    def test_no_follower_rearm_is_explicit_and_continues_to_classification(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = replace(
                _config(root),
                follower_pids=(),
                skip_generic_repair=True,
                skip_follower_catchup=True,
                retire_followers_before_full_extract=False,
            )
            incomplete = _matrix(terminal=10_414)

            with (
                patch.object(subject, "_checkout_evidence", return_value={}),
                patch.object(subject, "_publish_status"),
                patch.object(
                    subject, "_validate_and_wait_exact_downloaders", return_value={}
                ),
                patch.object(subject, "list_windows_processes", return_value=()),
                patch.object(
                    subject, "summarize_download_matrix", return_value=incomplete
                ),
                patch.object(subject, "_wait_for_follower_catchup") as catchup,
                patch.object(
                    subject,
                    "_run_logged_process",
                    side_effect=RuntimeError("stop-at-classification"),
                ) as run_process,
            ):
                result = subject.run_postrun(config)

            journal = (
                config.run_root
                / "reports"
                / "postrun"
                / f"{config.log_prefix}.events.jsonl"
            )
            events = [json.loads(line) for line in journal.read_text().splitlines()]
            skipped = next(
                event
                for event in events
                if event["event"] == "follower_catchup_skipped"
            )

        catchup.assert_not_called()
        self.assertEqual(
            run_process.call_args.kwargs["event_prefix"], "availability_classification"
        )
        self.assertTrue(result["config"]["skip_follower_catchup"])
        self.assertEqual(
            result["eligible_follower_catchup"]["outcome"],
            "SKIPPED_EXPLICIT_NO_FOLLOWER",
        )
        self.assertTrue(
            result["eligible_follower_catchup"]["full_range_extraction_required"]
        )
        self.assertEqual(
            result["follower_retirement"]["outcome"],
            "SKIPPED_EXPLICIT_NO_FOLLOWER",
        )
        self.assertTrue(skipped["explicit_skip_follower_catchup"])
        self.assertTrue(skipped["full_range_extraction_required"])

    def test_no_follower_rearm_rejects_contradictory_configuration(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config = _config(Path(temp))
            with self.assertRaisesRegex(
                ValueError, "requires follower_pids to be empty"
            ):
                replace(config, skip_follower_catchup=True).validated()
            with self.assertRaisesRegex(
                ValueError, "cannot request follower retirement"
            ):
                replace(
                    config,
                    follower_pids=(),
                    skip_follower_catchup=True,
                    retire_followers_before_full_extract=True,
                ).validated()
            with self.assertRaisesRegex(ValueError, "at least one positive PID"):
                replace(config, follower_pids=()).validated()

    def test_postrun_cli_forwards_explicit_skip_generic_repair(self) -> None:
        script = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "orchestrate_span_phase1_postrun.py"
        )
        spec = importlib.util.spec_from_file_location("postrun_cli_test", script)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        root = Path("test-root")
        with patch.object(
            module, "run_postrun", return_value={"outcome": "WAITING"}
        ) as run_postrun:
            exit_code = module.main(
                [
                    "--run-root",
                    str(root / "run"),
                    "--wait-for-pid",
                    "100",
                    "--log-prefix",
                    "postrun-rearm-test",
                    "--availability-manifest",
                    str(root / "availability.jsonl"),
                    "--availability-import",
                    str(root / "availability-import.json"),
                    "--provenance-root",
                    str(root / "provenance"),
                    "--benchmark-artifact",
                    str(root / "benchmark.json"),
                    "--skip-generic-repair",
                    "--skip-follower-catchup",
                ]
            )

        forwarded = run_postrun.call_args.args[0]
        self.assertEqual(exit_code, 2)
        self.assertTrue(forwarded.skip_generic_repair)
        self.assertTrue(forwarded.skip_follower_catchup)
        self.assertEqual(forwarded.follower_pids, ())

    def test_availability_classification_command_and_result_are_exact_range(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config = _config(Path(temp)).validated()
            command = build_availability_classification_command(config)
            self.assertEqual(command[4:6], ("span-backfill", "classify"))
            self.assertEqual(_value(command, "--start-date"), "2021-01-01")
            self.assertEqual(_value(command, "--end-date"), "2026-07-15")
            self.assertEqual(
                _value(command, "--availability-import"),
                str(config.availability_import),
            )
            self.assertEqual(
                _value(command, "--provenance-root"), str(config.provenance_root)
            )
            stdout = config.run_root / "classification.json"
            stdout.write_text(
                json.dumps(
                    {
                        "start_date": "2021-01-01",
                        "end_date": "2026-07-15",
                        "imported_dates": 2_022,
                        "classified_missing_cells": 0,
                        "unresolved_missing_cells": 1,
                        "source_boundary_cells": 0,
                        "retained_sources": 1,
                        "availability_manifest": str(config.availability_manifest),
                        "provenance_root": str(config.provenance_root),
                    }
                ),
                encoding="utf-8",
            )
            evidence = validate_availability_classification_result(
                config, stdout_path=stdout, exit_code=1
            )
            self.assertEqual(evidence["unresolved_missing_cells"], 1)
            self.assertEqual(evidence["latest_validated_events"], 0)

    def test_corrupt_recovery_artifact_exit_contract_is_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config = _config(Path(temp)).validated()
            (config.run_root / "raw").mkdir()
            root = config.run_root / "reports" / "corrupt_recovery"
            snapshots = root / "manifest_snapshots"
            snapshots.mkdir(parents=True)
            source = config.run_root / "manifests" / "download.jsonl"
            snapshot = snapshots / (hashlib.sha256(b"").hexdigest() + ".jsonl")
            snapshot.write_bytes(b"")
            markdown = root / "SPAN_CORRUPT_RECOVERY_run.md"
            markdown.write_text(
                "# evidence\n\n"
                "No alternative-source decision is required by this recovery run.\n",
                encoding="utf-8",
            )
            report = root / "span_corrupt_recovery_run.json"
            payload = {
                "schema_version": "span-corrupt-static-recovery/v1",
                "run_id": "run",
                "start_date": "2021-01-01",
                "end_date": "2026-07-15",
                "raw_root": str((config.run_root / "raw").resolve()),
                "source_manifest": str(source.resolve()),
                "availability_manifest": str(config.availability_manifest),
                "source_snapshot": str(snapshot.resolve()),
                "source_snapshot_sha256": hashlib.sha256(b"").hexdigest(),
                "source_snapshot_size_bytes": 0,
                "source_snapshot_events": 0,
                "ok": True,
                "selected_cells": 0,
                "network_calls": 0,
                "recovered_cells": 0,
                "classified_source_corrupt_cells": 0,
                "already_classified_cells": 0,
                "unresolved_cells": 0,
                "unresolved_corrupt_cells": 0,
                "unresolved_missing_cells": 0,
                "json_report": str(report.resolve()),
                "markdown_report": str(markdown.resolve()),
                "cells": [],
            }
            report.write_text(json.dumps(payload) + "\n", encoding="utf-8")
            evidence = validate_corrupt_recovery_artifact(
                config, root, files_before=(), exit_code=0
            )
            self.assertTrue(evidence["ok"])
            self.assertEqual(evidence["classified_source_corrupt_cells"], 0)
            self.assertTrue(evidence["artifact_sha256"])
            with self.assertRaisesRegex(RuntimeError, "exit and report disagree"):
                validate_corrupt_recovery_artifact(
                    config, root, files_before=(), exit_code=1
                )
            payload["ok"] = False
            payload["unresolved_cells"] = 1
            payload["unresolved_corrupt_cells"] = 1
            report.write_text(json.dumps(payload) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "disagrees"):
                validate_corrupt_recovery_artifact(
                    config, root, files_before=(), exit_code=1
                )

    def test_outcome_never_promotes_blocked_waiting_or_failed_evidence(self) -> None:
        common = {
            "matrix_full": True,
            "catchup_complete": True,
            "pilot_status": "PASS",
            "blocked_matrix_ready": True,
        }
        self.assertEqual(
            classify_orchestration_outcome(
                **common, finalizer_outcome="BLOCKED_SOURCE"
            ),
            "BLOCKED_SOURCE",
        )
        self.assertEqual(
            classify_orchestration_outcome(
                matrix_full=True,
                catchup_complete=True,
                pilot_status="WAITING",
                finalizer_outcome="FAIL_INCOMPLETE",
            ),
            "WAITING",
        )
        self.assertEqual(
            classify_orchestration_outcome(
                **common, finalizer_outcome="FAIL_INCOMPLETE"
            ),
            "FAIL",
        )
        self.assertEqual(
            classify_orchestration_outcome(**common, finalizer_outcome="PASS_READY"),
            "PASS_READY",
        )
        self.assertEqual(
            classify_orchestration_outcome(
                matrix_full=False,
                catchup_complete=True,
                pilot_status="WAITING",
                finalizer_outcome="BLOCKED_SOURCE",
                blocked_matrix_ready=False,
            ),
            "FAIL",
        )
        self.assertEqual(
            post_repair_matrix_status(_matrix(terminal=12_131)),
            "STABLE_INCOMPLETE_CONTINUE_TO_FINALIZER",
        )

    def test_missing_declared_benchmark_is_waitable_not_startup_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config = _config(Path(temp))
            config.benchmark_artifacts[0].unlink()
            validated = config.validated()
            self.assertEqual(
                missing_benchmark_artifacts(validated),
                (str(config.benchmark_artifacts[0].resolve()),),
            )
        self.assertEqual(
            apply_benchmark_wait("BLOCKED_SOURCE", evidence_complete=False),
            "BLOCKED_SOURCE",
        )
        self.assertEqual(
            apply_benchmark_wait("FAIL", evidence_complete=False), "WAITING"
        )

    def test_command_reporting_redacts_future_secret_flags(self) -> None:
        self.assertEqual(
            redact_command(
                ("python", "job.py", "--access-token", "secret", "--token=x")
            ),
            ["python", "job.py", "--access-token", "<redacted>", "--token=<redacted>"],
        )

    def test_fresh_artifact_outcome_must_match_subprocess_exit_contract(self) -> None:
        self.assertEqual(validated_subprocess_outcome("pilot", "WAITING", 2), "WAITING")
        self.assertEqual(
            validated_subprocess_outcome("finalizer", "BLOCKED_SOURCE", 1),
            "BLOCKED_SOURCE",
        )
        self.assertIsNone(validated_subprocess_outcome("finalizer", "PASS_READY", 1))
        self.assertIsNone(validated_subprocess_outcome("pilot", "PASS", 2))


def _config(temp: Path) -> PostrunConfig:
    repo = temp / "repo"
    run = temp / "run"
    python = repo / ".venv" / "Scripts" / "python.exe"
    python.parent.mkdir(parents=True)
    python.touch()
    manifest = run / "manifests" / "download.jsonl"
    manifest.parent.mkdir(parents=True)
    manifest.write_text("", encoding="utf-8")
    availability = run / "manifests" / "availability.jsonl"
    availability.touch()
    availability_import = repo / "availability-import.json"
    availability_import.write_text("{}", encoding="utf-8")
    provenance_root = run / "availability-sources"
    provenance_root.mkdir(parents=True)
    benchmark = repo / "benchmark.json"
    benchmark.write_text("{}", encoding="utf-8")
    return PostrunConfig(
        repo_root=repo,
        run_root=run,
        wait_for_pids=(100,),
        follower_pids=(200,),
        log_prefix="postrun.test",
        availability_manifest=availability,
        availability_import=availability_import,
        provenance_root=provenance_root,
        benchmark_artifacts=(benchmark,),
        quiescence_seconds=2,
        poll_seconds=1,
    )


def _matrix(*, terminal: int) -> MatrixSummary:
    return MatrixSummary(
        expected_cells=12_132,
        accounted_cells=12_132,
        terminal_cells=terminal,
        nonterminal_cells=12_132 - terminal,
        out_of_range_cells=0,
        latest_state_counts={"downloaded": terminal},
        source_event_count=12_132,
        source_prefix_sha256="a" * 64,
        ignored_trailing_bytes=0,
    )


def _follower(
    pid: int, parent_pid: int, manifest: Path, *, creation: str
) -> ProcessRecord:
    return ProcessRecord(
        pid,
        parent_pid,
        "python.exe",
        (
            "python scripts/follow_span_completed_months.py "
            f'--download-manifest "{manifest}"'
        ),
        creation,
    )


def _system_conhost(pid: int, parent_pid: int, *, creation: str) -> ProcessRecord:
    executable = (
        Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32" / "conhost.exe"
    )
    return ProcessRecord(
        pid,
        parent_pid,
        "conhost.exe",
        rf"\??\{executable} 0x4",
        creation,
    )


def _complete_month_events(year: int, month: int) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    day = date(year, month, 1)
    slots = ("BOD", "ID1", "ID2", "ID3", "ID4", "EOD")
    while day.month == month:
        for slot in slots:
            events.append(
                {
                    "trading_date": day.isoformat(),
                    "slot": slot,
                    "state": "not_returned_http_404",
                    "terminal": True,
                }
            )
        day += timedelta(days=1)
    return events


def _value(command: tuple[str, ...], flag: str) -> str:
    return command[command.index(flag) + 1]


def _download(
    day: str, slot: str, digest: str, *, state: str = "downloaded"
) -> dict[str, object]:
    return {
        "trading_date": day,
        "slot": slot,
        "state": state,
        "terminal": True,
        "sha256": digest,
    }


def _extraction(day: str, slot: str, digest: str, event: str) -> dict[str, object]:
    return {"date": day, "slot": slot, "source_sha256": digest, "event": event}


def _write_jsonl(path: Path, events: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(
            json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n"
            for event in events
        ),
        encoding="utf-8",
    )


if __name__ == "__main__":
    unittest.main()
