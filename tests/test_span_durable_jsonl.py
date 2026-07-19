from __future__ import annotations

import errno
import json
from pathlib import Path
import tempfile
import unittest

from nifty_span.span.backfill_downloader import _Manifest
from nifty_span.span.durable_jsonl import (
    JsonlAppendRetryPolicy,
    append_jsonl_record,
)


class DurableJsonlTests(unittest.TestCase):
    def test_open_sharing_failures_retry_then_append_exactly_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "manifest.jsonl"
            attempts = 0
            delays: list[float] = []

            def open_append(target: Path):
                nonlocal attempts
                attempts += 1
                if attempts < 3:
                    raise PermissionError(errno.EACCES, "external reader lock", target)
                return target.open("a", encoding="utf-8", newline="\n")

            append_jsonl_record(
                path,
                {"event_id": "one", "value": 1},
                policy=_policy(max_attempts=4),
                sleep=delays.append,
                random_value=lambda: 0.5,
                open_append=open_append,
            )

            self.assertEqual(attempts, 3)
            self.assertEqual(delays, [0.01, 0.02])
            lines = path.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 1)
            self.assertEqual(json.loads(lines[0]), {"event_id": "one", "value": 1})

    def test_open_sharing_failure_exhaustion_is_bounded_and_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "manifest.jsonl"
            attempts = 0
            delays: list[float] = []

            def locked(target: Path):
                nonlocal attempts
                attempts += 1
                raise PermissionError(errno.EACCES, "still locked", target)

            with self.assertRaises(PermissionError):
                append_jsonl_record(
                    path,
                    {"event_id": "never-written"},
                    sleep=delays.append,
                    random_value=lambda: 0.5,
                    open_append=locked,
                )

            self.assertEqual(attempts, 12)
            self.assertEqual(
                delays,
                [0.05, 0.1, 0.2, 0.4, 0.8, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            )
            self.assertAlmostEqual(sum(delays), 7.55)
            self.assertFalse(path.exists())

    def test_non_permission_oserror_is_not_retried(self) -> None:
        attempts = 0
        delays: list[float] = []

        def disk_full(_target: Path):
            nonlocal attempts
            attempts += 1
            raise OSError(errno.ENOSPC, "disk full")

        with self.assertRaises(OSError) as captured:
            append_jsonl_record(
                Path("unused.jsonl"),
                {"event_id": "not-written"},
                policy=_policy(max_attempts=8),
                sleep=delays.append,
                open_append=disk_full,
            )

        self.assertEqual(captured.exception.errno, errno.ENOSPC)
        self.assertEqual(attempts, 1)
        self.assertEqual(delays, [])

    def test_permission_failure_after_handle_acquisition_is_not_retried(self) -> None:
        attempts = 0
        delays: list[float] = []
        handle = _FailingFlushHandle()

        def acquired(_target: Path):
            nonlocal attempts
            attempts += 1
            return handle

        with self.assertRaises(PermissionError):
            append_jsonl_record(
                Path("unused.jsonl"),
                {"event_id": "ambiguous-durability"},
                policy=_policy(max_attempts=8),
                sleep=delays.append,
                open_append=acquired,
            )

        self.assertEqual(attempts, 1)
        self.assertEqual(handle.write_calls, 1)
        self.assertEqual(handle.flush_calls, 1)
        self.assertEqual(delays, [])


class DownloaderManifestRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_downloader_manifest_retries_open_without_duplicate_event(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "download.jsonl"
            manifest = _Manifest(path, "retry-test")
            attempts = 0

            def open_append(target: Path):
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise PermissionError(errno.EACCES, "reader sharing lock", target)
                return target.open("a", encoding="utf-8", newline="\n")

            from unittest.mock import patch

            with patch(
                "nifty_span.span.durable_jsonl._open_append",
                side_effect=open_append,
            ):
                record = await manifest.append(
                    {
                        "trading_date": "2021-01-05",
                        "slot": "BOD",
                        "suffix": "i1",
                        "state": "downloaded",
                        "terminal": True,
                    }
                )

            events = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual(attempts, 2)
            self.assertEqual(events, [record])
            self.assertEqual(
                manifest.latest[("2021-01-05", "BOD")]["event_id"],
                record["event_id"],
            )


def _policy(*, max_attempts: int) -> JsonlAppendRetryPolicy:
    return JsonlAppendRetryPolicy(
        max_attempts=max_attempts,
        initial_delay_seconds=0.01,
        max_delay_seconds=1.0,
        jitter_ratio=0.0,
    )


class _FailingFlushHandle:
    def __init__(self) -> None:
        self.write_calls = 0
        self.flush_calls = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def write(self, _payload: str) -> int:
        self.write_calls += 1
        return 1

    def flush(self) -> None:
        self.flush_calls += 1
        raise PermissionError(errno.EACCES, "flush state is ambiguous")


if __name__ == "__main__":
    unittest.main()
