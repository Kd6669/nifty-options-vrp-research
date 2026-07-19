from __future__ import annotations

from contextlib import redirect_stdout
from dataclasses import dataclass, replace
from datetime import date
from concurrent.futures import ThreadPoolExecutor
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase, mock
import asyncio
import io
import json
import stat
import warnings
import zipfile
import threading

from nifty_span.span import backfill_downloader as subject
from nifty_span.cli import span_backfill_main


DAY = date(2025, 1, 2)


@dataclass
class _Response:
    status_code: int
    content: bytes = b""
    headers: dict[str, str] | None = None


class _Controller:
    def __init__(self, actions: list[object], *, delay: float = 0.0) -> None:
        self.actions = list(actions)
        self.delay = delay
        self.api_calls: list[dict[str, object]] = []
        self.warm_calls = 0
        self.active = 0
        self.max_active = 0

    async def get(self, url: str, **kwargs: object) -> _Response:
        if url == subject.HOME_URL:
            self.warm_calls += 1
            return _Response(200)
        self.api_calls.append(kwargs)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
            if not self.actions:
                raise AssertionError("unexpected API request")
            action = self.actions.pop(0)
            if isinstance(action, BaseException):
                raise action
            if callable(action):
                action = action(kwargs)
            assert isinstance(action, _Response)
            return action
        finally:
            self.active -= 1


class _Client:
    def __init__(self, controller: _Controller) -> None:
        self.controller = controller
        self.closed = False

    async def get(self, url: str, **kwargs: object) -> _Response:
        return await self.controller.get(url, **kwargs)

    async def close(self) -> None:
        self.closed = True


def _factory(controller: _Controller):
    return lambda _worker_id: _Client(controller)


def _config(**changes: object) -> subject.BackfillConfig:
    values: dict[str, object] = {
        "max_concurrent": 1,
        "max_attempts": 4,
        "retry_incomplete_passes": 0,
        "backoff_base_seconds": 0,
        "backoff_max_seconds": 0,
        "jitter_seconds": 0,
    }
    values.update(changes)
    return subject.BackfillConfig(**values)


def _run(
    root: Path,
    controller: _Controller,
    *,
    start: date = DAY,
    end: date = DAY,
    config: subject.BackfillConfig | None = None,
    sleeps: list[float] | None = None,
) -> subject.SpanBackfillReport:
    async def record_sleep(delay: float) -> None:
        if sleeps is not None:
            sleeps.append(delay)

    return subject.download_span_backfill(
        start_date=start,
        end_date=end,
        output_root=root,
        config=config or _config(),
        client_factory=_factory(controller),
        sleep=record_sleep,
        random_fn=lambda: 0,
    )


def _inner_zip(
    day: date,
    suffix: str,
    *,
    payload: bytes = b"<spanFile/>",
    name: str | None = None,
    missing_spn: bool = False,
    extra_member: bool = False,
) -> bytes:
    stream = io.BytesIO()
    inner_suffix = f"i0{suffix[1]}" if suffix.startswith("i") else "s"
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        if missing_spn:
            archive.writestr("readme.txt", b"not span")
        else:
            archive.writestr(
                name or f"nsccl.{day:%Y%m%d}.{inner_suffix}.spn",
                payload,
            )
        if extra_member:
            archive.writestr("extra.txt", b"unexpected")
    return stream.getvalue()


def _outer_zip(
    day: date,
    suffixes: tuple[str, ...] = ("i1", "i2", "i3", "i4", "i5", "s"),
    *,
    inner_overrides: dict[str, bytes] | None = None,
    outer_names: dict[str, str] | None = None,
    duplicate_suffix: str | None = None,
) -> bytes:
    stream = io.BytesIO()
    overrides = inner_overrides or {}
    names = outer_names or {}
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_STORED) as archive:
            for suffix in suffixes:
                name = names.get(suffix, f"nsccl.{day:%Y%m%d}.{suffix}.zip")
                archive.writestr(name, overrides.get(suffix, _inner_zip(day, suffix)))
            if duplicate_suffix is not None:
                archive.writestr(
                    f"nsccl.{day:%Y%m%d}.{duplicate_suffix}.zip",
                    _inner_zip(day, duplicate_suffix),
                )
    return stream.getvalue()


def _zip_response(content: bytes) -> _Response:
    return _Response(200, content, {"Content-Type": "application/zip"})


def _mark_first_member_encrypted(content: bytes) -> bytes:
    mutated = bytearray(content)
    for signature, flag_offset, finder in (
        (b"PK\x03\x04", 6, mutated.find),
        (b"PK\x01\x02", 8, mutated.rfind),
    ):
        position = finder(signature)
        if position < 0:
            raise AssertionError(f"missing ZIP signature {signature!r}")
        offset = position + flag_offset
        flags = int.from_bytes(mutated[offset : offset + 2], "little") | 0x1
        mutated[offset : offset + 2] = flags.to_bytes(2, "little")
    return bytes(mutated)


def _outer_with_special_member(*, symlink: bool = False) -> bytes:
    stream = io.BytesIO()
    info = zipfile.ZipInfo("nsccl.20250102.i1.zip")
    if symlink:
        info.create_system = 3
        info.external_attr = (stat.S_IFLNK | 0o777) << 16
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(info, _inner_zip(DAY, "i1"))
    return stream.getvalue()


def _inner_with_special_member(
    *, symlink: bool = False, duplicate: bool = False
) -> bytes:
    stream = io.BytesIO()
    name = "nsccl.20250102.i01.spn"
    info = zipfile.ZipInfo(name)
    if symlink:
        info.create_system = 3
        info.external_attr = (stat.S_IFLNK | 0o777) << 16
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        with zipfile.ZipFile(stream, "w") as archive:
            archive.writestr(info, b"<spanFile/>")
            if duplicate:
                archive.writestr(name, b"<spanFile/>")
    return stream.getvalue()


def _events(root: Path) -> list[dict[str, object]]:
    path = root / "span_backfill_manifest.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


class SpanBackfillDownloaderTests(TestCase):
    def test_default_concurrency_and_safe_cap(self) -> None:
        self.assertEqual(subject.BackfillConfig().max_concurrent, 4)
        self.assertEqual(subject.BackfillConfig().retry_incomplete_passes, 1)
        self.assertEqual(subject.BackfillConfig().repair_order, "chronological")
        self.assertGreaterEqual(
            subject.DOWNLOAD_MANIFEST_APPEND_RETRY_POLICY.max_attempts,
            60,
        )
        self.assertEqual(
            subject.BackfillConfig(max_concurrent=8).validated().max_concurrent, 8
        )
        with self.assertRaisesRegex(ValueError, "max_concurrent must be <= 8"):
            subject.BackfillConfig(max_concurrent=9).validated()
        with self.assertRaisesRegex(ValueError, "repair_order must be one of"):
            subject.BackfillConfig(repair_order="invalid").validated()
        self.assertEqual(
            subject.BackfillConfig(
                max_concurrent=9,
                unsafe_allow_high_concurrency=True,
            )
            .validated()
            .max_concurrent,
            9,
        )
        with self.assertRaisesRegex(ValueError, "retry_incomplete_passes must be >= 0"):
            subject.BackfillConfig(retry_incomplete_passes=-1).validated()

    def test_six_slot_download_and_mapping(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            controller = _Controller([_zip_response(_outer_zip(DAY))])
            report = _run(root, controller)

            self.assertEqual(report.total_cells, 6)
            self.assertEqual(report.completed_dates, 1)
            self.assertEqual(report.downloaded_slots, 6)
            self.assertEqual(
                [(cell.slot, cell.suffix) for cell in report.cells],
                list(subject.SLOT_SPECS),
            )
            for slot, suffix in subject.SLOT_SPECS:
                path = root / f"2025/01/02/nsccl.20250102.{suffix}.zip"
                self.assertTrue(path.is_file(), slot)
                with zipfile.ZipFile(path) as archive:
                    inner_suffix = f"i0{suffix[1]}" if suffix.startswith("i") else "s"
                    self.assertEqual(
                        archive.namelist(),
                        [f"nsccl.20250102.{inner_suffix}.spn"],
                    )
            call = controller.api_calls[0]
            self.assertEqual(call["params"]["archives"], subject.ARCHIVES_PAYLOAD)

    def test_partial_special_session_has_six_terminal_cells(self) -> None:
        with TemporaryDirectory() as temp:
            report = _run(
                Path(temp),
                _Controller([_zip_response(_outer_zip(DAY, ("i1", "s")))]),
            )
            self.assertEqual(report.completed_dates, 1)
            self.assertEqual(report.downloaded_slots, 2)
            self.assertEqual(report.missing_slots, 4)
            self.assertEqual(
                [cell.state for cell in report.cells],
                [
                    "downloaded",
                    "slot_not_returned",
                    "slot_not_returned",
                    "slot_not_returned",
                    "slot_not_returned",
                    "downloaded",
                ],
            )

    def test_404_is_neutral_and_never_infers_holiday(self) -> None:
        with TemporaryDirectory() as temp:
            report = _run(Path(temp), _Controller([_Response(404, b"not found")]))
            self.assertEqual(report.completed_dates, 1)
            self.assertEqual(report.missing_slots, 6)
            self.assertEqual(
                {cell.state for cell in report.cells}, {"not_returned_http_404"}
            )
            self.assertNotIn("holiday", json.dumps(report.to_dict()).lower())

    def test_http_200_html_is_rejected_by_type_and_magic(self) -> None:
        cases = (
            (
                _Response(200, b"<html>blocked</html>", {"Content-Type": "text/html"}),
                "invalid_content_type",
            ),
            (_Response(200, b"<html>blocked</html>", {}), "invalid_zip_magic"),
        )
        for response, expected in cases:
            with self.subTest(expected=expected), TemporaryDirectory() as temp:
                report = _run(
                    Path(temp),
                    _Controller([response]),
                    config=_config(max_attempts=1),
                )
                self.assertEqual({cell.state for cell in report.cells}, {expected})
                self.assertEqual(report.incomplete_dates, 1)

    def test_invalid_or_corrupt_content_retries_and_recovers(self) -> None:
        cases = (
            (
                _Response(200, b"<html>blocked</html>", {"Content-Type": "text/html"}),
                "retrying_invalid_content_type",
            ),
            (_Response(200, b"<html>blocked</html>", {}), "retrying_invalid_zip_magic"),
            (_zip_response(b"PK\x03\x04broken"), "retrying_corrupt_outer_zip"),
            (
                _zip_response(
                    _outer_zip(DAY, ("i1",), inner_overrides={"i1": b"not a zip"})
                ),
                "retrying_corrupt_inner_zip",
            ),
            (
                _zip_response(
                    _outer_zip(
                        DAY,
                        ("i1",),
                        inner_overrides={"i1": _inner_zip(DAY, "i1", missing_spn=True)},
                    )
                ),
                "retrying_missing_spn",
            ),
        )
        for response, retry_state in cases:
            with self.subTest(retry_state=retry_state), TemporaryDirectory() as temp:
                root = Path(temp)
                sleeps: list[float] = []
                report = _run(
                    root,
                    _Controller([response, _zip_response(_outer_zip(DAY))]),
                    config=_config(max_attempts=2),
                    sleeps=sleeps,
                )

                self.assertEqual(report.completed_dates, 1)
                self.assertEqual(report.downloaded_slots, 6)
                self.assertEqual(report.network_calls, 2)
                self.assertEqual(len(sleeps), 1)
                self.assertIn(retry_state, {event["state"] for event in _events(root)})

    def test_403_retries_and_reduces_adaptive_concurrency(self) -> None:
        with TemporaryDirectory() as temp:
            sleeps: list[float] = []
            controller = _Controller([_Response(403), _zip_response(_outer_zip(DAY))])
            report = _run(
                Path(temp),
                controller,
                config=_config(max_concurrent=2),
                sleeps=sleeps,
            )
            self.assertEqual(report.network_calls, 2)
            self.assertEqual(report.throttle_events, 1)
            self.assertEqual(report.minimum_concurrency, 1)
            self.assertEqual(sleeps, [0])
            self.assertIn(
                "retrying_http_403", {event["state"] for event in _events(Path(temp))}
            )

    def test_429_honors_retry_after(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            sleeps: list[float] = []
            controller = _Controller(
                [
                    _Response(429, headers={"Retry-After": "7"}),
                    _zip_response(_outer_zip(DAY)),
                ]
            )
            report = _run(root, controller, sleeps=sleeps)
            self.assertEqual(report.completed_dates, 1)
            self.assertEqual(sleeps, [7.0])
            self.assertEqual(report.throttle_events, 1)

    def test_transient_5xx_timeout_and_reset_all_retry(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            controller = _Controller(
                [
                    _Response(503),
                    TimeoutError("slow"),
                    ConnectionResetError("reset"),
                    _zip_response(_outer_zip(DAY)),
                ]
            )
            report = _run(root, controller)
            self.assertEqual(report.network_calls, 4)
            self.assertEqual(report.completed_dates, 1)
            states = {event["state"] for event in _events(root)}
            self.assertIn("retrying_http_503", states)
            self.assertIn("retrying_transport_error", states)

    def test_exhausted_day_succeeds_on_bounded_retry_pass(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            controller = _Controller(
                [
                    TimeoutError("initial one"),
                    ConnectionResetError("initial two"),
                    _zip_response(_outer_zip(DAY)),
                ]
            )
            report = _run(
                root,
                controller,
                config=_config(max_attempts=2, retry_incomplete_passes=1),
            )

            self.assertEqual(report.completed_dates, 1)
            self.assertEqual(report.failed_slots, 0)
            self.assertEqual(report.network_calls, 3)
            self.assertEqual(report.configured_retry_passes, 1)
            self.assertEqual(report.executed_retry_passes, 1)
            self.assertEqual(report.retried_dates, 1)
            self.assertEqual(report.retry_network_calls, 1)
            self.assertEqual({cell.attempt for cell in report.cells}, {1})
            self.assertEqual(report.to_dict()["retry_network_calls"], 1)

    def test_persistent_failure_stays_nonterminal_after_fixed_retry_passes(
        self,
    ) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            controller = _Controller([TimeoutError("persistent")] * 6)
            report = _run(
                root,
                controller,
                config=_config(max_attempts=2, retry_incomplete_passes=2),
            )

            self.assertEqual(report.network_calls, 6)
            self.assertEqual(report.executed_retry_passes, 2)
            self.assertEqual(report.retried_dates, 1)
            self.assertEqual(report.retry_network_calls, 4)
            self.assertEqual(report.incomplete_dates, 1)
            self.assertEqual(report.failed_slots, 6)
            self.assertTrue(all(not cell.terminal for cell in report.cells))
            self.assertEqual({cell.state for cell in report.cells}, {"transport_error"})
            self.assertEqual({cell.attempt for cell in report.cells}, {2})

    def test_cli_forwards_retry_passes_and_returns_failure_for_failed_slots(
        self,
    ) -> None:
        captured = StringIO()
        fake_report = SimpleNamespace(
            failed_slots=6,
            downloaded_slots=0,
            to_dict=lambda: {"failed_slots": 6},
        )
        with (
            mock.patch(
                "nifty_span.span.backfill_downloader.download_span_backfill",
                return_value=fake_report,
            ) as download,
            redirect_stdout(captured),
        ):
            code = span_backfill_main(
                [
                    "download",
                    "--start-date",
                    "2025-01-02",
                    "--end-date",
                    "2025-01-02",
                    "--retry-incomplete-passes",
                    "2",
                    "--repair-order",
                    "unseen-first",
                    "--json",
                ]
            )

        self.assertEqual(code, 1)
        self.assertEqual(json.loads(captured.getvalue()), {"failed_slots": 6})
        self.assertEqual(download.call_args.kwargs["config"].retry_incomplete_passes, 2)
        self.assertEqual(
            download.call_args.kwargs["config"].repair_order, "unseen-first"
        )

    def test_unseen_first_skips_known_corrupt_bundle_and_blocked_companions(
        self,
    ) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = root / "span_backfill_manifest.jsonl"
            known_corrupt = [
                {
                    "trading_date": DAY.isoformat(),
                    "slot": slot,
                    "suffix": suffix,
                    "state": (
                        "corrupt_inner_zip"
                        if slot == "BOD"
                        else "bundle_validation_blocked"
                    ),
                    "terminal": False,
                    "attempt": 4,
                }
                for slot, suffix in subject.SLOT_SPECS
            ]
            manifest.write_text(
                "".join(json.dumps(event) + "\n" for event in known_corrupt),
                encoding="utf-8",
            )
            unseen = DAY.replace(day=3)
            controller = _Controller([_zip_response(_outer_zip(unseen))])

            report = _run(
                root,
                controller,
                start=DAY,
                end=unseen,
                config=_config(
                    max_attempts=1,
                    retry_incomplete_passes=1,
                    repair_order="unseen-first",
                ),
            )

            self.assertEqual(report.network_calls, 1)
            self.assertEqual(report.repair_order, "unseen-first")
            self.assertEqual(controller.api_calls[0]["params"]["date"], "03-Jan-2025")
            day_one_states = {
                cell.state
                for cell in report.cells
                if cell.trading_date == DAY.isoformat()
            }
            self.assertEqual(
                day_one_states,
                {"corrupt_inner_zip", "bundle_validation_blocked"},
            )

    def test_unseen_first_runs_transport_queue_after_unseen_dates(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            manifest = root / "span_backfill_manifest.jsonl"
            transport = [
                {
                    "trading_date": DAY.isoformat(),
                    "slot": slot,
                    "suffix": suffix,
                    "state": "transport_error",
                    "terminal": False,
                    "attempt": 4,
                }
                for slot, suffix in subject.SLOT_SPECS
            ]
            manifest.write_text(
                "".join(json.dumps(event) + "\n" for event in transport),
                encoding="utf-8",
            )
            unseen = DAY.replace(day=3)
            controller = _Controller(
                [
                    _zip_response(_outer_zip(unseen)),
                    _zip_response(_outer_zip(DAY)),
                ]
            )

            report = _run(
                root,
                controller,
                start=DAY,
                end=unseen,
                config=_config(
                    max_attempts=1,
                    retry_incomplete_passes=1,
                    repair_order="unseen-first",
                ),
            )

            self.assertEqual(report.network_calls, 2)
            self.assertEqual(report.executed_retry_passes, 0)
            self.assertEqual(
                [call["params"]["date"] for call in controller.api_calls],
                ["03-Jan-2025", "02-Jan-2025"],
            )
            self.assertEqual(report.completed_dates, 2)

    def test_unseen_first_does_not_generic_retry_a_new_corrupt_bundle(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            controller = _Controller(
                [_zip_response(_outer_zip(DAY, inner_overrides={"i1": b"not a zip"}))]
            )

            report = _run(
                root,
                controller,
                config=_config(
                    max_attempts=1,
                    retry_incomplete_passes=2,
                    repair_order="unseen-first",
                ),
            )

            self.assertEqual(report.network_calls, 1)
            self.assertEqual(report.executed_retry_passes, 0)
            self.assertEqual(report.retry_network_calls, 0)
            self.assertEqual(
                {cell.state for cell in report.cells},
                {"corrupt_inner_zip", "downloaded"},
            )

    def test_terminal_date_is_not_scheduled_for_retry_pass(self) -> None:
        with TemporaryDirectory() as temp:
            controller = _Controller([_Response(404)])
            report = _run(
                Path(temp),
                controller,
                config=_config(retry_incomplete_passes=2),
            )

            self.assertEqual(report.network_calls, 1)
            self.assertEqual(report.completed_dates, 1)
            self.assertEqual(report.executed_retry_passes, 0)
            self.assertEqual(report.retried_dates, 0)
            self.assertEqual(report.retry_network_calls, 0)

    def test_corrupt_outer_and_corrupt_inner_are_rejected(self) -> None:
        cases = (
            (_zip_response(b"PK\x03\x04broken"), "corrupt_outer_zip"),
            (
                _zip_response(
                    _outer_zip(DAY, ("i1",), inner_overrides={"i1": b"not a zip"})
                ),
                "corrupt_inner_zip",
            ),
        )
        for response, expected in cases:
            with self.subTest(expected=expected), TemporaryDirectory() as temp:
                report = _run(
                    Path(temp),
                    _Controller([response]),
                    config=_config(max_attempts=1),
                )
                self.assertIn(expected, {cell.state for cell in report.cells})
                self.assertEqual(report.downloaded_slots, 0)

    def test_missing_spn_and_inner_filename_mismatch(self) -> None:
        cases = (
            (
                _inner_zip(DAY, "i1", missing_spn=True),
                "missing_spn",
            ),
            (
                _inner_zip(DAY, "i1", name="nsccl.20250102.i1.spn"),
                "filename_mismatch",
            ),
            (
                _inner_zip(DAY, "i1", extra_member=True),
                "filename_mismatch",
            ),
        )
        for inner, expected in cases:
            with self.subTest(expected=expected), TemporaryDirectory() as temp:
                response = _zip_response(
                    _outer_zip(DAY, ("i1",), inner_overrides={"i1": inner})
                )
                report = _run(
                    Path(temp),
                    _Controller([response]),
                    config=_config(max_attempts=1),
                )
                self.assertIn(expected, {cell.state for cell in report.cells})

    def test_path_traversal_outer_and_inner_is_rejected(self) -> None:
        outer_traversal = _outer_zip(
            DAY,
            ("i1",),
            outer_names={"i1": "../nsccl.20250102.i1.zip"},
        )
        inner_traversal = _inner_zip(
            DAY,
            "i1",
            name="../nsccl.20250102.i01.spn",
        )
        cases = (
            (_zip_response(outer_traversal), "unsafe_outer_member"),
            (
                _zip_response(
                    _outer_zip(DAY, ("i1",), inner_overrides={"i1": inner_traversal})
                ),
                "unsafe_inner_member",
            ),
        )
        for response, expected in cases:
            with self.subTest(expected=expected), TemporaryDirectory() as temp:
                report = _run(Path(temp), _Controller([response]))
                self.assertIn(expected, {cell.state for cell in report.cells})
                self.assertFalse(list(Path(temp).rglob("*.zip")))

    def test_wrong_date_and_duplicate_suffix_are_rejected(self) -> None:
        cases = (
            _outer_zip(
                DAY,
                ("i1",),
                outer_names={"i1": "nsccl.20250103.i1.zip"},
            ),
            _outer_zip(DAY, ("i1",), duplicate_suffix="i1"),
        )
        expected = ("filename_mismatch", "duplicate_member_name")
        for content, state in zip(cases, expected, strict=True):
            with self.subTest(state=state), TemporaryDirectory() as temp:
                report = _run(Path(temp), _Controller([_zip_response(content)]))
                self.assertEqual({cell.state for cell in report.cells}, {state})

    def test_partial_file_is_cleaned_if_revalidation_fails(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            controller = _Controller([_zip_response(_outer_zip(DAY, ("i1",)))])
            failure = subject._ArchiveError("local_file_invalid", "forced")
            with mock.patch.object(
                subject, "_validate_saved_inner", side_effect=failure
            ):
                report = _run(root, controller)
            self.assertIn("local_file_invalid", {cell.state for cell in report.cells})
            self.assertFalse(list(root.rglob("*.partial")))
            self.assertFalse(list(root.rglob("*.zip")))

    def test_existing_different_hash_is_never_overwritten(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            destination = root / "2025/01/02/nsccl.20250102.i1.zip"
            destination.parent.mkdir(parents=True)
            original = _inner_zip(DAY, "i1", payload=b"<spanFile>old</spanFile>")
            destination.write_bytes(original)
            returned = _inner_zip(DAY, "i1", payload=b"<spanFile>new</spanFile>")
            response = _zip_response(
                _outer_zip(DAY, ("i1",), inner_overrides={"i1": returned})
            )
            report = _run(root, _Controller([response]))
            self.assertIn(
                "immutable_hash_conflict", {cell.state for cell in report.cells}
            )
            self.assertEqual(destination.read_bytes(), original)
            self.assertFalse(list(root.rglob("*.partial")))

    def test_concurrent_publish_never_overwrites_immutable_winner(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            first_bytes = _inner_zip(DAY, "i1", payload=b"<spanFile>first</spanFile>")
            second_bytes = _inner_zip(DAY, "i1", payload=b"<spanFile>second</spanFile>")
            outer_info = zipfile.ZipInfo("nsccl.20250102.i1.zip")
            outer_info.CRC = 0
            first = subject._validate_inner_archive(DAY, "i1", outer_info, first_bytes)
            second = subject._validate_inner_archive(
                DAY, "i1", outer_info, second_bytes
            )
            real_link = subject.os.link
            publication_barrier = threading.Barrier(2)

            def synchronized_link(source: Path, destination: Path) -> None:
                publication_barrier.wait(timeout=5)
                real_link(source, destination)

            def publish(
                archive: subject._InnerArchive,
            ) -> tuple[str, Path] | subject._ArchiveError:
                try:
                    return subject._persist_inner_archive(root, DAY, archive)
                except subject._ArchiveError as exc:
                    return exc

            with mock.patch.object(subject.os, "link", side_effect=synchronized_link):
                with ThreadPoolExecutor(max_workers=2) as pool:
                    outcomes = list(pool.map(publish, (first, second)))

            successes = [item for item in outcomes if isinstance(item, tuple)]
            conflicts = [
                item for item in outcomes if isinstance(item, subject._ArchiveError)
            ]
            self.assertEqual(len(successes), 1)
            self.assertEqual(len(conflicts), 1)
            self.assertEqual(conflicts[0].state, "immutable_hash_conflict")
            destination = root / "2025/01/02/nsccl.20250102.i1.zip"
            winning_bytes = destination.read_bytes()
            self.assertIn(winning_bytes, (first_bytes, second_bytes))
            # The winner remains unchanged after the losing publisher has exited.
            self.assertEqual(destination.read_bytes(), winning_bytes)
            self.assertFalse(list(root.rglob("*.partial")))

    def test_interrupted_resume_refetches_invalid_local_cell(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            first = _run(root, _Controller([_zip_response(_outer_zip(DAY))]))
            self.assertEqual(first.downloaded_slots, 6)
            missing = root / "2025/01/02/nsccl.20250102.i3.zip"
            missing.unlink()

            controller = _Controller([_zip_response(_outer_zip(DAY))])
            second = _run(root, controller)
            self.assertEqual(second.network_calls, 1)
            self.assertTrue(missing.is_file())
            events = _events(root)
            id2 = [event["state"] for event in events if event["slot"] == "ID2"]
            self.assertEqual(id2[-2:], ["local_file_invalid", "downloaded"])

    def test_completed_resume_makes_zero_network_or_warm_calls(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            _run(root, _Controller([_zip_response(_outer_zip(DAY))]))
            controller = _Controller([])
            report = _run(root, controller)
            self.assertEqual(report.network_calls, 0)
            self.assertEqual(report.skipped_completed_dates, 1)
            self.assertEqual(controller.api_calls, [])
            self.assertEqual(controller.warm_calls, 0)

    def test_reprobe_missing_controls_network_resume(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            _run(root, _Controller([_Response(404)]))
            skipped = _run(root, _Controller([]))
            self.assertEqual(skipped.network_calls, 0)

            controller = _Controller([_Response(404)])
            reprobed = _run(root, controller, config=_config(reprobe_missing=True))
            self.assertEqual(reprobed.network_calls, 1)

    def test_manifest_is_append_only_and_records_transitions_and_metadata(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            controller = _Controller(
                [TimeoutError("first"), _zip_response(_outer_zip(DAY))]
            )
            _run(root, controller)
            events = _events(root)
            self.assertEqual(len(events), 12)
            self.assertEqual({event["schema_version"] for event in events}, {1})
            self.assertEqual(
                {event["state"] for event in events[:6]}, {"retrying_transport_error"}
            )
            self.assertTrue(all(event["terminal"] is False for event in events[:6]))
            downloaded = events[6:]
            self.assertTrue(all(event["terminal"] is True for event in downloaded))
            self.assertTrue(all(event.get("sha256") for event in downloaded))
            self.assertTrue(all(event.get("outer_member") for event in downloaded))
            self.assertTrue(all(event.get("inner_spn") for event in downloaded))
            self.assertTrue(
                all(event.get("zip_crc_ok") is True for event in downloaded)
            )
            self.assertTrue(all(event.get("members") for event in downloaded))
            self.assertTrue(all(event.get("response") for event in downloaded))

    def test_corrupt_inner_writes_independent_durable_state_for_every_cell(
        self,
    ) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            response = _zip_response(
                _outer_zip(DAY, ("i1",), inner_overrides={"i1": b"not a zip"})
            )
            _run(root, _Controller([response]), config=_config(max_attempts=1))
            events = _events(root)
            self.assertEqual(len(events), 6)
            self.assertEqual(
                {event["slot"] for event in events},
                {slot for slot, _ in subject.SLOT_SPECS},
            )
            self.assertEqual(
                {event["state"] for event in events},
                {"corrupt_inner_zip", "slot_not_returned"},
            )
            corrupt = next(event for event in events if event["suffix"] == "i1")
            self.assertEqual(
                corrupt["response"]["body_sha256"],
                subject.sha256(response.content).hexdigest(),
            )
            self.assertEqual(
                corrupt["rejected_inner"]["sha256"],
                subject.sha256(b"not a zip").hexdigest(),
            )

    def test_five_valid_inner_archives_are_salvaged_when_one_is_corrupt(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            corrupt_bytes = b"not a zip"
            response = _zip_response(
                _outer_zip(DAY, inner_overrides={"i3": corrupt_bytes})
            )

            report = _run(
                root,
                _Controller([response]),
                config=_config(max_attempts=1),
            )

            self.assertEqual(report.downloaded_slots, 5)
            self.assertEqual(report.failed_slots, 1)
            self.assertEqual(report.incomplete_dates, 1)
            self.assertEqual(
                {(cell.suffix, cell.state) for cell in report.cells},
                {
                    ("i1", "downloaded"),
                    ("i2", "downloaded"),
                    ("i3", "corrupt_inner_zip"),
                    ("i4", "downloaded"),
                    ("i5", "downloaded"),
                    ("s", "downloaded"),
                },
            )
            saved = sorted(path.name for path in root.rglob("*.zip"))
            self.assertEqual(len(saved), 5)
            self.assertNotIn("nsccl.20250102.i3.zip", saved)

            events = _events(root)
            self.assertEqual(len(events), 6)
            corrupt = next(event for event in events if event["suffix"] == "i3")
            self.assertFalse(corrupt["terminal"])
            self.assertNotIn("path", corrupt)
            self.assertEqual(corrupt["outer_member"]["name"], "nsccl.20250102.i3.zip")
            self.assertEqual(
                corrupt["rejected_inner"]["size_bytes"], len(corrupt_bytes)
            )
            self.assertEqual(
                corrupt["rejected_inner"]["sha256"],
                subject.sha256(corrupt_bytes).hexdigest(),
            )
            self.assertEqual(
                {event["response"]["body_sha256"] for event in events},
                {subject.sha256(response.content).hexdigest()},
            )

    def test_multiple_corrupt_inner_archives_do_not_block_valid_siblings(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            response = _zip_response(
                _outer_zip(
                    DAY,
                    inner_overrides={"i2": b"bad i2", "s": b"bad settlement"},
                )
            )

            report = _run(
                root,
                _Controller([response]),
                config=_config(max_attempts=1),
            )

            self.assertEqual(report.downloaded_slots, 4)
            self.assertEqual(report.failed_slots, 2)
            self.assertEqual(
                {
                    cell.suffix
                    for cell in report.cells
                    if cell.state == "corrupt_inner_zip"
                },
                {"i2", "s"},
            )
            self.assertEqual(len(list(root.rglob("*.zip"))), 4)
            self.assertFalse((root / "2025/01/02/nsccl.20250102.i2.zip").exists())
            self.assertFalse((root / "2025/01/02/nsccl.20250102.s.zip").exists())

    def test_unexpected_outer_member_blocks_bundle_but_missing_slot_does_not(
        self,
    ) -> None:
        with self.subTest(case="unexpected"), TemporaryDirectory() as temp:
            root = Path(temp)
            response = _zip_response(
                _outer_zip(
                    DAY,
                    outer_names={"i3": "unexpected.zip"},
                )
            )
            report = _run(
                root,
                _Controller([response]),
                config=_config(max_attempts=1),
            )
            self.assertEqual(
                {cell.state for cell in report.cells}, {"filename_mismatch"}
            )
            self.assertEqual(report.downloaded_slots, 0)
            self.assertFalse(list(root.rglob("*.zip")))

        with self.subTest(case="missing"), TemporaryDirectory() as temp:
            root = Path(temp)
            response = _zip_response(_outer_zip(DAY, ("i1", "i2", "i3", "i4", "s")))
            report = _run(root, _Controller([response]))
            self.assertEqual(report.downloaded_slots, 5)
            self.assertEqual(report.missing_slots, 1)
            self.assertEqual(report.completed_dates, 1)
            id4 = next(cell for cell in report.cells if cell.suffix == "i5")
            self.assertEqual(id4.state, "slot_not_returned")
            self.assertTrue(id4.terminal)

    def test_partial_salvage_resume_fetches_only_unresolved_slot_in_manifest(
        self,
    ) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            first_response = _zip_response(
                _outer_zip(DAY, inner_overrides={"i4": b"not a zip"})
            )
            first = _run(
                root,
                _Controller([first_response]),
                config=_config(max_attempts=1),
            )
            self.assertEqual(first.downloaded_slots, 5)
            before = {
                path.name: (path.stat().st_size, subject._file_sha256(path))
                for path in root.rglob("*.zip")
            }

            controller = _Controller([_zip_response(_outer_zip(DAY))])
            resumed = _run(root, controller, config=_config(max_attempts=1))

            self.assertEqual(resumed.network_calls, 1)
            self.assertEqual(resumed.downloaded_slots, 6)
            self.assertEqual(resumed.completed_dates, 1)
            self.assertEqual(len(list(root.rglob("*.zip"))), 6)
            after = {
                path.name: (path.stat().st_size, subject._file_sha256(path))
                for path in root.rglob("*.zip")
                if path.name in before
            }
            self.assertEqual(after, before)

            events = _events(root)
            by_suffix = {
                suffix: [event for event in events if event["suffix"] == suffix]
                for _slot, suffix in subject.SLOT_SPECS
            }
            self.assertEqual(len(by_suffix["i4"]), 2)
            self.assertEqual(
                [event["state"] for event in by_suffix["i4"]],
                ["corrupt_inner_zip", "downloaded"],
            )
            self.assertTrue(
                all(
                    len(by_suffix[suffix]) == 1
                    for suffix in {"i1", "i2", "i3", "i5", "s"}
                )
            )

    def test_queue_and_active_requests_are_bounded_for_long_range(self) -> None:
        start = date(2025, 1, 1)
        end = date(2025, 1, 30)
        controller = _Controller([_Response(404)] * 30, delay=0.001)
        with TemporaryDirectory() as temp:
            report = _run(
                Path(temp),
                controller,
                start=start,
                end=end,
                config=_config(max_concurrent=3, queue_size=2),
            )
            self.assertEqual(report.requested_dates, 30)
            self.assertLessEqual(report.max_queue_depth, 2)
            self.assertLessEqual(controller.max_active, 3)
            self.assertEqual(report.completed_dates, 30)

    def test_retry_pass_queue_and_active_requests_remain_bounded(self) -> None:
        start = date(2025, 1, 1)
        end = date(2025, 1, 6)
        controller = _Controller(
            [TimeoutError("initial pass")] * 6 + [_Response(404)] * 6,
            delay=0.001,
        )
        with TemporaryDirectory() as temp:
            report = _run(
                Path(temp),
                controller,
                start=start,
                end=end,
                config=_config(
                    max_concurrent=3,
                    queue_size=2,
                    max_attempts=1,
                    retry_incomplete_passes=1,
                ),
            )

            self.assertEqual(report.completed_dates, 6)
            self.assertEqual(report.executed_retry_passes, 1)
            self.assertEqual(report.retried_dates, 6)
            self.assertEqual(report.retry_network_calls, 6)
            self.assertLessEqual(report.max_queue_depth, 2)
            self.assertLessEqual(controller.max_active, 3)

    def test_session_refreshes_and_rewarms_periodically(self) -> None:
        start = date(2025, 1, 1)
        end = date(2025, 1, 3)
        controller = _Controller([_Response(404)] * 3)
        with TemporaryDirectory() as temp:
            report = _run(
                Path(temp),
                controller,
                start=start,
                end=end,
                config=_config(session_refresh_requests=1),
            )
            self.assertEqual(report.network_calls, 3)
            self.assertEqual(controller.warm_calls, 3)

    def test_response_and_zip_resource_limits_are_configurable_and_enforced(
        self,
    ) -> None:
        defaults = subject.ArchiveResourceLimits()
        cases = (
            (
                replace(defaults, max_response_bytes=8),
                _outer_zip(DAY),
                "response_resource_limit_exceeded",
            ),
            (
                replace(defaults, max_outer_members=1),
                _outer_zip(DAY),
                "zip_resource_limit_exceeded",
            ),
            (
                replace(defaults, max_outer_member_compressed_bytes=1),
                _outer_zip(DAY, ("i1",)),
                "zip_resource_limit_exceeded",
            ),
            (
                replace(defaults, max_outer_member_uncompressed_bytes=1),
                _outer_zip(DAY, ("i1",)),
                "zip_resource_limit_exceeded",
            ),
            (
                replace(defaults, max_outer_total_compressed_bytes=1),
                _outer_zip(DAY, ("i1",)),
                "zip_resource_limit_exceeded",
            ),
            (
                replace(defaults, max_outer_total_uncompressed_bytes=1),
                _outer_zip(DAY, ("i1",)),
                "zip_resource_limit_exceeded",
            ),
            (
                replace(defaults, max_inner_archive_bytes=1),
                _outer_zip(DAY, ("i1",)),
                "zip_resource_limit_exceeded",
            ),
            (
                replace(defaults, max_inner_member_compressed_bytes=1),
                _outer_zip(DAY, ("i1",)),
                "zip_resource_limit_exceeded",
            ),
            (
                replace(defaults, max_inner_member_uncompressed_bytes=1),
                _outer_zip(DAY, ("i1",)),
                "zip_resource_limit_exceeded",
            ),
            (
                replace(defaults, max_inner_total_compressed_bytes=1),
                _outer_zip(DAY, ("i1",)),
                "zip_resource_limit_exceeded",
            ),
            (
                replace(defaults, max_inner_total_uncompressed_bytes=1),
                _outer_zip(DAY, ("i1",)),
                "zip_resource_limit_exceeded",
            ),
            (
                replace(defaults, max_compression_ratio=2),
                _outer_zip(
                    DAY,
                    ("i1",),
                    inner_overrides={
                        "i1": _inner_zip(DAY, "i1", payload=b"0" * 10_000)
                    },
                ),
                "zip_resource_limit_exceeded",
            ),
        )
        for limits, content, state in cases:
            with self.subTest(state=state, limits=limits), TemporaryDirectory() as temp:
                report = _run(
                    Path(temp),
                    _Controller([_zip_response(content)]),
                    config=_config(archive_limits=limits),
                )
                self.assertIn(state, {cell.state for cell in report.cells})
                self.assertEqual(report.downloaded_slots, 0)
        with self.assertRaisesRegex(ValueError, "must be > 0"):
            replace(defaults, max_inner_members=0).validated()

    def test_encrypted_symlink_and_duplicate_members_are_explicitly_rejected(
        self,
    ) -> None:
        encrypted_inner = _mark_first_member_encrypted(_inner_zip(DAY, "i1"))
        cases = (
            (
                _mark_first_member_encrypted(_outer_zip(DAY, ("i1",))),
                "encrypted_zip_member",
            ),
            (_outer_with_special_member(symlink=True), "symlink_zip_member"),
            (_outer_zip(DAY, ("i1",), duplicate_suffix="i1"), "duplicate_member_name"),
            (
                _outer_zip(DAY, ("i1",), inner_overrides={"i1": encrypted_inner}),
                "encrypted_zip_member",
            ),
            (
                _outer_zip(
                    DAY,
                    ("i1",),
                    inner_overrides={"i1": _inner_with_special_member(symlink=True)},
                ),
                "symlink_zip_member",
            ),
            (
                _outer_zip(
                    DAY,
                    ("i1",),
                    inner_overrides={"i1": _inner_with_special_member(duplicate=True)},
                ),
                "duplicate_member_name",
            ),
        )
        for content, state in cases:
            with self.subTest(state=state), TemporaryDirectory() as temp:
                report = _run(Path(temp), _Controller([_zip_response(content)]))
                self.assertIn(state, {cell.state for cell in report.cells})
                self.assertFalse(list(Path(temp).rglob("*.zip")))

    def test_truncated_final_manifest_record_is_quarantined_and_prefix_restored(
        self,
    ) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            _run(root, _Controller([_Response(404)]))
            manifest = root / "span_backfill_manifest.jsonl"
            corrupt_tail = b'{"schema_version":1,"trading_date":"2025-01'
            manifest.write_bytes(manifest.read_bytes() + corrupt_tail)

            controller = _Controller([])
            report = _run(root, controller)
            self.assertEqual(report.network_calls, 0)
            self.assertEqual(report.skipped_completed_dates, 1)
            recovered_lines = manifest.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(recovered_lines), 6)
            self.assertTrue(
                all(isinstance(json.loads(line), dict) for line in recovered_lines)
            )
            quarantines = list(
                root.glob("span_backfill_manifest.jsonl.corrupt-tail.*.bin")
            )
            self.assertEqual(len(quarantines), 1)
            self.assertEqual(quarantines[0].read_bytes(), corrupt_tail)
            self.assertFalse(list(root.glob("*.recovery.partial")))

    def test_manifest_corruption_in_middle_fails_without_repair(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            _run(root, _Controller([_Response(404)]))
            manifest = root / "span_backfill_manifest.jsonl"
            lines = manifest.read_bytes().splitlines(keepends=True)
            original = b"".join(lines[:1] + [b"{broken-middle}\n"] + lines[1:])
            manifest.write_bytes(original)

            with self.assertRaisesRegex(ValueError, "invalid manifest JSON"):
                _run(root, _Controller([]))
            self.assertEqual(manifest.read_bytes(), original)
            self.assertFalse(
                list(root.glob("span_backfill_manifest.jsonl.corrupt-tail.*.bin"))
            )
