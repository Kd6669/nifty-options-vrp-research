"""Append-only JSONL writes resilient to brief Windows reader sharing locks.

Retries are deliberately limited to acquiring the append handle.  Once a
handle has been acquired, write/flush/fsync is attempted exactly once because a
failure at any of those stages has an ambiguous durability boundary and
replaying the record could duplicate it.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO
import asyncio
import errno
import json
import os
import random
import time


_TRANSIENT_WINDOWS_ERRORS = frozenset({5, 32, 33})


@dataclass(frozen=True)
class JsonlAppendRetryPolicy:
    """Bounded retry policy for acquiring an append handle."""

    max_attempts: int = 12
    initial_delay_seconds: float = 0.05
    max_delay_seconds: float = 1.0
    jitter_ratio: float = 0.2

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.initial_delay_seconds < 0:
            raise ValueError("initial_delay_seconds must be nonnegative")
        if self.max_delay_seconds < self.initial_delay_seconds:
            raise ValueError("max_delay_seconds must be >= initial_delay_seconds")
        if not 0 <= self.jitter_ratio <= 1:
            raise ValueError("jitter_ratio must be between 0 and 1")


DEFAULT_APPEND_RETRY_POLICY = JsonlAppendRetryPolicy()
OpenAppend = Callable[[Path], TextIO]
Sleep = Callable[[float], None]
AsyncSleep = Callable[[float], Awaitable[None]]
RandomValue = Callable[[], float]


def append_jsonl_records(
    path: str | Path,
    records: Iterable[Mapping[str, Any]],
    *,
    policy: JsonlAppendRetryPolicy = DEFAULT_APPEND_RETRY_POLICY,
    sleep: Sleep = time.sleep,
    random_value: RandomValue = random.random,
    open_append: OpenAppend | None = None,
) -> None:
    """Append serialized records after bounded retry of handle acquisition."""

    payload = _serialize_records(records)
    if not payload:
        return
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    opener = _open_append if open_append is None else open_append
    handle = _acquire_append_handle(
        destination,
        policy=policy,
        sleep=sleep,
        random_value=random_value,
        open_append=opener,
    )
    _write_once(handle, payload)


def append_jsonl_record(
    path: str | Path,
    record: Mapping[str, Any],
    **kwargs: Any,
) -> None:
    """Append one record using :func:`append_jsonl_records`."""

    append_jsonl_records(path, (record,), **kwargs)


async def append_jsonl_record_async(
    path: str | Path,
    record: Mapping[str, Any],
    *,
    policy: JsonlAppendRetryPolicy = DEFAULT_APPEND_RETRY_POLICY,
    sleep: AsyncSleep = asyncio.sleep,
    random_value: RandomValue = random.random,
    open_append: OpenAppend | None = None,
) -> None:
    """Asynchronously wait between append-handle acquisition attempts."""

    payload = _serialize_records((record,))
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    opener = _open_append if open_append is None else open_append
    handle = await _acquire_append_handle_async(
        destination,
        policy=policy,
        sleep=sleep,
        random_value=random_value,
        open_append=opener,
    )
    _write_once(handle, payload)


def _serialize_records(records: Iterable[Mapping[str, Any]]) -> str:
    return "".join(
        json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
        for record in records
    )


def _open_append(path: Path) -> TextIO:
    return path.open("a", encoding="utf-8", newline="\n")


def _acquire_append_handle(
    path: Path,
    *,
    policy: JsonlAppendRetryPolicy,
    sleep: Sleep,
    random_value: RandomValue,
    open_append: OpenAppend,
) -> TextIO:
    for attempt in range(1, policy.max_attempts + 1):
        try:
            return open_append(path)
        except PermissionError as exc:
            if not _is_transient_sharing_error(exc) or attempt == policy.max_attempts:
                raise
            sleep(_retry_delay(policy, attempt, random_value()))
    raise AssertionError("bounded append-open loop returned unexpectedly")


async def _acquire_append_handle_async(
    path: Path,
    *,
    policy: JsonlAppendRetryPolicy,
    sleep: AsyncSleep,
    random_value: RandomValue,
    open_append: OpenAppend,
) -> TextIO:
    for attempt in range(1, policy.max_attempts + 1):
        try:
            return open_append(path)
        except PermissionError as exc:
            if not _is_transient_sharing_error(exc) or attempt == policy.max_attempts:
                raise
            await sleep(_retry_delay(policy, attempt, random_value()))
    raise AssertionError("bounded append-open loop returned unexpectedly")


def _is_transient_sharing_error(exc: PermissionError) -> bool:
    winerror = getattr(exc, "winerror", None)
    if winerror is not None:
        return int(winerror) in _TRANSIENT_WINDOWS_ERRORS
    # CPython can expose a Windows sharing/access violation only as EACCES when
    # an exception is synthesized by pathlib or a filesystem interception layer.
    return exc.errno == errno.EACCES


def _retry_delay(
    policy: JsonlAppendRetryPolicy,
    failed_attempt: int,
    random_sample: float,
) -> float:
    bounded_sample = min(1.0, max(0.0, float(random_sample)))
    base = min(
        policy.max_delay_seconds,
        policy.initial_delay_seconds * (2 ** (failed_attempt - 1)),
    )
    multiplier = 1 + policy.jitter_ratio * (2 * bounded_sample - 1)
    return base * multiplier


def _write_once(handle: TextIO, payload: str) -> None:
    # Never retry this block: a failure may occur after bytes reached the file.
    with handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
