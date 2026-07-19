"""Validate and compare paired legacy/optimized SPAN extraction benchmarks."""

from __future__ import annotations

import argparse
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence
import uuid


COMPARISON_SCHEMA_VERSION = "span-benchmark-comparison-v1"


def compare_benchmarks(
    legacy_path: str | Path,
    optimized_path: str | Path,
    *,
    minimum_fresh_speedup: float = 3.0,
) -> dict[str, Any]:
    legacy_file = Path(legacy_path).resolve()
    optimized_file = Path(optimized_path).resolve()
    legacy = _load_evidence(legacy_file)
    optimized = _load_evidence(optimized_file)

    legacy_name = str(legacy.get("implementation", {}).get("name", "legacy"))
    optimized_name = str(optimized.get("implementation", {}).get("name", "optimized"))
    if legacy_name != "legacy":
        raise ValueError(f"legacy evidence identifies implementation {legacy_name!r}")
    if optimized_name == legacy_name:
        raise ValueError("benchmark implementations must differ")

    _require_equal(legacy.get("date_filter"), optimized.get("date_filter"), "date_filter")
    _require_equal(legacy.get("workers"), optimized.get("workers"), "workers")
    _require_equal(legacy.get("symbols"), optimized.get("symbols"), "symbols")
    legacy_inputs = _validated_inputs(legacy, "legacy")
    optimized_inputs = _validated_inputs(optimized, "optimized")
    _require_equal(legacy_inputs, optimized_inputs, "validated input archives")
    _validate_runs(legacy, "legacy")
    _validate_runs(optimized, "optimized")

    legacy_digest = _semantic_digest(legacy, "legacy")
    optimized_digest = _semantic_digest(optimized, "optimized")
    _require_equal(legacy_digest, optimized_digest, "semantic digest")
    legacy_rows = _row_count(legacy, "legacy")
    optimized_rows = _row_count(optimized, "optimized")
    _require_equal(legacy_rows, optimized_rows, "semantic row count")

    legacy_fresh = _summary_metric(legacy, "fresh_runs", "wall_seconds", "median")
    optimized_fresh = _summary_metric(
        optimized, "fresh_runs", "wall_seconds", "median"
    )
    legacy_rerun = _summary_metric(legacy, "rerun_runs", "wall_seconds", "median")
    optimized_rerun = _summary_metric(
        optimized, "rerun_runs", "wall_seconds", "median"
    )
    legacy_rss = _summary_metric(
        legacy,
        "fresh_runs",
        "peak_aggregate_process_tree_rss_estimate_bytes",
        "max",
    )
    optimized_rss = _summary_metric(
        optimized,
        "fresh_runs",
        "peak_aggregate_process_tree_rss_estimate_bytes",
        "max",
    )
    legacy_private = _summary_metric(
        legacy,
        "fresh_runs",
        "peak_process_tree_private_memory_estimate_bytes",
        "max",
    )
    optimized_private = _summary_metric(
        optimized,
        "fresh_runs",
        "peak_process_tree_private_memory_estimate_bytes",
        "max",
    )
    fresh_speedup = legacy_fresh / optimized_fresh
    rerun_speedup = legacy_rerun / optimized_rerun
    rss_reduction = legacy_rss / optimized_rss
    private_reduction = legacy_private / optimized_private
    fresh_gate = fresh_speedup >= minimum_fresh_speedup

    return {
        "comparison_schema_version": COMPARISON_SCHEMA_VERSION,
        "configuration": {
            "date_filter": legacy.get("date_filter"),
            "fresh_runs": legacy.get("configuration", {}).get("fresh_runs"),
            "minimum_fresh_speedup": minimum_fresh_speedup,
            "rerun_runs": legacy.get("configuration", {}).get("rerun_runs"),
            "symbols": legacy.get("symbols"),
            "workers": legacy.get("workers"),
        },
        "evidence": {
            "legacy": _evidence_reference(legacy_file, legacy_name),
            "optimized": _evidence_reference(optimized_file, optimized_name),
        },
        "input_archives": [
            {"path": path, "bytes": size, "sha256": digest}
            for path, size, digest in legacy_inputs
        ],
        "semantic_equivalence": {
            "canonical_semantic_sha256": legacy_digest,
            "row_count": legacy_rows,
            "passed": True,
        },
        "measurements": {
            "fresh_wall_seconds_median": {
                "legacy": legacy_fresh,
                "optimized": optimized_fresh,
                "speedup": fresh_speedup,
            },
            "rerun_wall_seconds_median": {
                "legacy": legacy_rerun,
                "optimized": optimized_rerun,
                "speedup": rerun_speedup,
            },
            "fresh_peak_process_tree_rss_bytes": {
                "legacy": legacy_rss,
                "optimized": optimized_rss,
                "reduction_ratio": rss_reduction,
            },
            "fresh_peak_process_tree_private_bytes": {
                "legacy": legacy_private,
                "optimized": optimized_private,
                "reduction_ratio": private_reduction,
            },
        },
        "gates": {
            "fresh_extraction_speedup": {
                "actual": fresh_speedup,
                "minimum": minimum_fresh_speedup,
                "passed": fresh_gate,
            },
            "identical_inputs": {"passed": True},
            "semantic_equivalence": {"passed": True},
        },
        "overall_outcome": "PASS" if fresh_gate else "FAIL_FRESH_SPEEDUP",
    }


def render_markdown(comparison: Mapping[str, Any]) -> str:
    measurements = comparison["measurements"]
    fresh = measurements["fresh_wall_seconds_median"]
    rerun = measurements["rerun_wall_seconds_median"]
    rss = measurements["fresh_peak_process_tree_rss_bytes"]
    private = measurements["fresh_peak_process_tree_private_bytes"]
    semantic = comparison["semantic_equivalence"]
    gate = comparison["gates"]["fresh_extraction_speedup"]
    outcome = comparison["overall_outcome"]
    return "\n".join(
        [
            "# Paired SPAN extraction benchmark",
            "",
            f"Overall outcome: **{outcome}**",
            "",
            "Both implementations consumed the exact same validated archive paths, sizes, and SHA-256 hashes. "
            "All measured runs were valid, the raw inputs were unchanged, and canonical semantic output matched.",
            "",
            "| Metric | Legacy | Optimized | Ratio |",
            "|---|---:|---:|---:|",
            f"| Fresh wall median (s) | {fresh['legacy']:.6f} | {fresh['optimized']:.6f} | {fresh['speedup']:.3f}x speedup |",
            f"| Unchanged rerun median (s) | {rerun['legacy']:.6f} | {rerun['optimized']:.6f} | {rerun['speedup']:.3f}x speedup |",
            f"| Fresh peak process-tree RSS (bytes) | {int(rss['legacy']):,} | {int(rss['optimized']):,} | {rss['reduction_ratio']:.3f}x lower |",
            f"| Fresh peak private memory (bytes) | {int(private['legacy']):,} | {int(private['optimized']):,} | {private['reduction_ratio']:.3f}x lower |",
            "",
            f"Semantic rows: `{semantic['row_count']:,}`; canonical SHA-256: `{semantic['canonical_semantic_sha256']}`.",
            "",
            "## Acceptance",
            "",
            f"The fresh-extraction gate requires at least `{gate['minimum']:.3f}x`; measured `{gate['actual']:.3f}x`: "
            f"**{'PASS' if gate['passed'] else 'FAIL'}**.",
            "",
            "This paired fixture does not substitute for the required representative complete-month benchmark. "
            "A failed speed gate must remain failed unless a complete-month result or a measured binding-limit report supports the final acceptance decision.",
            "",
        ]
    )


def write_comparison(
    comparison: Mapping[str, Any], json_path: str | Path, markdown_path: str | Path
) -> None:
    json_bytes = (
        json.dumps(comparison, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    ).encode("utf-8")
    markdown_bytes = render_markdown(comparison).encode("utf-8")
    _atomic_write(Path(json_path), json_bytes)
    _atomic_write(Path(markdown_path), markdown_bytes)


def _load_evidence(path: Path) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read benchmark evidence {path}: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise ValueError(f"benchmark evidence is not an object: {path}")
    return payload


def _validated_inputs(
    evidence: Mapping[str, Any], label: str
) -> tuple[tuple[str, int, str], ...]:
    validation = evidence.get("input_validation")
    if not isinstance(validation, Mapping):
        raise ValueError(f"{label} input_validation is missing")
    if validation.get("prevalidated") is not True:
        raise ValueError(f"{label} inputs were not prevalidated")
    if validation.get("inputs_unchanged_after_runs") is not True:
        raise ValueError(f"{label} inputs changed during the benchmark")
    before = validation.get("before")
    after = validation.get("after")
    if not isinstance(before, Sequence) or isinstance(before, (str, bytes)):
        raise ValueError(f"{label} validated input list is missing")
    rows = tuple(sorted(_input_identity(item, label) for item in before))
    after_rows = tuple(sorted(_input_identity(item, label) for item in after or ()))
    if rows != after_rows:
        raise ValueError(f"{label} before/after input identities differ")
    if not rows:
        raise ValueError(f"{label} validated input list is empty")
    return rows


def _input_identity(item: Any, label: str) -> tuple[str, int, str]:
    if not isinstance(item, Mapping):
        raise ValueError(f"{label} input entry is not an object")
    if item.get("zip_error") is not None or item.get("zip_testzip") is not None:
        raise ValueError(f"{label} input archive failed ZIP validation: {item.get('path')}")
    path = str(item.get("path", ""))
    digest = str(item.get("sha256", ""))
    try:
        size = int(item.get("bytes"))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} input size is invalid: {item!r}") from exc
    if not path or len(digest) != 64 or size <= 0:
        raise ValueError(f"{label} input identity is incomplete: {item!r}")
    return path, size, digest


def _validate_runs(evidence: Mapping[str, Any], label: str) -> None:
    configuration = evidence.get("configuration")
    if not isinstance(configuration, Mapping):
        raise ValueError(f"{label} configuration is missing")
    for group, configured_name in (
        ("fresh_runs", "fresh_runs"),
        ("rerun_runs", "rerun_runs"),
    ):
        runs = evidence.get(group)
        expected = int(configuration.get(configured_name, -1))
        if not isinstance(runs, list) or len(runs) != expected or expected < 1:
            raise ValueError(f"{label} {group} count does not match configuration")
        if any(run.get("valid") is not True for run in runs if isinstance(run, Mapping)):
            raise ValueError(f"{label} contains an invalid {group} run")
        if any(not isinstance(run, Mapping) for run in runs):
            raise ValueError(f"{label} contains a malformed {group} run")


def _semantic_digest(evidence: Mapping[str, Any], label: str) -> str:
    summaries = evidence.get("summaries")
    if not isinstance(summaries, Mapping) or summaries.get("semantic_outputs_identical") is not True:
        raise ValueError(f"{label} semantic outputs are not identical")
    variants = summaries.get("semantic_digest_variants")
    if not isinstance(variants, list) or len(variants) != 1:
        raise ValueError(f"{label} does not have one semantic digest")
    return str(variants[0])


def _row_count(evidence: Mapping[str, Any], label: str) -> int:
    counts = {
        int(run["semantic_output"]["row_count"])
        for group in ("fresh_runs", "rerun_runs")
        for run in evidence[group]
    }
    if len(counts) != 1:
        raise ValueError(f"{label} semantic row counts differ across runs")
    return counts.pop()


def _summary_metric(
    evidence: Mapping[str, Any], group: str, metric: str, statistic: str
) -> float:
    try:
        value = float(evidence["summaries"][group][metric][statistic])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"missing summary metric {group}.{metric}.{statistic}") from exc
    if value <= 0:
        raise ValueError(f"summary metric {group}.{metric}.{statistic} must be positive")
    return value


def _evidence_reference(path: Path, implementation: str) -> dict[str, Any]:
    content = path.read_bytes()
    return {
        "implementation": implementation,
        "path": str(path),
        "sha256": sha256(content).hexdigest(),
        "size_bytes": len(content),
    }


def _require_equal(left: Any, right: Any, name: str) -> None:
    if left != right:
        raise ValueError(f"paired benchmark {name} differs")


def _atomic_write(path: Path, content: bytes) -> None:
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    try:
        with partial.open("xb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(partial, path)
    finally:
        partial.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--legacy-json", required=True)
    parser.add_argument("--optimized-json", required=True)
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--output-markdown", required=True)
    parser.add_argument("--minimum-fresh-speedup", type=float, default=3.0)
    arguments = parser.parse_args()
    if arguments.minimum_fresh_speedup <= 0:
        parser.error("--minimum-fresh-speedup must be positive")
    comparison = compare_benchmarks(
        arguments.legacy_json,
        arguments.optimized_json,
        minimum_fresh_speedup=arguments.minimum_fresh_speedup,
    )
    write_comparison(comparison, arguments.output_json, arguments.output_markdown)
    print(json.dumps(comparison, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
