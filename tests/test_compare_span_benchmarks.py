from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from scripts.compare_span_benchmarks import compare_benchmarks, write_comparison


class CompareSpanBenchmarksTests(unittest.TestCase):
    def test_valid_pair_computes_ratios_and_failed_gate_honestly(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            legacy, optimized = _write_pair(root)
            comparison = compare_benchmarks(legacy, optimized)

            self.assertEqual(comparison["overall_outcome"], "FAIL_FRESH_SPEEDUP")
            self.assertAlmostEqual(
                comparison["measurements"]["fresh_wall_seconds_median"]["speedup"],
                2.0,
            )
            self.assertAlmostEqual(
                comparison["measurements"]["rerun_wall_seconds_median"]["speedup"],
                10.0,
            )
            self.assertTrue(comparison["semantic_equivalence"]["passed"])
            self.assertFalse(comparison["gates"]["fresh_extraction_speedup"]["passed"])

    def test_input_identity_mismatch_is_a_hard_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            legacy, optimized = _write_pair(root)
            payload = json.loads(optimized.read_text(encoding="utf-8"))
            payload["input_validation"]["before"][0]["sha256"] = "f" * 64
            payload["input_validation"]["after"][0]["sha256"] = "f" * 64
            optimized.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "validated input archives differs"):
                compare_benchmarks(legacy, optimized)

    def test_invalid_run_and_semantic_mismatch_are_hard_failures(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            legacy, optimized = _write_pair(root)
            payload = json.loads(optimized.read_text(encoding="utf-8"))
            payload["fresh_runs"][0]["valid"] = False
            optimized.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "invalid fresh_runs run"):
                compare_benchmarks(legacy, optimized)

            legacy, optimized = _write_pair(root)
            payload = json.loads(optimized.read_text(encoding="utf-8"))
            payload["summaries"]["semantic_digest_variants"] = ["b" * 64]
            optimized.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "semantic digest differs"):
                compare_benchmarks(legacy, optimized)

    def test_json_and_markdown_outputs_are_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            legacy, optimized = _write_pair(root)
            comparison = compare_benchmarks(legacy, optimized)
            json_path = root / "comparison.json"
            markdown_path = root / "comparison.md"
            write_comparison(comparison, json_path, markdown_path)
            first = (json_path.read_bytes(), markdown_path.read_bytes())
            write_comparison(comparison, json_path, markdown_path)

            self.assertEqual((json_path.read_bytes(), markdown_path.read_bytes()), first)
            self.assertIn("**FAIL**", markdown_path.read_text(encoding="utf-8"))


def _write_pair(root: Path) -> tuple[Path, Path]:
    legacy = root / "legacy.json"
    optimized = root / "optimized.json"
    legacy.write_text(json.dumps(_evidence("legacy", fresh=20.0, rerun=10.0)), encoding="utf-8")
    optimized.write_text(
        json.dumps(_evidence("streaming", fresh=10.0, rerun=1.0)), encoding="utf-8"
    )
    return legacy, optimized


def _evidence(name: str, *, fresh: float, rerun: float) -> dict[str, object]:
    archive = {
        "path": "C:/fixture/raw/2025/01/02/nsccl.20250102.i1.zip",
        "bytes": 1234,
        "sha256": "a" * 64,
        "zip_error": None,
        "zip_testzip": None,
    }
    fresh_runs = [_run(fresh) for _ in range(3)]
    rerun_runs = [_run(rerun)]
    implementation = {} if name == "legacy" else {"name": name}
    return {
        "configuration": {"fresh_runs": 3, "rerun_runs": 1},
        "date_filter": "20250102",
        "fresh_runs": fresh_runs,
        "implementation": implementation,
        "input_validation": {
            "after": [archive],
            "before": [archive],
            "inputs_unchanged_after_runs": True,
            "prevalidated": True,
        },
        "rerun_runs": rerun_runs,
        "summaries": {
            "fresh_runs": {
                "peak_aggregate_process_tree_rss_estimate_bytes": {"max": 400.0 if name == "legacy" else 100.0},
                "peak_process_tree_private_memory_estimate_bytes": {"max": 300.0 if name == "legacy" else 100.0},
                "wall_seconds": {"median": fresh},
            },
            "rerun_runs": {"wall_seconds": {"median": rerun}},
            "semantic_digest_variants": ["c" * 64],
            "semantic_outputs_identical": True,
        },
        "symbols": ["NIFTY"],
        "workers": 1,
    }


def _run(seconds: float) -> dict[str, object]:
    return {
        "semantic_output": {"row_count": 42},
        "valid": True,
        "wall_seconds": seconds,
    }


if __name__ == "__main__":
    unittest.main()
