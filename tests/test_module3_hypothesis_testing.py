from __future__ import annotations

from pathlib import Path

from research.module3_hypothesis_testing.closeout import (
    DECISION,
    build_closeout,
    render_report,
    verify_manifest,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_closed_packet_covers_every_hypothesis_stage() -> None:
    summary = build_closeout(REPO_ROOT)
    assert summary["decision"] == DECISION
    assert summary["status"] == "closed"
    tests = summary["tests"]
    assert len(tests["tail_crossing_level_direction"]["cells"]) == 12
    assert len(tests["velocity_acceleration"]["minute_grid_correlations"]) == 12
    assert tests["alternative_structures_horizons"]["cells_tested"] == 48
    assert tests["alternative_structures_horizons"]["positive_net_mean_cells"] == 0
    assert len(tests["tail_mean_reversion"]["cells"]) == 6
    assert len(tests["unified_180min"]["cells"]) == 32
    assert tests["unified_180min"]["credible_positive_cell_count"] == 0


def test_material_180min_gross_is_preserved_without_promoting_it() -> None:
    summary = build_closeout(REPO_ROOT)
    cells = summary["tests"]["unified_180min"]["positive_gross_cells_at_least_65_rupees"]
    assert len(cells) == 5
    assert all(cell["gross_mean_rupees"] >= 65 for cell in cells)
    positive_net = [cell for cell in cells if cell["net_mean_rupees"] > 0]
    assert [(cell["mapping"], cell["signal_name"], cell["trades"]) for cell in positive_net] == [
        ("inverse", "reversal_top", 96)
    ]
    assert positive_net[0]["coverage"] < 0.80
    assert positive_net[0]["expected_events"] == 191
    assert positive_net[0]["trades"] == 96
    assert positive_net[0]["unevaluated_events"] == 95
    assert summary["data_coverage_boundary"]["no_extrapolation"] is True


def test_report_and_manifest_are_reproducible() -> None:
    summary = build_closeout(REPO_ROOT)
    report = render_report(summary)
    assert "+₹22.60 gross" in report
    assert "selected gross means rise above ₹65" in report
    assert "cannot support an unbiased test beyond 180" in report
    assert verify_manifest(REPO_ROOT) == []
