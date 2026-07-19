"""Build the exactly eight-page LaTeX research paper and its evidence figures."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
RESULTS = ROOT / "research/module5_final_submission/results"
TEX = ROOT / "submission/NIFTY_VRP_Research_Memo.tex"
OUTPUT = ROOT / "submission/NIFTY_VRP_Research_Memo.pdf"
HIGHLIGHTS_TEX = ROOT / "submission/NIFTY_VRP_Research_Highlights.tex"
HIGHLIGHTS_OUTPUT = ROOT / "submission/NIFTY_VRP_Research_Highlights.pdf"
FIGURES = ROOT / "submission/figures"
BUILD = ROOT / "tmp/pdfs/nifty_vrp_latex"

BLUE = "#1D4ED8"
NAVY = "#0F172A"
RED = "#B91C1C"
GREEN = "#047857"
AMBER = "#B45309"
GREY = "#64748B"


def _style() -> None:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 7.2,
            "axes.titlesize": 8.2,
            "axes.labelsize": 7.2,
            "xtick.labelsize": 6.7,
            "ytick.labelsize": 6.7,
            "axes.edgecolor": "#CBD5E1",
            "axes.linewidth": 0.7,
            "grid.color": "#E2E8F0",
            "grid.linewidth": 0.6,
            "savefig.bbox": "tight",
            "savefig.pad_inches": 0.04,
            "pdf.fonttype": 42,
        }
    )


def _save(fig: plt.Figure, name: str) -> None:
    fig.savefig(
        FIGURES / name,
        format="pdf",
        transparent=False,
        metadata={
            "Creator": "Kd6669 deterministic research build",
            "CreationDate": None,
            "ModDate": None,
        },
    )
    plt.close(fig)


def build_figures() -> None:
    FIGURES.mkdir(parents=True, exist_ok=True)
    _style()

    # Page 3: conservative moneyness-by-horizon boundary from the full unconditional audit.
    horizons = np.array([60, 90, 120, 180])
    offsets = np.array([6, 5, 4, 3])
    fig, ax = plt.subplots(figsize=(5.9, 1.65))
    ax.plot(horizons, offsets, marker="o", lw=2, color=BLUE)
    ax.fill_between(horizons, 0, offsets, alpha=0.12, color=BLUE)
    for x, y in zip(horizons, offsets, strict=True):
        ax.annotate(f"ATM +/-{y}", (x, y), xytext=(0, 6), textcoords="offset points", ha="center")
    ax.axvline(180, color=RED, ls="--", lw=1)
    ax.set(xticks=horizons, yticks=range(0, 8), xlabel="Fixed exit horizon (minutes)", ylabel="Largest offset with >=99% strict path coverage")
    ax.set_title("Playable-universe boundary: exact fixed-contract paths")
    ax.grid(True)
    ax.spines[["top", "right"]].set_visible(False)
    _save(fig, "coverage_boundary.pdf")

    # Page 4: hypothesis ladder. Values are one-lot means from canonical closeout artifacts.
    labels = ["H1\nzero-cross\ncondor 60m", "H2\nq85-up\ncondor 60m", "H3\ninverse reversal\n180m", "H4\np85 gated fly\n60m"]
    gross = np.array([22.60, 102.73, 104.60, 292.42])
    net = np.array([-248.54, -169.32, -181.43, 52.06])
    x = np.arange(len(labels))
    fig, ax = plt.subplots(figsize=(6.1, 2.0))
    w = 0.34
    ax.bar(x - w / 2, gross, width=w, label="Gross mean", color=BLUE)
    ax.bar(x + w / 2, net, width=w, label="Net mean", color=np.where(net > 0, GREEN, RED))
    ax.axhline(0, color=NAVY, lw=0.8)
    ax.set_xticks(x, labels)
    ax.set_ylabel("Rs per one-lot completed trade")
    ax.set_title("Hypothesis ladder: costs reject H1-H3; H4 clears narrowly at one lot")
    ax.legend(frameon=False, ncol=2, loc="upper left")
    ax.grid(axis="y")
    ax.spines[["top", "right"]].set_visible(False)
    _save(fig, "hypothesis_ladder.pdf")

    # Page 6: capital curve and drawdown.
    equity = pd.read_csv(RESULTS / "curves/equity_curve.csv", parse_dates=["date"])
    fig, axes = plt.subplots(2, 1, figsize=(6.2, 2.7), sharex=True, gridspec_kw={"height_ratios": [2.0, 1.0], "hspace": 0.08})
    axes[0].plot(equity.date, equity.equity_rupees / 1e6, color=BLUE, lw=1.4)
    axes[0].set_ylabel("Equity (Rs mn)")
    axes[0].set_title("Frozen Rs 10 lakh historical capital path")
    axes[1].fill_between(equity.date, equity.drawdown_pct * 100, 0, color=RED, alpha=0.65)
    axes[1].set_ylabel("DD (%)")
    for ax in axes:
        ax.grid(True)
        ax.spines[["top", "right"]].set_visible(False)
    _save(fig, "equity_drawdown.pdf")

    # Page 6: annual net P&L and execution attribution.
    annual = pd.read_csv(RESULTS / "curves/annual_returns.csv")
    fig, axes = plt.subplots(1, 2, figsize=(6.2, 1.8))
    axes[0].bar(annual.year.astype(str), annual.net_pnl_rupees / 1000, color=[BLUE] * len(annual))
    axes[0].set(title="Calendar net P&L", ylabel="Rs 000")
    axes[0].grid(axis="y")
    summary = json.loads((RESULTS / "summary.json").read_text(encoding="utf-8"))
    gross_total = summary["metrics"]["gross_pnl_rupees"]
    charges = 21877.70010821564
    slip = summary["metrics"]["total_cost_rupees"] - charges
    axes[1].bar(["Gross", "Charges", "Slippage", "Net"], [gross_total, -charges, -slip, summary["metrics"]["net_profit_rupees"]], color=[BLUE, AMBER, RED, GREEN])
    axes[1].set(title="Gross-to-net attribution", ylabel="Rs")
    axes[1].grid(axis="y")
    for ax in axes:
        ax.spines[["top", "right"]].set_visible(False)
    _save(fig, "annual_costs.pdf")

    # Page 7: execution decay and capacity.
    decay = pd.read_csv(RESULTS / "robustness/execution_decay.csv")
    capacity = pd.read_csv(RESULTS / "robustness/capacity_curve.csv")
    capacity["net_per_lot"] = capacity.net_pnl_rupees / capacity.lots
    fig, axes = plt.subplots(1, 2, figsize=(6.2, 1.85))
    axes[0].plot(decay.slippage_multiplier, decay.net_pnl_rupees / 1000, marker="o", color=RED)
    axes[0].axhline(0, color=NAVY, lw=0.8)
    axes[0].axvline(float(decay.break_even_slippage_multiplier.iloc[0]), color=GREY, ls="--", lw=0.9)
    axes[0].set(title="Execution decay", xlabel="Slippage multiplier", ylabel="Net Rs 000")
    axes[1].plot(capacity.lots, capacity.net_per_lot, color=GREEN, lw=1.4)
    axes[1].set(title="Equal-size capacity diagnostic", xlabel="Lots per signal", ylabel="Aggregate net / lot (Rs)")
    for ax in axes:
        ax.grid(True)
        ax.spines[["top", "right"]].set_visible(False)
    _save(fig, "execution_capacity.pdf")

    # Page 7: regime/break/event diagnostics.
    structural = pd.read_csv(RESULTS / "robustness/nov2024_structural_break.csv")
    events = pd.read_csv(RESULTS / "robustness/event_conditioning.csv")
    pair = events[events.group.isin(["event_week", "non_event_week"])]
    fig, axes = plt.subplots(1, 3, figsize=(6.2, 1.85))
    axes[0].bar(["Pre", "Post"], structural.set_index("group").loc[["pre_2024_11_20", "post_2024_11_20"], "mean_net_pnl_rupees"], color=[BLUE, AMBER])
    axes[0].set_title("20-Nov-2024 split")
    axes[0].set_ylabel("Mean sized net (Rs)")
    axes[1].bar(["Non-event", "Event"], pair.set_index("group").loc[["non_event_week", "event_week"], "mean_net_pnl_rupees"], color=[GREEN, RED])
    axes[1].set_title("Event +/-5 days")
    axes[2].bar(["Discovery", "Holdout"], [0.337, 0.214], color=[BLUE, AMBER])
    axes[2].errorbar([1], [0.214], yerr=[[0.214 - (-0.066)], [0.484 - 0.214]], fmt="none", ecolor=NAVY, capsize=3)
    axes[2].axhline(0, color=NAVY, lw=0.8)
    axes[2].set_title("Composite-score Spearman")
    for ax in axes:
        ax.grid(axis="y")
        ax.spines[["top", "right"]].set_visible(False)
    _save(fig, "robustness_panels.pdf")


def _latex_executable() -> str:
    found = shutil.which("pdflatex")
    if found:
        return found
    candidate = Path.home() / "AppData/Local/Programs/MiKTeX/miktex/bin/x64/pdflatex.exe"
    if candidate.exists():
        return str(candidate)
    raise FileNotFoundError("pdflatex was not found; install MiKTeX or TeX Live")


def _compile(tex: Path, output: Path, executable: str) -> None:
    if not tex.exists():
        raise FileNotFoundError(tex)
    command = [
        executable,
        "-interaction=nonstopmode",
        "-halt-on-error",
        f"-output-directory={BUILD}",
        str(tex),
    ]
    if "miktex" in executable.lower():
        command.insert(1, "--disable-installer")
    for _ in range(2):
        environment = os.environ.copy()
        environment.update({"SOURCE_DATE_EPOCH": "1784428200", "FORCE_SOURCE_DATE": "1"})
        result = subprocess.run(
            command,
            cwd=ROOT,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode:
            log = BUILD / f"{tex.stem}.log"
            tail = log.read_text(encoding="utf-8", errors="replace")[-8000:] if log.exists() else result.stdout[-8000:]
            raise RuntimeError(f"LaTeX compilation failed:\n{tail}")
    shutil.copy2(BUILD / f"{tex.stem}.pdf", output)
    print(output)


def build() -> Path:
    build_figures()
    BUILD.mkdir(parents=True, exist_ok=True)
    executable = _latex_executable()
    _compile(TEX, OUTPUT, executable)
    _compile(HIGHLIGHTS_TEX, HIGHLIGHTS_OUTPUT, executable)
    return OUTPUT


if __name__ == "__main__":
    build()
