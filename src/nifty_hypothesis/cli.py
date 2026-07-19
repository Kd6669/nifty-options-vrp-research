"""Command-line interface for hypothesis-formulation evidence."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .config import ResearchConfig
from .contracts import STAGE_ORDER, ArtifactSet
from .pipeline import build_manifest, run_pipeline, select_stages, stage_plan
from .validation import validate_inputs, validate_outputs


DEFAULT_CONFIG = Path("research/phase2/hypothesis_formulation.example.json")


def _stage_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--from-stage", choices=STAGE_ORDER)
    parser.add_argument("--through-stage", choices=STAGE_ORDER)
    parser.add_argument("--stage", action="append", choices=STAGE_ORDER, default=[])


def _selected(args: argparse.Namespace) -> tuple[str, ...]:
    return select_stages(
        start=args.from_stage,
        through=args.through_stage,
        only=tuple(args.stage),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan = subparsers.add_parser("plan", help="Show ordered stages and artifact paths.")
    _stage_flags(plan)

    validate = subparsers.add_parser("validate", help="Validate inputs and existing outputs.")
    _stage_flags(validate)
    validate.add_argument("--hash", action="store_true", dest="include_hash")

    run = subparsers.add_parser("run", help="Run the selected evidence stages.")
    _stage_flags(run)
    run.add_argument(
        "--resume",
        action="store_true",
        help="Reuse a stage only when every declared output already exists.",
    )

    closeout = subparsers.add_parser(
        "closeout",
        help="Rebuild the frozen final hypothesis closeout from existing evidence.",
    )
    closeout.add_argument(
        "--rebuild-curve",
        action="store_true",
        help="Rebuild the VRP-curve stage before the final closeout.",
    )

    subparsers.add_parser("manifest", help="Hash and row-count all current outputs.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = ResearchConfig.from_json(args.config)
    artifacts = ArtifactSet.from_config(config)

    if args.command == "plan":
        payload = {"configuration": config.as_dict(), "stages": stage_plan(config, _selected(args))}
    elif args.command == "validate":
        stages = _selected(args)
        payload = {
            "inputs": validate_inputs(config),
            "outputs": validate_outputs(
                artifacts,
                stages=stages,
                include_hash=args.include_hash,
            ),
        }
    elif args.command == "run":
        payload = {
            "configuration": config.as_dict(),
            "results": run_pipeline(config, _selected(args), resume=args.resume),
        }
    elif args.command == "closeout":
        stages = (
            ("curve_crossings", "hypothesis_closeout")
            if args.rebuild_curve
            else ("hypothesis_closeout",)
        )
        payload = {
            "configuration": config.as_dict(),
            "results": run_pipeline(config, stages),
        }
    elif args.command == "manifest":
        payload = build_manifest(config, artifacts)
    else:  # pragma: no cover - argparse enforces the command choices.
        raise AssertionError(args.command)
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
