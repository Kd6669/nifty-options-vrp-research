.PHONY: lint test audit-sample reproduce submission bundle verify-bundle

lint:
	python -m ruff check .
	python -m compileall -q src tests tools research

test:
	python -m pytest -q

audit-sample:
	python tools/audit_sample.py samples/nifty_gold_sample.parquet samples/nifty_gold_sample.manifest.json

submission:
	python -m research.module5_final_submission.run build
	python -m research.module5_final_submission.run verify

reproduce:
	powershell -ExecutionPolicy Bypass -File scripts/reproduce_compact.ps1

bundle:
	python -m tools.team_bundle build

verify-bundle:
	python -m tools.team_bundle verify
