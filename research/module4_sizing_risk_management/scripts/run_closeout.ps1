$ErrorActionPreference = "Stop"

python -m research.module4_sizing_risk_management.run build
python -m research.module4_sizing_risk_management.run verify
python -m pytest tests/test_module4_sizing_risk_management.py -q
