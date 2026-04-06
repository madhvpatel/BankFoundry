# Intelligence Validation Framework

This repository now includes three deterministic validation layers for intelligence recommendations:

1. Data quality checks (`app/intelligence/quality_checks.py`)
2. Drift checks (`app/intelligence/drift_checks.py`)
3. Recovery-rate calibration and backtest utilities (`app/intelligence/calibration.py`, `app/intelligence/experiments.py`)

## Runtime integration

- `run_intelligence()` executes data quality and drift checks before ranking recommendations.
- Controls in `config.py`:
  - `INTELLIGENCE_ENABLE_DQ_CHECKS`
  - `INTELLIGENCE_ENABLE_DRIFT_CHECKS`

## Test suite

Run:

```bash
/Users/madhavpatel/.pyenv/versions/3.10.14/bin/python -m unittest discover -s tests -p 'test_*.py'
```

Covered scenarios:

- FAILED vs FAILURE label normalization
- recommendation contract integrity
- data quality issue detection
- drift signal detection
- recovery-rate calibration and backtest scaffolding

