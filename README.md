# Batch Data Quality Checker

A Python script using Pandas that runs **automated data quality checks** on flat-file batch exports (CSV / JSON). Detects null rates, type mismatches, duplicate rows, value range violations, allowed-value violations, and regex pattern failures. Outputs a concise summary report per run.

---

## Features

| Check | Description |
|-------|-------------|
| **Null Rate** | Flag columns exceeding null % threshold |
| **Type Check** | Validate integer, float, date, email types |
| **Duplicates** | Detect duplicate composite keys |
| **Value Range** | Catch out-of-range numeric values (min/max) |
| **Allowed Values** | Enforce categorical constraints |
| **Regex Pattern** | Validate format (e.g. IDs, codes) |
| **Auto-Profile** | Profile any file without a rules config |

---

## Project Structure

```
batch-data-quality-checker/
├── data_quality_checker.py      # Main checker script
├── requirements.txt
├── rules/
│   ├── employees_rules.json     # Sample rules for employee CSV
│   └── products_rules.json      # Sample rules for product JSON
├── sample_data/
│   ├── employees.csv            # Sample CSV with intentional errors
│   └── products.json            # Sample JSON with intentional errors
└── tests/
    └── test_data_quality_checker.py   # 22 unit tests
```

---

## Quickstart

```bash
git clone https://github.com/jeeva-s0604/batch-data-quality-checker.git
cd batch-data-quality-checker
pip install -r requirements.txt

# Rule-based check
python data_quality_checker.py --input sample_data/employees.csv --rules rules/employees_rules.json

# Auto-profile mode (no rules needed)
python data_quality_checker.py --input sample_data/employees.csv --profile

# JSON input
python data_quality_checker.py --input sample_data/products.json --rules rules/products_rules.json

# Run tests
python -m pytest tests/ -v
```

---

## Rules Config Format

```json
{
  "row_count": { "min_rows": 100, "max_rows": 10000 },
  "duplicates": { "key_columns": ["order_id"], "max_dup_pct": 0.0 },
  "columns": [
    { "name": "order_id",    "max_null_pct": 0.0,  "type": "integer" },
    { "name": "email",       "max_null_pct": 5.0,  "type": "email" },
    { "name": "status",      "allowed_values": ["PENDING","COMPLETED","FAILED"] },
    { "name": "amount",      "min_val": 0, "max_val": 1000000 },
    { "name": "product_code","regex": "^PROD-\\d{4}$" }
  ]
}
```

---

## Sample Output

```
======================================================================
  DATA QUALITY REPORT  |  sample_data/employees.csv
  Rows: 11   Cols: 7   Run at: 2025-01-15T06:30:00
======================================================================
  ✅  row_count:  Row count 11 within bounds
  ❌  duplicates [emp_id]:  1 duplicate rows (9.09%) on [emp_id]
  ✅  null_rate [name]:  Null rate 0.0% (≤ allowed 0.0%)
  ❌  null_rate [email]:  Null rate 9.09% (exceeds allowed 5.0%)
  ❌  type_check [email]:  1 values not castable to email
  ❌  value_range [salary]:  1 values < 0
  ✅  allowed_values [status]:  All values in allowed set
----------------------------------------------------------------------
  Status: FAIL ❌   Errors: 4   Warnings: 0
======================================================================
```

Reports saved as CSV + JSON to `reports/` folder.

---

## Tech Stack

`Python 3.12` · `Pandas` · `pytest`

---

*Built by Jeeva S — [linkedin.com/in/jeeva-s0604](https://linkedin.com/in/jeeva-s0604)*
