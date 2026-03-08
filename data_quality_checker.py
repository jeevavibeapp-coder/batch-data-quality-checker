"""
data_quality_checker.py
========================
Automated data quality checks on flat-file batch exports (CSV / JSON).
Checks: null rate, type mismatches, duplicate rows, value range violations,
        string pattern violations, allowed-value set checks, and row count.
Outputs a concise summary report per run.

Usage:
    python data_quality_checker.py --input sample_data/employees.csv --rules rules/employees_rules.json
    python data_quality_checker.py --input sample_data/products.json  --rules rules/products_rules.json
    python data_quality_checker.py --input sample_data/employees.csv  --profile   # auto-profile mode
"""

import csv
import json
import re
import sys
import argparse
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    print("pandas not installed. Run: pip install pandas")
    sys.exit(1)

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt= "%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class QualityIssue:
    check:    str
    column:   Optional[str]
    severity: str           # ERROR | WARN | INFO
    message:  str
    count:    int = 0
    pct:      float = 0.0


@dataclass
class QualityReport:
    file:         str
    run_at:       str
    row_count:    int
    col_count:    int
    issues:       list[QualityIssue] = field(default_factory=list)

    @property
    def errors(self) -> list[QualityIssue]:
        return [i for i in self.issues if i.severity == "ERROR"]

    @property
    def warnings(self) -> list[QualityIssue]:
        return [i for i in self.issues if i.severity == "WARN"]

    @property
    def passed(self) -> bool:
        return len(self.errors) == 0


# ── File loader ────────────────────────────────────────────────────────────────

def load_file(path: str) -> pd.DataFrame:
    ext = Path(path).suffix.lower()
    log.info("Loading %s …", path)
    if ext == ".csv":
        df = pd.read_csv(path)
    elif ext in (".json", ".jsonl"):
        try:
            df = pd.read_json(path)
        except ValueError:
            df = pd.read_json(path, lines=True)
    else:
        raise ValueError(f"Unsupported file type: {ext}. Supported: .csv, .json, .jsonl")
    log.info("Loaded %d rows × %d columns", len(df), len(df.columns))
    return df


# ── Individual checks ─────────────────────────────────────────────────────────

def check_row_count(df: pd.DataFrame, rule: dict) -> Optional[QualityIssue]:
    n         = len(df)
    min_rows  = rule.get("min_rows")
    max_rows  = rule.get("max_rows")
    if min_rows and n < min_rows:
        return QualityIssue("row_count", None, "ERROR",
                            f"Row count {n} < min {min_rows}", count=n)
    if max_rows and n > max_rows:
        return QualityIssue("row_count", None, "WARN",
                            f"Row count {n} > max {max_rows}", count=n)
    return QualityIssue("row_count", None, "INFO",
                        f"Row count {n} within bounds", count=n)


def check_nulls(df: pd.DataFrame, col: str, max_null_pct: float) -> QualityIssue:
    total     = len(df)
    null_count = df[col].isna().sum()
    null_pct   = round(null_count / total * 100, 2) if total > 0 else 0.0
    sev = "ERROR" if null_pct > max_null_pct else "INFO"
    return QualityIssue(
        check="null_rate",
        column=col,
        severity=sev,
        message=f"Null rate {null_pct}% ({'exceeds' if sev=='ERROR' else '≤'} allowed {max_null_pct}%)",
        count=int(null_count),
        pct=null_pct,
    )


def check_type(df: pd.DataFrame, col: str, expected_type: str) -> QualityIssue:
    """Attempt to coerce column and count failures."""
    series    = df[col].dropna()
    bad_count = 0

    if expected_type in ("int", "integer"):
        bad_count = (~series.apply(lambda v: str(v).lstrip("-").isdigit())).sum()
    elif expected_type in ("float", "numeric", "double"):
        def _is_float(v):
            try: float(v); return True
            except: return False
        bad_count = (~series.apply(_is_float)).sum()
    elif expected_type in ("date",):
        def _is_date(v):
            try:
                datetime.strptime(str(v), "%Y-%m-%d"); return True
            except: return False
        bad_count = (~series.apply(_is_date)).sum()
    elif expected_type == "email":
        pattern   = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
        bad_count = (~series.apply(lambda v: bool(pattern.match(str(v))))).sum()

    sev = "ERROR" if bad_count > 0 else "INFO"
    return QualityIssue(
        check="type_check",
        column=col,
        severity=sev,
        message=f"{bad_count} values not castable to {expected_type}" if bad_count else f"All values valid {expected_type}",
        count=int(bad_count),
        pct=round(bad_count / len(series) * 100, 2) if len(series) > 0 else 0.0,
    )


def check_duplicates(df: pd.DataFrame, key_cols: list[str], max_dup_pct: float) -> QualityIssue:
    subset     = df[key_cols]
    total      = len(subset)
    dup_count  = int(subset.duplicated().sum())
    dup_pct    = round(dup_count / total * 100, 2) if total > 0 else 0.0
    sev = "ERROR" if dup_pct > max_dup_pct else "INFO"
    return QualityIssue(
        check="duplicates",
        column="+".join(key_cols),
        severity=sev,
        message=f"{dup_count} duplicate rows ({dup_pct}%) on [{', '.join(key_cols)}]",
        count=dup_count,
        pct=dup_pct,
    )


def check_value_range(df: pd.DataFrame, col: str, min_val=None, max_val=None) -> QualityIssue:
    series = pd.to_numeric(df[col], errors="coerce").dropna()
    violations = 0
    details    = []
    if min_val is not None:
        below = int((series < min_val).sum())
        violations += below
        if below: details.append(f"{below} values < {min_val}")
    if max_val is not None:
        above = int((series > max_val).sum())
        violations += above
        if above: details.append(f"{above} values > {max_val}")

    sev = "ERROR" if violations > 0 else "INFO"
    return QualityIssue(
        check="value_range",
        column=col,
        severity=sev,
        message="; ".join(details) if details else f"All values in [{min_val}, {max_val}]",
        count=violations,
        pct=round(violations / len(series) * 100, 2) if len(series) > 0 else 0.0,
    )


def check_allowed_values(df: pd.DataFrame, col: str, allowed: list) -> QualityIssue:
    allowed_set = set(str(v) for v in allowed)
    series      = df[col].dropna().astype(str)
    invalid     = int((~series.isin(allowed_set)).sum())
    sev = "ERROR" if invalid > 0 else "INFO"
    return QualityIssue(
        check="allowed_values",
        column=col,
        severity=sev,
        message=f"{invalid} values not in allowed set {allowed}" if invalid else "All values in allowed set",
        count=invalid,
        pct=round(invalid / len(series) * 100, 2) if len(series) > 0 else 0.0,
    )


def check_regex_pattern(df: pd.DataFrame, col: str, pattern: str) -> QualityIssue:
    compiled = re.compile(pattern)
    series   = df[col].dropna().astype(str)
    invalid  = int((~series.apply(lambda v: bool(compiled.match(v)))).sum())
    sev = "ERROR" if invalid > 0 else "INFO"
    return QualityIssue(
        check="regex_pattern",
        column=col,
        severity=sev,
        message=f"{invalid} values don't match pattern '{pattern}'" if invalid else "All values match pattern",
        count=invalid,
        pct=round(invalid / len(series) * 100, 2) if len(series) > 0 else 0.0,
    )


# ── Auto-profiler (no rules needed) ──────────────────────────────────────────

def auto_profile(df: pd.DataFrame) -> list[QualityIssue]:
    """Generate a basic data profile without any rule file."""
    issues = []
    for col in df.columns:
        null_count = int(df[col].isna().sum())
        null_pct   = round(null_count / len(df) * 100, 2) if len(df) > 0 else 0.0
        sev = "WARN" if null_pct > 5 else "INFO"
        issues.append(QualityIssue("null_rate", col, sev,
                                   f"Null rate: {null_pct}%  ({null_count}/{len(df)})",
                                   count=null_count, pct=null_pct))

        # Unique ratio
        unique_pct = round(df[col].nunique() / len(df) * 100, 2) if len(df) > 0 else 0.0
        issues.append(QualityIssue("unique_ratio", col, "INFO",
                                   f"Unique values: {df[col].nunique()} ({unique_pct}%)",
                                   count=df[col].nunique(), pct=unique_pct))

    dup_count = int(df.duplicated().sum())
    dup_pct   = round(dup_count / len(df) * 100, 2) if len(df) > 0 else 0.0
    issues.append(QualityIssue("duplicates", None,
                               "WARN" if dup_count > 0 else "INFO",
                               f"Full-row duplicates: {dup_count} ({dup_pct}%)",
                               count=dup_count, pct=dup_pct))
    return issues


# ── Main runner ───────────────────────────────────────────────────────────────

def run_checks(df: pd.DataFrame, rules: dict) -> list[QualityIssue]:
    issues = []

    # Row-count check
    if "row_count" in rules:
        issue = check_row_count(df, rules["row_count"])
        if issue: issues.append(issue)

    # Duplicate check
    if "duplicates" in rules:
        dup_cfg = rules["duplicates"]
        issues.append(check_duplicates(
            df,
            key_cols    = dup_cfg.get("key_columns", list(df.columns)),
            max_dup_pct = dup_cfg.get("max_dup_pct", 0.0),
        ))

    # Per-column checks
    for col_rule in rules.get("columns", []):
        col = col_rule["name"]
        if col not in df.columns:
            issues.append(QualityIssue("missing_column", col, "ERROR", f"Column '{col}' not found in file"))
            continue

        if "max_null_pct" in col_rule:
            issues.append(check_nulls(df, col, col_rule["max_null_pct"]))

        if "type" in col_rule:
            issues.append(check_type(df, col, col_rule["type"]))

        if "min_val" in col_rule or "max_val" in col_rule:
            issues.append(check_value_range(df, col, col_rule.get("min_val"), col_rule.get("max_val")))

        if "allowed_values" in col_rule:
            issues.append(check_allowed_values(df, col, col_rule["allowed_values"]))

        if "regex" in col_rule:
            issues.append(check_regex_pattern(df, col, col_rule["regex"]))

    return issues


def build_report(file: str, df: pd.DataFrame, issues: list[QualityIssue]) -> QualityReport:
    return QualityReport(
        file      = file,
        run_at    = datetime.now().isoformat(timespec="seconds"),
        row_count = len(df),
        col_count = len(df.columns),
        issues    = issues,
    )


# ── Output ────────────────────────────────────────────────────────────────────

ICONS = {"ERROR": "❌", "WARN": "⚠️ ", "INFO": "✅"}


def print_report(report: QualityReport):
    print("\n" + "=" * 70)
    print(f"  DATA QUALITY REPORT  |  {report.file}")
    print(f"  Rows: {report.row_count}   Cols: {report.col_count}   Run at: {report.run_at}")
    print("=" * 70)
    for issue in report.issues:
        icon = ICONS.get(issue.severity, "  ")
        col  = f" [{issue.column}]" if issue.column else ""
        print(f"  {icon}  {issue.check}{col}:  {issue.message}")
    print("-" * 70)
    errors   = len(report.errors)
    warnings = len(report.warnings)
    status   = "PASS ✅" if report.passed else "FAIL ❌"
    print(f"  Status: {status}   Errors: {errors}   Warnings: {warnings}")
    print("=" * 70 + "\n")


def save_report(report: QualityReport, output_dir: str):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    stem = Path(report.file).stem
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")

    # JSON
    json_path = Path(output_dir) / f"dq_{stem}_{ts}.json"
    with open(json_path, "w") as f:
        json.dump({
            "file":      report.file,
            "run_at":    report.run_at,
            "row_count": report.row_count,
            "col_count": report.col_count,
            "status":    "PASS" if report.passed else "FAIL",
            "errors":    len(report.errors),
            "warnings":  len(report.warnings),
            "issues":    [asdict(i) for i in report.issues],
        }, f, indent=2)

    # CSV
    csv_path = Path(output_dir) / f"dq_{stem}_{ts}.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["check","column","severity","message","count","pct"])
        w.writeheader()
        w.writerows([asdict(i) for i in report.issues])

    log.info("Reports saved → %s  |  %s", json_path, csv_path)
    return str(json_path), str(csv_path)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Batch Data Quality Checker")
    parser.add_argument("--input",   required=True, help="Path to CSV or JSON file")
    parser.add_argument("--rules",   default=None,  help="Path to JSON rules file")
    parser.add_argument("--output",  default="reports", help="Output directory")
    parser.add_argument("--profile", action="store_true", help="Auto-profile mode (no rules needed)")
    args = parser.parse_args()

    df = load_file(args.input)

    if args.profile or not args.rules:
        log.info("Running in auto-profile mode…")
        issues = auto_profile(df)
    else:
        with open(args.rules) as f:
            rules = json.load(f)
        log.info("Running rule-based checks (%d column rules)…", len(rules.get("columns", [])))
        issues = run_checks(df, rules)

    report = build_report(args.input, df, issues)
    print_report(report)
    save_report(report, args.output)

    sys.exit(0 if report.passed else 1)


if __name__ == "__main__":
    main()
