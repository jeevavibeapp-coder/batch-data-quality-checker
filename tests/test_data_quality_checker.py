"""
tests/test_data_quality_checker.py
Run with: python -m pytest tests/ -v
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
import pandas as pd
from data_quality_checker import (
    check_nulls, check_type, check_duplicates, check_value_range,
    check_allowed_values, check_regex_pattern, auto_profile,
    run_checks, build_report, load_file,
)


@pytest.fixture
def clean_df():
    return pd.DataFrame({
        "id":     [1, 2, 3, 4, 5],
        "name":   ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "email":  ["a@x.com", "b@x.com", "c@x.com", "d@x.com", "e@x.com"],
        "salary": [50000, 60000, 70000, 80000, 90000],
        "status": ["ACTIVE", "ACTIVE", "INACTIVE", "ACTIVE", "ACTIVE"],
    })


@pytest.fixture
def dirty_df():
    return pd.DataFrame({
        "id":     [1, 2, 2, 3, None],
        "name":   ["Alice", "Bob", "Bob", None, "Eve"],
        "email":  ["a@x.com", "not-email", None, "d@x.com", "e@x.com"],
        "salary": [50000, -100, 70000, 2000000, 90000],
        "status": ["ACTIVE", "ACTIVE", "UNKNOWN", "ACTIVE", "ACTIVE"],
    })


class TestNullCheck:
    def test_no_nulls(self, clean_df):
        r = check_nulls(clean_df, "name", max_null_pct=0.0)
        assert r.severity == "INFO"
        assert r.count == 0

    def test_null_exceeds_threshold(self, dirty_df):
        r = check_nulls(dirty_df, "name", max_null_pct=0.0)
        assert r.severity == "ERROR"
        assert r.count == 1

    def test_null_within_tolerance(self, dirty_df):
        r = check_nulls(dirty_df, "email", max_null_pct=30.0)
        assert r.severity == "INFO"


class TestTypeCheck:
    def test_valid_integer_column(self, clean_df):
        r = check_type(clean_df, "id", "integer")
        assert r.severity == "INFO"
        assert r.count == 0

    def test_invalid_email_detected(self, dirty_df):
        r = check_type(dirty_df, "email", "email")
        assert r.severity == "ERROR"
        assert r.count >= 1

    def test_valid_numeric(self, clean_df):
        r = check_type(clean_df, "salary", "numeric")
        assert r.severity == "INFO"


class TestDuplicateCheck:
    def test_no_duplicates(self, clean_df):
        r = check_duplicates(clean_df, ["id"], max_dup_pct=0.0)
        assert r.severity == "INFO"
        assert r.count == 0

    def test_duplicates_detected(self, dirty_df):
        r = check_duplicates(dirty_df, ["id"], max_dup_pct=0.0)
        assert r.severity == "ERROR"
        assert r.count >= 1

    def test_duplicates_within_tolerance(self, dirty_df):
        r = check_duplicates(dirty_df, ["id"], max_dup_pct=50.0)
        assert r.severity == "INFO"


class TestValueRange:
    def test_all_in_range(self, clean_df):
        r = check_value_range(clean_df, "salary", min_val=0, max_val=200000)
        assert r.severity == "INFO"

    def test_negative_value_detected(self, dirty_df):
        r = check_value_range(dirty_df, "salary", min_val=0)
        assert r.severity == "ERROR"
        assert r.count >= 1

    def test_exceeds_max(self, dirty_df):
        r = check_value_range(dirty_df, "salary", max_val=100000)
        assert r.severity == "ERROR"


class TestAllowedValues:
    def test_valid_values(self, clean_df):
        r = check_allowed_values(clean_df, "status", ["ACTIVE", "INACTIVE", "TERMINATED"])
        assert r.severity == "INFO"
        assert r.count == 0

    def test_invalid_value_detected(self, dirty_df):
        r = check_allowed_values(dirty_df, "status", ["ACTIVE", "INACTIVE"])
        assert r.severity == "ERROR"
        assert r.count == 1


class TestRegexPattern:
    def test_valid_emails(self, clean_df):
        r = check_regex_pattern(clean_df, "email", r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
        assert r.severity == "INFO"

    def test_invalid_pattern_detected(self, dirty_df):
        r = check_regex_pattern(dirty_df, "email", r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
        assert r.severity == "ERROR"


class TestAutoProfile:
    def test_profile_generates_issues(self, dirty_df):
        issues = auto_profile(dirty_df)
        assert len(issues) > 0
        checks = {i.check for i in issues}
        assert "null_rate" in checks
        assert "duplicates" in checks


class TestRunChecks:
    def test_clean_data_passes(self, clean_df):
        rules = {
            "row_count": {"min_rows": 1},
            "duplicates": {"key_columns": ["id"], "max_dup_pct": 0.0},
            "columns": [
                {"name": "id",     "max_null_pct": 0.0, "type": "integer"},
                {"name": "email",  "max_null_pct": 0.0, "type": "email"},
                {"name": "salary", "min_val": 0, "max_val": 500000},
                {"name": "status", "allowed_values": ["ACTIVE", "INACTIVE", "TERMINATED"]},
            ],
        }
        issues = run_checks(clean_df, rules)
        report = build_report("test.csv", clean_df, issues)
        assert report.passed

    def test_dirty_data_fails(self, dirty_df):
        rules = {
            "columns": [
                {"name": "salary", "min_val": 0},
                {"name": "status", "allowed_values": ["ACTIVE", "INACTIVE"]},
            ]
        }
        issues = run_checks(dirty_df, rules)
        report = build_report("test.csv", dirty_df, issues)
        assert not report.passed

    def test_missing_column_flagged(self, clean_df):
        rules = {"columns": [{"name": "nonexistent_col", "max_null_pct": 0.0}]}
        issues = run_checks(clean_df, rules)
        assert any(i.severity == "ERROR" and "not found" in i.message for i in issues)


class TestLoadFile:
    def test_load_csv(self, tmp_path):
        p = tmp_path / "test.csv"
        p.write_text("a,b\n1,2\n3,4\n")
        df = load_file(str(p))
        assert len(df) == 2
        assert list(df.columns) == ["a", "b"]

    def test_load_json(self, tmp_path):
        p = tmp_path / "test.json"
        p.write_text('[{"a":1,"b":2},{"a":3,"b":4}]')
        df = load_file(str(p))
        assert len(df) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
