import os
import glob
import polars as pl
import sys
import logging
from pathlib import Path
from typing import List, Any
from datetime import datetime


class Validator:
    def __init__(self, df: pl.DataFrame, table_name: str):
        self.df = df
        self.table_name = table_name
        self.results = []

    def log(self, rule: str, passed: bool, details: Any = None):
        self.results.append({
            "table": self.table_name,
            "rule": rule,
            "passed": passed,
            "details": details,
        })

    def check_not_null(self, column: str):
        if column not in self.df.columns:
            self.log(f"{column} not null", False, "column missing")
            return
        null_count = self.df[column].null_count()
        self.log(f"{column} not null", null_count == 0, f"{null_count} nulls")

    def check_unique(self, column: str):
        if column not in self.df.columns:
            self.log(f"{column} unique", False, "column missing")
            return
        total = self.df.height
        unique = self.df[column].n_unique()
        self.log(f"{column} unique", total == unique, f"{total - unique} duplicates")

    def check_regex(self, column: str, pattern: str):
        if column not in self.df.columns:
            self.log(f"{column} regex {pattern}", False, "column missing")
            return
        s = self.df[column].cast(pl.Utf8)
        mask = s.str.contains(pattern)
        invalid_count = (~mask.fill_null(False)).sum() + s.is_null().sum()
        self.log(f"{column} regex {pattern}", invalid_count == 0, f"{invalid_count} invalid")

    def check_in_set(self, column: str, valid_set: List[str]):
        if column not in self.df.columns:
            self.log(f"{column} in {valid_set}", False, "column missing")
            return
        invalid = self.df.filter(~self.df[column].is_in(valid_set) | self.df[column].is_null())
        self.log(f"{column} in {valid_set}", invalid.is_empty(), f"{invalid.height} invalid")

    def check_range(self, column: str, min_val: float, max_val: float):
        if column not in self.df.columns:
            self.log(f"{column} between {min_val} and {max_val}", False, "column missing")
            return
        casted = self.df[column].cast(pl.Float64)
        non_numeric = ((self.df[column].is_not_null()) & (casted.is_null())).sum()
        out_of_range = ((casted < min_val) | (casted > max_val)).sum()
        invalid_count = int(non_numeric) + int(out_of_range)
        self.log(
            f"{column} between {min_val} and {max_val}",
            invalid_count == 0,
            f"{invalid_count} invalid ({non_numeric} non-numeric, {out_of_range} out-of-range)",
        )

    def report(self) -> pl.DataFrame:
        return pl.DataFrame(self.results)



def validate_parks(df: pl.DataFrame) -> pl.DataFrame:
    v = Validator(df, "parks")
    if "latitude" in v.df.columns:
        v.df = v.df.with_columns(pl.col("latitude").cast(pl.Float64))
    if "longitude" in v.df.columns:
        v.df = v.df.with_columns(pl.col("longitude").cast(pl.Float64))


    if "parkCode" in v.df.columns:
        vals = [
            (s.strip().upper() if (s is not None and isinstance(s, str)) else None)
            for s in v.df["parkCode"].to_list()
        ]
        v.df = v.df.with_columns(pl.Series("parkCode", vals))

    v.check_not_null("id")
    v.check_unique("id")
    v.check_regex("parkCode", r"^[A-Z]{4}$")
    v.check_range("latitude", -90, 90)
    v.check_range("longitude", -180, 180)
    v.check_not_null("_ingestion_timestamp")
    v.check_unique("_record_id")
    return v.report()


def validate_alerts(df: pl.DataFrame) -> pl.DataFrame:
    v = Validator(df, "alerts")
    if "parkCode" in v.df.columns:
        vals = [
            (s.strip().upper() if (s is not None and isinstance(s, str)) else None)
            for s in v.df["parkCode"].to_list()
        ]
        v.df = v.df.with_columns(pl.Series("parkCode", vals))
    v.check_not_null("id")
    v.check_unique("id")
    v.check_in_set("category", ["Information", "Caution", "Danger", "Park Closure"])
    v.check_regex("parkCode", r"^[A-Z]{4}$")
    v.check_unique("_record_id")
    return v.report()


def validate_public_use(df: pl.DataFrame) -> pl.DataFrame:
    v = Validator(df, "public_use")
    v.check_not_null("ParkName")
    v.check_regex("UnitCode", r"^[A-Z]{4}$")
    v.check_unique("_record_id")
    return v.report()


DATA_DIR = Path("data")


def get_latest_parquet_file(folder_path: str):
    files = glob.glob(f"{folder_path}/*.parquet")
    if not files:
        print(f"No Parquet files found in {folder_path}")
        return None
    files.sort(key=lambda x: os.path.getmtime(x))
    return files[-1]


def get_latest_parquet_file_for(table_name: str, layer: str = "RAW") -> str | None:
    folder = os.path.join("data", layer, table_name.upper())
    return get_latest_parquet_file(folder)


def run_all_validations(layer: str = "RAW") -> pl.DataFrame:

    parks_path = get_latest_parquet_file_for("PARKS", layer)
    if parks_path:
        parks_df = pl.read_parquet(parks_path)
        parks_results = validate_parks(parks_df)
    else:
        parks_results = pl.DataFrame([])

    alerts_path = get_latest_parquet_file_for("ALERTS", layer)
    if alerts_path:
        alerts_df = pl.read_parquet(alerts_path)
        alerts_results = validate_alerts(alerts_df)
    else:
        alerts_results = pl.DataFrame([])

    public_use_path = get_latest_parquet_file_for("PUBLIC_USE", layer)
    if public_use_path:
        public_use_df = pl.read_parquet(public_use_path)
        public_use_results = validate_public_use(public_use_df)
    else:
        public_use_results = pl.DataFrame([])

    results = pl.concat([
        parks_results,
        alerts_results,
        public_use_results,
    ])

    return results


def run_validations_nonblocking(layer: str = "RAW", fail_threshold: int = 0, raise_on_failure: bool = False):

    logger = logging.getLogger("data_validation")
    results = run_all_validations(layer=layer)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = DATA_DIR / "validation_reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / f"validation_results_{layer}_{timestamp}.parquet"
    try:
        results.write_parquet(report_path)
    except Exception as e:
        logger.exception("Failed to write validation report: %s", e)

    try:
        total = int(results.height)
        failures = int(results.filter(pl.col("passed") == False).height) if total > 0 else 0
    except Exception:
        total = 0
        failures = 0

    logger.info("Validation summary (layer=%s): %s failures / %s checks. report=%s", layer, failures, total, report_path)

    summary = {"layer": layer, "failures": failures, "total": total, "report": str(report_path)}

    if raise_on_failure and failures > fail_threshold:
        raise RuntimeError(f"Validation failures {failures} exceed threshold {fail_threshold}")

    return summary


def data_quality_checks(layer: str | None = None, fail_threshold: int = 0, raise_on_failure: bool = False):

    if layer is None:
        layer = sys.argv[1] if len(sys.argv) > 1 else "RAW"
    summary = run_validations_nonblocking(layer=layer, fail_threshold=fail_threshold, raise_on_failure=raise_on_failure)
    print(summary)
    return summary
