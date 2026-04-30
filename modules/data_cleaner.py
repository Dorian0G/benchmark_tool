"""
data_cleaner.py
Converts raw extracted strings to numeric values in natural display units.

CHANGES:
  - Added "charitable giving ($m)" to _DIVISOR (values in millions)
"""

import logging
import re
import pandas as pd

from modules.ai_extractor import ExtractedValue

logger = logging.getLogger(__name__)

_DIVISOR: dict[str, float] = {
    "revenue": 1_000_000_000,
    "renewable energy %": 1.0,
    "outage frequency": 1.0,
    "customer satisfaction score": 10.0,
    "carbon emissions (mt co2)": 1_000_000,
    "charitable giving ($m)": 1_000_000,
    "foundation assets ($m)": 1_000_000,
    "number of grants awarded": 1.0,
    "giving as % of revenue": 1.0,
}

_SCALE: dict[str, float] = {
    "billion": 1_000_000_000,
    "million": 1_000_000,
    "thousand": 1_000,
}

_NUM_RE = re.compile(r"\d[\d,]*\.?\d*")


def _parse_numeric(raw: str, metric: str = "") -> float | None:
    if not raw or raw.strip().upper() in ("N/A", "NONE", "NULL", ""):
        return None

    raw_lower = raw.lower()
    m = _NUM_RE.search(raw)
    if not m:
        return None

    try:
        num = float(m.group().replace(",", ""))
    except ValueError:
        return None

    for word, mult in _SCALE.items():
        if word in raw_lower:
            num *= mult
            break

    divisor = _DIVISOR.get(metric.lower().strip(), 1.0)
    return round(num / divisor, 4)


def build_raw_df(values: list[ExtractedValue]) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "Company": v.company,
            "Metric": v.metric,
            "Value": v.raw_value,
            "Source": v.source_url,
            "Source Type": v.source_type,
            "Confidence": v.confidence,
        }
        for v in values
    ])


def build_clean_df(
    raw_df: pd.DataFrame,
    expected_companies: list[str] | None = None,
    expected_metrics: list[str] | None = None,
) -> pd.DataFrame:
    df = raw_df.copy()

    df["Numeric Value"] = df.apply(
        lambda row: _parse_numeric(row["Value"], row["Metric"]),
        axis=1,
    )

    all_companies = expected_companies or df["Company"].drop_duplicates().tolist()
    all_metrics = expected_metrics or df["Metric"].drop_duplicates().tolist()

    pivot = (
        df.drop_duplicates(subset=["Company", "Metric"], keep="first")
        .pivot(index="Company", columns="Metric", values="Numeric Value")
        .reindex(index=all_companies, columns=all_metrics)
        .reset_index()
    )

    pivot.columns.name = None
    return pivot


def fill_missing(clean_df: pd.DataFrame) -> pd.DataFrame:
    df = clean_df.copy()

    for col in [c for c in df.columns if c != "Company"]:
        col_mean = df[col].mean()
        n = df[col].isna().sum()

        if n and pd.notna(col_mean):
            logger.info("Imputing %d missing in '%s' with mean %.4f", n, col, col_mean)
            df[col] = df[col].fillna(col_mean)
        elif n:
            logger.info("Column '%s' entirely missing — leaving as NaN.", col)

    return df


def compute_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute metrics that are derived from other metrics rather than scraped.

    Currently:
      - "Giving as % of Revenue" = Charitable Giving ($M) / (Revenue ($B) * 1000) * 100
        Formula: giving_M / (revenue_B * 10) yields percent.
        Example: $30M / $25B = 0.0012 = 0.12%
    """
    out = df.copy()

    if "Giving as % of Revenue" in out.columns:
        if "Charitable Giving ($M)" in out.columns and "Revenue" in out.columns:
            giving = pd.to_numeric(out["Charitable Giving ($M)"], errors="coerce")
            revenue = pd.to_numeric(out["Revenue"], errors="coerce")
            # Avoid divide-by-zero — replace 0 revenue with NaN
            ratio = giving / (revenue.where(revenue > 0) * 10.0)
            out["Giving as % of Revenue"] = ratio.round(4)
            logger.info("Computed 'Giving as % of Revenue' from Revenue + Charitable Giving")
        else:
            logger.warning(
                "'Giving as %% of Revenue' requested but Revenue and/or "
                "'Charitable Giving ($M)' missing from data — column left as-is."
            )

    return out
