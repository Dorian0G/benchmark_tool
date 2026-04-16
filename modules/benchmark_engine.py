"""
benchmark_engine.py
Computes rankings, industry averages, and percentiles from cleaned data.

For each metric:
  - Rank 1 = best performer (configurable: higher-is-better or lower-is-better)
  - Industry average = mean across all companies
  - Percentile = scipy-free approximation using pandas rank

"Higher is better" metrics: Revenue, Renewable Energy %, Customer Satisfaction
"Lower is better" metrics:  Outage Frequency, Carbon Emissions
"""

import pandas as pd

# Metrics where a LOWER value = better rank
LOWER_IS_BETTER = {
    "Outage Frequency",
    "Carbon Emissions (MT CO2)",
}


def _rank_metric(series: pd.Series, metric: str) -> pd.Series:
    """Rank companies on a single metric. Returns rank as integer (1 = best)."""
    ascending = metric in LOWER_IS_BETTER
    return series.rank(ascending=ascending, method="min").astype("Int64")


def _percentile(series: pd.Series, metric: str) -> pd.Series:
    """
    Compute 0-100 percentile score for each company on a metric.
    For 'lower is better' metrics the percentile is inverted so that
    the best performer always has the highest percentile.
    """
    pct = series.rank(pct=True) * 100
    if metric in LOWER_IS_BETTER:
        pct = 100 - pct
    return pct.round(1)


def build_benchmark(clean_df: pd.DataFrame) -> pd.DataFrame:
    """
    Take the cleaned pivoted DataFrame and return a long-form benchmark table.

    Output columns:
        Company | Metric | Value | Rank | Industry Average | Percentile
    """
    rows = []
    numeric_cols = [c for c in clean_df.columns if c != "Company"]

    for metric in numeric_cols:
        col = clean_df[metric]
        ranks = _rank_metric(col, metric)
        pcts  = _percentile(col, metric)
        avg   = col.mean()

        for i, company in enumerate(clean_df["Company"]):
            rows.append(
                {
                    "Company": company,
                    "Metric": metric,
                    "Value": round(col.iloc[i], 2) if pd.notna(col.iloc[i]) else None,
                    "Rank": ranks.iloc[i],
                    "Industry Average": round(avg, 2),
                    "Percentile": pcts.iloc[i],
                }
            )

    return pd.DataFrame(rows)
