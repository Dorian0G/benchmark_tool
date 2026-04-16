"""
insight_generator.py
Produces human-readable insight text from benchmark results.

Two modes:
1. Rule-based (default)  — template-driven, fully deterministic
2. LLM-enhanced (opt-in) — sends the benchmark summary to GPT for richer prose

The output is structured so it can be pasted directly into an Excel sheet
or a Word/PowerPoint document — which maps to the Microsoft Copilot
integration concept described in the requirements.
"""

import logging

import pandas as pd


from modules.config import OPENAI_MODEL, USE_REAL_LLM
from modules.benchmark_engine import LOWER_IS_BETTER

logger = logging.getLogger(__name__)


# ── Rule-based generator ────────────────────────────────────────────────────

def _top_performer(bench_df: pd.DataFrame, metric: str) -> str:
    sub = bench_df[bench_df["Metric"] == metric].sort_values("Rank")
    if sub.empty:
        return "N/A"
    return sub.iloc[0]["Company"]


def _bottom_performer(bench_df: pd.DataFrame, metric: str) -> str:
    sub = bench_df[bench_df["Metric"] == metric].sort_values("Rank", ascending=False)
    if sub.empty:
        return "N/A"
    return sub.iloc[0]["Company"]


def generate_rule_based_insights(bench_df: pd.DataFrame) -> str:
    """Generate deterministic insight bullets from benchmark data."""
    metrics = bench_df["Metric"].unique().tolist()
    lines = ["## AI-Generated Benchmark Insights\n"]

    for metric in metrics:
        top  = _top_performer(bench_df, metric)
        bot  = _bottom_performer(bench_df, metric)
        avg  = bench_df[bench_df["Metric"] == metric]["Industry Average"].iloc[0]
        unit = "lower" if metric in LOWER_IS_BETTER else "higher"

        lines.append(f"**{metric}**")
        lines.append(f"  - Top performer: {top} leads with the best {unit}-is-better score.")
        lines.append(f"  - Underperformer: {bot} is below the peer group average of {avg:,.2f}.")
        lines.append("")

    # Overall summary
    lines.append("## Overall Summary")
    lines.append(
        "The benchmark analysis covers "
        f"{bench_df['Company'].nunique()} companies across "
        f"{len(metrics)} metrics. "
        "Rankings are computed peer-relative; industry averages are unweighted means."
    )
    return "\n".join(lines)


# ── LLM-enhanced generator ───────────────────────────────────────────────────

def _bench_to_summary_text(bench_df: pd.DataFrame) -> str:
    """Convert benchmark DataFrame to a compact text representation for the LLM."""
    lines = []
    for metric in bench_df["Metric"].unique():
        sub = bench_df[bench_df["Metric"] == metric].sort_values("Rank")
        lines.append(f"\n{metric}:")
        for _, row in sub.iterrows():
            lines.append(
                f"  {row['Company']}: {row['Value']:,.2f} "
                f"(rank {row['Rank']}, {row['Percentile']}th percentile)"
            )
    return "\n".join(lines)


def generate_llm_insights(bench_df: pd.DataFrame) -> str:
    """Send benchmark summary to the LLM for enriched prose insights."""
    try:
        from openai import OpenAI
        client = OpenAI()
        summary_text = _bench_to_summary_text(bench_df)
        prompt = f"""
You are a utility industry analyst. Based on the benchmark data below,
write 3-5 concise executive-level insights (2-3 sentences each).
Focus on competitive positioning, standout performance, and areas for improvement.
Use plain English suitable for a PowerPoint slide or Excel comment.

Data:
{summary_text}
"""
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.4,
            max_tokens=600,
        )
        return resp.choices[0].message.content or generate_rule_based_insights(bench_df)
    except Exception as exc:
        logger.warning("LLM insight generation failed: %s. Using rule-based.", exc)
        return generate_rule_based_insights(bench_df)


# ── Public interface ──────────────────────────────────────────────────────────

def generate_insights(bench_df: pd.DataFrame) -> str:
    """Generate insights using LLM if configured, else rule-based."""
    if USE_REAL_LLM:
        return generate_llm_insights(bench_df)
    return generate_rule_based_insights(bench_df)
