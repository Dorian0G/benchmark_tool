"""
app.py — Utility Benchmark Tool
Every chart and table element shows explicit units so all numbers are
immediately understandable without context.

CHANGES:
  - Added METRIC_META entry for "Charitable Giving ($M)"
  - Cloud deployment fixes:
    • Background updater wrapped in try/except so it never blocks or crashes
    • st.cache_resource used for initial cache load (survives Streamlit reruns)
    • Graceful fallback when threading or scheduler fails
    • html.parser fallback when lxml not installed
"""

import logging
import sys
import threading
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent))

from modules.config import DEFAULT_COMPANIES, DEFAULT_METRICS, REPORT_YEAR
from modules.input_handler import parse_input
from modules.data_collector import collect_all
from modules.ai_extractor import extract_metrics
from modules.data_cleaner import build_raw_df, build_clean_df, fill_missing
from modules.benchmark_engine import build_benchmark
from modules.insight_generator import generate_rule_based_insights
from modules.output_generator import generate_excel
from modules.copilot_bridge import build_copilot_prompt
from modules import data_cache as cache_module

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Utility Benchmark Tool",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Auto-update on startup (cloud-safe) ───────────────────────────────────────
# The updater runs in a background thread so the UI loads immediately.
# On cloud deployments where threading or network may behave differently,
# all failures are caught and logged — the app always works using cached/
# verified fallback data even if live scraping is unavailable.
if "_updater_started" not in st.session_state:
    st.session_state._updater_started = True
    try:
        from modules.data_updater import check_for_updates, start_background_scheduler
        _all_companies = [c.strip() for c in DEFAULT_COMPANIES if c.strip()]

        def _safe_update():
            try:
                check_for_updates(_all_companies)
            except Exception as e:
                logger.warning("Background update failed (non-fatal): %s", e)

        threading.Thread(target=_safe_update, daemon=True).start()

        try:
            start_background_scheduler(_all_companies)
        except Exception as e:
            logger.warning("Scheduler start failed (non-fatal): %s", e)
    except Exception as e:
        logger.warning("Updater import/start failed (non-fatal): %s — using cached data.", e)

st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; }
    .stTabs [data-baseweb="tab"] { font-size: 0.9rem; }
    .copilot-box {
        background:#f0f4ff; border:1px solid #c5d3f5; border-radius:10px;
        padding:1rem 1.2rem; font-family:monospace; font-size:0.82rem;
        white-space:pre-wrap; color:#1a1a2e; max-height:340px; overflow-y:auto;
    }
    .copilot-tip {
        background:#eaf6ff; border-left:4px solid #0078d4; border-radius:4px;
        padding:0.6rem 1rem; font-size:0.88rem; color:#003a70;
    }
    .metric-legend {
        background:#f8f9fa; border:1px solid #dee2e6; border-radius:8px;
        padding:0.8rem 1rem; margin-bottom:1rem; font-size:0.87rem;
    }
</style>
""", unsafe_allow_html=True)

# ── Metric definitions ─────────────────────────────────────────────────────────

METRIC_META = {
    "Revenue": {
        "unit":           "$B",
        "unit_long":      "Billions of USD",
        "format":         lambda v: f"${v:.1f}B",
        "axis_title":     "Revenue ($B)",
        "description":    "Total annual revenue in billions of USD (FY2024).",
        "expected_range": "Typically $10B – $35B for large US utilities",
        "lower_is_better": False,
        "chart_note":     "↑ Higher = larger company",
    },
    "Renewable Energy %": {
        "unit":           "%",
        "unit_long":      "% of energy mix from renewables",
        "format":         lambda v: f"{v:.0f}%",
        "axis_title":     "Renewable Energy (%)",
        "description":    "Share of energy supply from renewable sources (solar, wind, hydro). FY2024.",
        "expected_range": "Typically 15% – 50% for US utilities in 2024",
        "lower_is_better": False,
        "chart_note":     "↑ Higher = greener energy mix",
    },
    "Outage Frequency": {
        "unit":           "min",
        "unit_long":      "Average minutes without power per customer per year (SAIDI)",
        "format":         lambda v: f"{v:.0f} min",
        "axis_title":     "SAIDI (minutes/customer/year)",
        "description":    "System Average Interruption Duration Index — avg minutes per customer without power per year. Lower is better.",
        "expected_range": "Typically 50 – 200 min/year for US utilities (lower = more reliable)",
        "lower_is_better": True,
        "chart_note":     "↓ Lower = more reliable grid (fewer/shorter outages)",
    },
    "Customer Satisfaction Score": {
        "unit":           "/100",
        "unit_long":      "Score out of 100 (normalised from J.D. Power /1000 scale)",
        "format":         lambda v: f"{v:.1f}/100",
        "axis_title":     "J.D. Power Score (/100)",
        "description":    "J.D. Power 2025 Residential Electric Satisfaction score, normalised to /100. Industry avg 2025 = 49.9/100 (dropped sharply due to rising bills).",
        "expected_range": "Typically 45 – 55 out of 100 in 2025 (industry avg fell to 49.9 due to rising bills)",
        "lower_is_better": False,
        "chart_note":     "↑ Higher = happier customers",
    },
    "Carbon Emissions (MT CO2)": {
        "unit":           "M MT",
        "unit_long":      "Million metric tons of CO₂ equivalent (Scope 1)",
        "format":         lambda v: f"{v:.1f} M MT",
        "axis_title":     "Scope 1 Emissions (million MT CO₂)",
        "description":    "Total Scope 1 carbon emissions in million metric tons CO₂e. ⚠️ Integrated generators (Duke, Southern) emit far more than distributors (Con Edison, Eversource).",
        "expected_range": "Distributors: 2–10 M MT  |  Integrated generators: 50–100 M MT",
        "lower_is_better": True,
        "chart_note":     "↓ Lower = smaller carbon footprint  ⚠️ Not comparable across generator vs distributor",
    },
    "Charitable Giving ($M)": {
        "unit":           "$M",
        "unit_long":      "Millions of USD in charitable/foundation giving",
        "format":         lambda v: f"${v:.1f}M",
        "axis_title":     "Charitable Giving ($M)",
        "description":    "Total corporate philanthropy and foundation giving in millions USD. Sources: IRS Form 990, CSR reports, SEC 10-K filings.",
        "expected_range": "Typically $5M – $50M for large US utilities (varies with company size)",
        "lower_is_better": False,
        "chart_note":     "↑ Higher = more community investment  (scale with company size)",
    },
}

METRIC_DATA_YEAR: dict[str, str] = {
    "Revenue":                      "FY2025",
    "Renewable Energy %":           "FY2024",
    "Outage Frequency":             "2024",
    "Customer Satisfaction Score":  "2025",
    "Carbon Emissions (MT CO2)":    "FY2024",
    "Charitable Giving ($M)":       "FY2024",
}


def _fmt(metric: str, value: float) -> str:
    meta = METRIC_META.get(metric, {})
    fmt_fn = meta.get("format")
    if fmt_fn and pd.notna(value):
        try:
            return fmt_fn(float(value))
        except Exception:
            pass
    return str(value)


def make_metric_chart(bench_df: pd.DataFrame, metric: str) -> alt.Chart:
    sub = bench_df[bench_df["Metric"] == metric][["Company", "Value"]].copy()
    sub["Value"] = pd.to_numeric(sub["Value"], errors="coerce")
    sub = sub.dropna(subset=["Value"])
    if sub.empty:
        return alt.Chart(pd.DataFrame()).mark_text().encode()

    meta = METRIC_META.get(metric, {})
    lower_better = meta.get("lower_is_better", False)
    axis_title = meta.get("axis_title", metric)
    note = meta.get("chart_note", "")
    fmt_fn = meta.get("format", lambda v: f"{v:.2f}")
    expected = meta.get("expected_range", "")

    sub = sub.sort_values("Value", ascending=lower_better).reset_index(drop=True)
    sub["rank"] = range(len(sub))
    sub["label"] = sub["Value"].apply(lambda v: fmt_fn(float(v)))

    bars = (
        alt.Chart(sub)
        .mark_bar()
        .encode(
            x=alt.X(
                "Value:Q",
                title=axis_title,
                scale=alt.Scale(zero=True),
                axis=alt.Axis(format="~g"),
            ),
            y=alt.Y(
                "Company:N",
                sort=list(sub["Company"]),
                title="",
            ),
            color=alt.Color(
                "rank:Q",
                scale=alt.Scale(range=["#1B3A5C", "#7BB8F0"]),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("Company:N", title="Company"),
                alt.Tooltip("label:N", title=axis_title),
            ],
        )
    )

    bar_labels = (
        alt.Chart(sub)
        .mark_text(align="left", dx=4, fontSize=11, color="#333")
        .encode(
            x=alt.X("Value:Q"),
            y=alt.Y("Company:N", sort=list(sub["Company"])),
            text=alt.Text("label:N"),
        )
    )

    data_year = METRIC_DATA_YEAR.get(metric, "FY2024")
    subtitle_parts = [f"{note}  |  Data: {data_year}"]
    if expected:
        subtitle_parts.append(f"Expected range: {expected}")

    return (
        (bars + bar_labels)
        .properties(
            title=alt.TitleParams(
                text=metric,
                subtitle=subtitle_parts,
                subtitleColor="#666",
                fontSize=14,
                subtitleFontSize=10,
            ),
            height=max(200, len(sub) * 42),
        )
        .configure_axis(labelFontSize=12, titleFontSize=11)
        .configure_view(strokeWidth=0)
    )


# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image(
        "https://upload.wikimedia.org/wikipedia/commons/thumb/9/9f/Lightning_bolt_simple.svg/240px-Lightning_bolt_simple.svg.png",
        width=48,
    )
    st.title("Utility Benchmark Tool")
    st.caption("Competitive intelligence for energy companies.")
    st.divider()

    st.subheader("🏢 Companies")
    companies_raw = st.text_area(
        "One company per line",
        value="\n".join(DEFAULT_COMPANIES[:4]),
        height=130,
    )

    st.subheader("📐 Metrics")
    metrics_raw = st.text_area(
        "One metric per line",
        value="\n".join(DEFAULT_METRICS),
        height=130,
    )

    run_btn = st.button("🚀 Run Benchmark", use_container_width=True, type="primary")
    st.divider()
    st.markdown(
        "**Data sources:** SEC EDGAR · ESG reports · IRS Form 990 · J.D. Power · Verified fallback\n\n"
        "**No API keys required.**"
    )


# ── Main ───────────────────────────────────────────────────────────────────────
st.title("⚡ Utility Company Benchmark Analysis")
st.caption("FY2025 data (most recent available) · Primary sources: SEC EDGAR, IRS Form 990, company CSR reports, J.D. Power 2025")

if not run_btn and "bench_df" not in st.session_state:
    col1, col2, col3 = st.columns(3)
    col1.metric("Companies supported", "2 – 10")
    col2.metric("Metrics supported", "3 – 10")
    col3.metric("Export formats", "Excel + Copilot prompt")
    st.info("Enter companies and metrics in the sidebar, then click **Run Benchmark**.")
    st.stop()

# ── Run pipeline only on button click ─────────────────────────────────────────
if run_btn:
    companies = [c for c in companies_raw.strip().splitlines() if c.strip()]
    metrics = [m for m in metrics_raw.strip().splitlines() if m.strip()]

    try:
        request = parse_input(companies, metrics)
    except ValueError as e:
        st.error(str(e))
        st.stop()

    progress = st.progress(0, text="Starting…")
    progress.progress(10, text="🔍 Collecting data from SEC EDGAR, IRS 990 & ESG sources…")
    docs = collect_all(request.companies)

    progress.progress(35, text="🧠 Extracting metrics…")
    extracted = extract_metrics(docs, request.metrics)

    progress.progress(55, text="🧹 Cleaning and normalizing…")
    raw_df = build_raw_df(extracted)

    clean_df = build_clean_df(raw_df)
    filled_df = fill_missing(clean_df)

    progress.progress(72, text="📊 Computing rankings…")
    bench_df = build_benchmark(filled_df)

    if bench_df.empty:
        st.error("No benchmark rows were generated.")
        st.dataframe(raw_df, use_container_width=True, hide_index=True)
        st.stop()

    progress.progress(87, text="💡 Generating insights…")
    insights = generate_rule_based_insights(bench_df)

    progress.progress(95, text="📦 Preparing Excel…")
    copilot_prompt = build_copilot_prompt(bench_df, request.companies, request.metrics)
    excel_bytes = generate_excel(raw_df, clean_df, bench_df, insights, copilot_prompt=copilot_prompt)
    progress.progress(100, text="✅ Done!")

    st.session_state.bench_df = bench_df
    st.session_state.raw_df = raw_df
    st.session_state.filled_df = filled_df
    st.session_state.insights = insights
    st.session_state.copilot_prompt = copilot_prompt
    st.session_state.excel_bytes = excel_bytes
    st.session_state.request = request

bench_df = st.session_state.bench_df
raw_df = st.session_state.raw_df
filled_df = st.session_state.filled_df
insights = st.session_state.insights
copilot_prompt = st.session_state.copilot_prompt
excel_bytes = st.session_state.excel_bytes
request = st.session_state.request

# ── Tabs ───────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 Benchmark", "📈 Charts", "🧹 Clean Data", "📄 Raw Data", "💡 Insights", "🤖 Copilot"
])

# ── Tab 1: Benchmark ───────────────────────────────────────────────────────────
with tab1:
    st.subheader("Benchmark Summary")

    st.markdown('<div class="metric-legend">', unsafe_allow_html=True)
    st.markdown("**What each Value means:**")
    active_meta = [(m, METRIC_META[m]) for m in request.metrics if m in METRIC_META]
    if active_meta:
        cols = st.columns(min(len(active_meta), 3))
        for i, (metric, meta) in enumerate(active_meta):
            with cols[i % len(cols)]:
                st.markdown(
                    f"**{metric}**  \n"
                    f"`{meta['unit']}` — {meta['unit_long']}  \n"
                    f"*{meta['expected_range']}*"
                )
    st.markdown('</div>', unsafe_allow_html=True)

    display = bench_df[["Company", "Metric", "Value", "Rank", "Percentile"]].copy()

    try:
        _cache = cache_module.load()

        def _get_year(row):
            entry = cache_module.get_value(_cache, row["Company"], row["Metric"])
            if entry and entry.get("year") and entry["year"] != "seed":
                return entry["year"]
            return METRIC_DATA_YEAR.get(row["Metric"], "FY2025")

        display["Data Year"] = display.apply(_get_year, axis=1)
    except Exception:
        display["Data Year"] = display["Metric"].map(
            lambda m: METRIC_DATA_YEAR.get(m, "FY2025")
        )

    display = display[["Company", "Metric", "Data Year", "Value", "Rank", "Percentile"]]
    display["Value"] = display.apply(
        lambda row: _fmt(row["Metric"], row["Value"]) if pd.notna(row["Value"]) else "N/A",
        axis=1
    )
    display["Percentile"] = display["Percentile"].apply(
        lambda v: f"{v:.0f}th" if pd.notna(v) else "N/A"
    )

    st.dataframe(display, use_container_width=True, hide_index=True)

# ── Tab 2: Charts ──────────────────────────────────────────────────────────────
with tab2:
    st.subheader("Metric Charts")
    st.caption(
        "Each bar is labelled with its value and unit. "
        "Best performer is at the top."
    )

    active_metrics = bench_df["Metric"].unique().tolist()

    for i in range(0, len(active_metrics), 2):
        pair = active_metrics[i:i + 2]
        cols = st.columns(len(pair))
        for col, metric in zip(cols, pair):
            with col:
                st.altair_chart(make_metric_chart(bench_df, metric), use_container_width=True)

    if "Carbon Emissions (MT CO2)" in active_metrics:
        st.info(
            "⚠️ **Carbon Emissions note:** Duke Energy and Southern Company are integrated generation "
            "utilities that burn fuel to generate electricity, so their Scope 1 emissions (50–70 M MT) "
            "are 10–20× higher than pure distributors like Con Edison or Eversource (2–5 M MT). "
            "This is a structural difference — compare within peer type for a fair assessment."
        )
    if "Charitable Giving ($M)" in active_metrics:
        st.info(
            "💡 **Charitable Giving note:** Giving amounts correlate with company size and revenue. "
            "Larger integrated utilities (Duke, Southern) typically have larger foundation budgets than "
            "smaller distributors. For a fairer comparison, consider giving as a % of revenue."
        )

# ── Tab 3: Clean Data ──────────────────────────────────────────────────────────
with tab3:
    st.subheader("Cleaned & Normalized Data")

    st.markdown(
        "| Metric | Unit | What it means |\n"
        "|--------|------|---------------|\n" +
        "\n".join(
            f"| **{m}** | `{meta['unit']}` | {meta['unit_long']} |"
            for m, meta in METRIC_META.items()
            if m in request.metrics
        ),
        unsafe_allow_html=False,
    )

    st.dataframe(filled_df, use_container_width=True, hide_index=True)
    st.caption("Missing values imputed with column mean.")

# ── Tab 4: Raw Data ────────────────────────────────────────────────────────────
with tab4:
    st.subheader("Raw Extracted Data")
    raw_display = raw_df.copy()
    raw_display["Source"] = raw_display["Source"].apply(
        lambda url: f'<a href="{url}" target="_blank">🔗 Source</a>'
        if url and url.startswith("http") else url
    )
    st.markdown(raw_display.to_html(escape=False, index=False), unsafe_allow_html=True)
    fb_count = raw_df["Source Type"].str.contains("verified-fallback", na=False).sum()
    live_count = len(raw_df) - raw_df["Source Type"].str.contains("verified-fallback|not-found", na=False).sum()
    p990_count = raw_df["Source Type"].str.contains("propublica-990", na=False).sum()
    st.info(
        f"**{live_count}** values from live sources (SEC EDGAR / ESG scrape / IRS 990)  |  "
        f"**{fb_count}** values from verified fallback data  "
        + (f"|  **{p990_count}** from ProPublica 990 API" if p990_count else "")
    )

# ── Tab 5: Insights ────────────────────────────────────────────────────────────
with tab5:
    st.subheader("AI-Generated Insights")
    st.markdown(insights)

# ── Tab 6: Copilot ─────────────────────────────────────────────────────────────
with tab6:
    st.subheader("🤖 Microsoft Copilot Integration")
    st.markdown(
        '<div class="copilot-tip"><strong>No API required.</strong> Copy the prompt below and paste it into '
        '<a href="https://copilot.microsoft.com" target="_blank">copilot.microsoft.com</a>.</div>',
        unsafe_allow_html=True,
    )
    st.markdown("#### Ready-to-paste Copilot prompt")
    st.markdown(f'<div class="copilot-box">{copilot_prompt}</div>', unsafe_allow_html=True)
    st.link_button("🌐 Open Copilot →", url="https://copilot.microsoft.com")
    st.divider()
    st.markdown("""**Follow-up prompts to try:**
- *"Summarise as 3 executive bullet points"*
- *"Which company should we benchmark against and why?"*
- *"Draft a PowerPoint slide for each metric"*
- *"Write a CEO briefing note on top and bottom performers"*
- *"Compare charitable giving relative to company revenue"*
""")

# ── Download ───────────────────────────────────────────────────────────────────
st.divider()
col1, col2 = st.columns([2, 1])
col1.download_button(
    label="📥 Download Excel Report",
    data=excel_bytes,
    file_name="utility_benchmark.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    use_container_width=True,
    type="primary",
)
col2.markdown("Includes all data sheets + Copilot prompt tab.")
