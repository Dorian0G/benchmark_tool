"""
ai_extractor.py
Extracts metric values from collected documents.

CHANGES:
  - Added PATTERNS and METRIC_SYNONYMS entries for "Charitable Giving ($M)"
"""

import json
import logging
import re
from dataclasses import dataclass

from modules.config import OPENAI_MODEL, USE_REAL_LLM
from modules.data_collector import CollectedDoc

logger = logging.getLogger(__name__)


@dataclass
class ExtractedValue:
    company: str
    metric: str
    raw_value: str
    source_url: str
    source_type: str
    confidence: float


# ── Metric-specific patterns ──────────────────────────────────────────────────

PATTERNS: dict[str, list[re.Pattern]] = {
    "Revenue": [
        re.compile(r'revenues?\D{0,30}?\$\s*(\d[\d,]*\.?\d*)\s*(billion|million|thousand)?', re.I),
        re.compile(r'\$\s*(\d[\d,]*\.?\d*)\s*(billion|million|thousand)', re.I),
        re.compile(r'revenues?\D{0,50}?(\d[\d,]*\.?\d*)\s*(billion|million)', re.I),
    ],
    "Renewable Energy %": [
        re.compile(r'(?:renewable|clean)\s+energy\D{0,50}?(\d[\d,]*\.?\d*)\s*(%|percent)', re.I),
        re.compile(r'(\d[\d,]*\.?\d*)\s*(?:%|percent)\s+of\s+(?:electric\s+)?(?:supply|generation|electricity|energy)', re.I),
        re.compile(r'(\d[\d,]*\.?\d*)\s*(?:%|percent)\D{0,30}(?:renewable|clean\s+energy)', re.I),
    ],
    "Outage Frequency": [
        re.compile(r'SAIDI\D{0,50}?(\d[\d,]*\.?\d*)\s*(minutes?)?', re.I),
        re.compile(r'(?:system\s+average\s+interruption\s+duration|interruption\s+duration\s+index)\D{0,50}?(\d[\d,]*\.?\d*)', re.I),
        re.compile(r'(\d[\d,]*\.?\d*)\s*(?:customer[- ])?minutes?\s+(?:of\s+)?(?:outage|interruption)', re.I),
    ],
    "Customer Satisfaction Score": [
        re.compile(r'(?:J\.?\s*D\.?\s*Power|satisfaction\s+(?:score|index|rating))\D{0,60}?(\d{2,3})\b', re.I),
        re.compile(r'(\d{2,3})\s*(?:out\s+of\s+(?:100|1000)|/\s*(?:100|1000))', re.I),
    ],
    "Carbon Emissions (MT CO2)": [
        re.compile(r'(?:scope\s*1|greenhouse\s+gas|GHG|carbon)\s+emissions\D{0,50}?(\d[\d,]*\.?\d*)\s*(million\s+metric\s+tons?|million\s+MT|Mt\b|MMT)', re.I),
        re.compile(r'(\d[\d,]*\.?\d*)\s*(million\s+metric\s+tons?|million\s+MT|Mt\b|MMT)\s+(?:of\s+)?CO2', re.I),
        re.compile(r'(?:scope\s*1|GHG|carbon)\D{0,30}?(\d[\d,]*\.?\d*)\s*(million\s+metric\s+tons?|MMT|Mt\b)', re.I),
    ],
    "Charitable Giving ($M)": [
        # "$34 million in charitable contributions"
        re.compile(r'\$\s*(\d[\d,]*\.?\d*)\s*(million|billion)\s+(?:in\s+)?(?:charitable|philanthropic|community)\s+(?:contributions?|giving|investments?|grants?)', re.I),
        # "charitable contributions of $34 million"
        re.compile(r'(?:charitable|philanthropic|community)\s+(?:contributions?|giving|investments?|grants?)\D{0,40}?\$\s*(\d[\d,]*\.?\d*)\s*(million|billion)?', re.I),
        # "donated $34 million"
        re.compile(r'(?:donated|contributed|invested|awarded)\s+(?:more\s+than\s+)?\$\s*(\d[\d,]*\.?\d*)\s*(million|billion)', re.I),
        # "foundation giving totaled $34 million"
        re.compile(r'(?:foundation|corporate)\s+(?:giving|grants?)\D{0,40}?\$\s*(\d[\d,]*\.?\d*)\s*(million|billion)?', re.I),
        # "grants paid $8.5 million" (990 language)
        re.compile(r'grants?\s+(?:paid|awarded|distributed)\D{0,40}?\$\s*(\d[\d,]*\.?\d*)\s*(million|billion)?', re.I),
        # Fallback: any dollar amount near philanthropy keywords
        re.compile(r'(?:philanthrop|charit|foundation|community\s+invest)\D{0,60}?\$\s*(\d[\d,]*\.?\d*)\s*(million|billion)?', re.I),
    ],
}

METRIC_SYNONYMS: dict[str, list[str]] = {
    "Revenue":                      ["total revenue", "net revenue", "operating revenue"],
    "Renewable Energy %":           ["renewable energy percentage", "clean energy", "renewables"],
    "Outage Frequency":             ["saidi", "outage frequency", "interruption duration"],
    "Customer Satisfaction Score":  ["customer satisfaction", "j.d. power", "csat"],
    "Carbon Emissions (MT CO2)":    ["carbon emissions", "scope 1", "ghg emissions", "greenhouse gas"],
    "Charitable Giving ($M)":       [
        "charitable giving", "charitable contributions", "foundation giving",
        "philanthropy", "community investment", "corporate giving",
        "grants paid", "foundation grants", "donations",
    ],
}

_GENERIC_NUM_RE = re.compile(
    r'(?:\$\s*)?(\d[\d,]*\.?\d*)\s*(billion|million|thousand|%|percent|minutes?|mt\b)?',
    re.IGNORECASE
)


def _extract(text: str, metric: str) -> str | None:
    """Try metric-specific patterns, then synonym keyword search."""
    if not text or not text.strip():
        return None

    for pat in PATTERNS.get(metric, []):
        m = pat.search(text)
        if m:
            num  = m.group(1)
            unit = m.group(2) if m.lastindex and m.lastindex >= 2 and m.group(2) else ""
            return f"{num} {unit}".strip()

    synonyms = METRIC_SYNONYMS.get(metric, [metric])
    sentences = re.split(r'(?<=[a-zA-Z0-9%])\.\s+|\n', text)
    for kw in synonyms:
        for sentence in sentences:
            if kw.lower() in sentence.lower():
                m = _GENERIC_NUM_RE.search(sentence)
                if m:
                    unit = m.group(2) or ""
                    return f"{m.group(1)} {unit}".strip()

    return None


def extract_rule_based(doc: CollectedDoc, metrics: list[str]) -> list[ExtractedValue]:
    results = []
    for metric in metrics:
        raw = _extract(doc.raw_text, metric)
        if raw:
            source_type = doc.source_type
            confidence  = 0.85
        else:
            raw = _extract(doc.fallback_text, metric)
            if raw:
                source_type = "verified-fallback"
                confidence  = 0.75
            else:
                source_type = "not-found"
                confidence  = 0.0

        results.append(ExtractedValue(
            company=doc.company,
            metric=metric,
            raw_value=raw or "N/A",
            source_url=doc.source_url,
            source_type=source_type,
            confidence=confidence,
        ))

    return results


# ── LLM extractor (optional) ──────────────────────────────────────────────────

def _build_llm_prompt(company: str, metrics: list[str], text: str) -> str:
    metric_list = "\n".join(f"- {m}" for m in metrics)
    return f"""
You are a financial data analyst. Extract the following metrics for {company}
from the text below. Return ONLY valid JSON — no markdown, no explanation.

Metrics:
{metric_list}

Text:
{text[:4000]}

Return format:
{{
  "metrics": {{
    "<metric name>": "<value as string with unit, or null>"
  }}
}}
"""


def extract_llm(doc: CollectedDoc, metrics: list[str]) -> list[ExtractedValue]:
    combined = "\n\n".join(t for t in [doc.raw_text, doc.fallback_text] if t.strip())
    try:
        from openai import OpenAI
        client = OpenAI()
        prompt = _build_llm_prompt(doc.company, metrics, combined)
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        raw_json = re.sub(r"```json|```", "", response.choices[0].message.content or "{}").strip()
        extracted = json.loads(raw_json).get("metrics", {})
    except Exception as e:
        logger.warning("LLM failed for %s: %s — falling back to rule-based", doc.company, e)
        return extract_rule_based(doc, metrics)

    return [
        ExtractedValue(
            company=doc.company,
            metric=metric,
            raw_value=extracted.get(metric) or "N/A",
            source_url=doc.source_url,
            source_type=f"{doc.source_type}+llm" if extracted.get(metric) else "not-found",
            confidence=0.9 if extracted.get(metric) else 0.0,
        )
        for metric in metrics
    ]


def extract_metrics(docs: list[CollectedDoc], metrics: list[str]) -> list[ExtractedValue]:
    extractor = extract_llm if USE_REAL_LLM else extract_rule_based
    results = []
    for doc in docs:
        results.extend(extractor(doc, metrics))
    return results
