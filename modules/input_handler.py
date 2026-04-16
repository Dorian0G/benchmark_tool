"""
input_handler.py
Validates and normalizes user-supplied company and metric lists.

Design decisions:
- Strip whitespace and collapse internal spaces so "Con  Edison" → "Con Edison"
- Deduplicate case-insensitively (preserving first occurrence casing)
- Return a structured BenchmarkRequest dataclass so all downstream modules
  share an identical contract — no raw strings passed around.
"""

from dataclasses import dataclass, field
import re


@dataclass
class BenchmarkRequest:
    companies: list[str]
    metrics: list[str]


def _normalize_str(s: str) -> str:
    """Strip edges, collapse internal whitespace."""
    return re.sub(r"\s+", " ", s.strip())


def _deduplicate(items: list[str]) -> list[str]:
    """Return items with duplicates removed (case-insensitive, first-wins)."""
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        key = item.lower()
        if key not in seen:
            seen.add(key)
            result.append(item)
    return result


def parse_input(companies: list[str], metrics: list[str]) -> BenchmarkRequest:
    """
    Validate and normalize raw user input.

    Parameters
    ----------
    companies : list of raw company name strings
    metrics   : list of raw metric name strings

    Returns
    -------
    BenchmarkRequest with cleaned, deduplicated lists

    Raises
    ------
    ValueError if either list is empty after cleaning
    """
    cleaned_companies = _deduplicate([_normalize_str(c) for c in companies if c.strip()])
    cleaned_metrics   = _deduplicate([_normalize_str(m) for m in metrics   if m.strip()])

    if not cleaned_companies:
        raise ValueError("At least one company name is required.")
    if not cleaned_metrics:
        raise ValueError("At least one metric is required.")

    return BenchmarkRequest(companies=cleaned_companies, metrics=cleaned_metrics)
