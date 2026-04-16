"""
data_collector.py
Builds CollectedDoc objects from the live cache + live scraping.

CHANGES:
  - Added "Charitable Giving ($M)" template in _cache_to_text()
  - Added html.parser fallback when lxml is not available (cloud deploys)
"""

import logging
import time
from dataclasses import dataclass, field

import requests
from bs4 import BeautifulSoup

from modules import data_cache as cache_module
from modules.config import COMPANY_IR_URLS, COMPANY_TICKERS, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
SEC_HEADERS = {
    "User-Agent": "UtilityBenchmark contact@example.com",
    "Accept": "application/json",
}


def _get_parser() -> str:
    """Return 'lxml' if available, else fall back to 'html.parser'."""
    try:
        import lxml  # noqa: F401
        return "lxml"
    except ImportError:
        return "html.parser"


@dataclass
class CollectedDoc:
    company: str
    source_url: str
    source_type: str
    raw_text: str
    fallback_text: str = ""


def _real_url(company: str) -> str:
    key = company.lower().strip()
    if key in COMPANY_IR_URLS:
        return COMPANY_IR_URLS[key]
    return f"https://www.sec.gov/cgi-bin/browse-edgar?company={requests.utils.quote(company)}&action=getcompany&type=10-K"


def _cache_to_text(company: str, cache: dict) -> str:
    """
    Convert cached metric values into extractable sentences.
    Each sentence uses the exact phrasing the regex patterns expect.
    """
    key = company.lower().strip()
    company_data = cache.get("companies", {}).get(key, {})
    if not company_data:
        return ""

    lines = []
    templates = {
        "Revenue": lambda v, y: f"{company} reported total revenue of ${v:.2f} billion for fiscal year {y.replace('FY','')}.",
        "Renewable Energy %": lambda v, y: f"Renewable energy percentage was {v:.1f}% of total generation.",
        "Outage Frequency": lambda v, y: f"Outage frequency (SAIDI) was {v:.0f} minutes per customer.",
        "Customer Satisfaction Score": lambda v, y: f"Customer satisfaction score was {v*10:.0f} out of 1000 per J.D. Power {y}.",
        "Carbon Emissions (MT CO2)": lambda v, y: f"Carbon emissions were {v:.2f} million metric tons of CO2.",
        "Charitable Giving ($M)": lambda v, y: f"Charitable contributions totaled ${v:.1f} million in {y.replace('FY','')}.",
    }
    for metric, tmpl in templates.items():
        entry = company_data.get(metric)
        if entry and entry.get("value") is not None:
            try:
                lines.append(tmpl(entry["value"], entry.get("year", "")))
            except Exception:
                pass

    return "\n".join(lines)


def _get(url: str, headers: dict) -> requests.Response | None:
    try:
        r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        return r if r.status_code == 200 else None
    except Exception:
        return None


def _sec_xbrl_revenue(company: str, ciks: dict) -> str | None:
    cik = ciks.get(company.lower().strip())
    if not cik:
        return None
    r = _get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json", SEC_HEADERS)
    if not r:
        return None
    try:
        us_gaap = r.json().get("facts", {}).get("us-gaap", {})
        for tag in ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"]:
            entries = [e for e in us_gaap.get(tag, {}).get("units", {}).get("USD", [])
                       if e.get("form") == "10-K" and e.get("fp") == "FY"]
            if not entries:
                continue
            latest = sorted(entries, key=lambda x: x.get("end", ""), reverse=True)[0]
            val_b = latest["val"] / 1e9
            year  = latest["end"][:4]
            return f"{company} total revenue of ${val_b:.2f} billion for fiscal year {year}."
    except Exception:
        pass
    return None


def _sec_10k_snippets(company: str, search_term: str, ciks: dict) -> str | None:
    cik = ciks.get(company.lower().strip())
    entity = f"CIK{cik}" if cik else company
    url = (
        "https://efts.sec.gov/LATEST/search-index"
        f"?q={requests.utils.quote(search_term)}"
        f"&entity={requests.utils.quote(entity)}"
        "&forms=10-K&dateRange=custom&startdt=2024-01-01&enddt=2026-12-31"
    )
    r = _get(url, SEC_HEADERS)
    if not r:
        return None
    try:
        hits = r.json().get("hits", {}).get("hits", [])
        if not hits:
            return None
        snippets = []
        for fh in hits[0].get("highlight", {}).values():
            snippets.extend(s.replace("<em>", "").replace("</em>", "") for s in fh[:3])
        return " ".join(snippets) if snippets else None
    except Exception:
        return None


def _scrape_esg(company: str, esg_urls: dict) -> str | None:
    parser = _get_parser()
    for url in esg_urls.get(company.lower().strip(), []):
        r = _get(url, HEADERS)
        if r:
            soup = BeautifulSoup(r.text, parser)
            for t in soup(["script", "style", "nav", "footer"]):
                t.decompose()
            text = soup.get_text(" ", strip=True)[:8000]
            if len(text) > 500:
                return text
    return None


def collect_for_company(company: str, cache: dict,
                        ciks: dict, esg_urls: dict) -> CollectedDoc:
    live_chunks: list[str] = []
    sources: list[str] = []

    # Revenue from EDGAR XBRL
    rev = _sec_xbrl_revenue(company, ciks)
    if rev:
        live_chunks.append(rev)
        sources.append("edgar-xbrl")

    # SAIDI, Carbon, Customer Sat, Renewable, Charitable from EDGAR 10-K text
    for term, label in [
        ("SAIDI",                     "edgar-saidi"),
        ("scope 1 emissions",        "edgar-carbon"),
        ("J.D. Power",               "edgar-jdpower"),
        ("renewable energy",         "edgar-renewable"),
        ("charitable contributions", "edgar-charitable"),
    ]:
        snippet = _sec_10k_snippets(company, term, ciks)
        if snippet:
            live_chunks.append(snippet)
            sources.append(label)

    # ESG page scrape
    esg = _scrape_esg(company, esg_urls)
    if esg:
        live_chunks.append(esg)
        sources.append("esg-scrape")

    # Build fallback text from cache
    fallback = _cache_to_text(company, cache)

    return CollectedDoc(
        company=company,
        source_url=_real_url(company),
        source_type="+".join(sources) if sources else "cache",
        raw_text="\n\n".join(c.strip() for c in live_chunks if c.strip()),
        fallback_text=fallback,
    )


def collect_all(companies: list[str]) -> list[CollectedDoc]:
    from modules.data_updater import COMPANY_CIKS, ESG_URLS

    cache = cache_module.load()
    docs = []

    for i, company in enumerate(companies):
        cache_module.add_company(cache, company)

        try:
            doc = collect_for_company(company, cache, COMPANY_CIKS, ESG_URLS)
        except Exception as e:
            logger.error("Error collecting %s: %s", company, e)
            doc = CollectedDoc(
                company=company,
                source_url=_real_url(company),
                source_type="cache",
                raw_text="",
                fallback_text=_cache_to_text(company, cache),
            )
        docs.append(doc)
        if i < len(companies) - 1:
            time.sleep(0.3)

    cache_module.save(cache)
    return docs


SIMULATED_DATA = cache_module.VERIFIED_DEFAULTS
