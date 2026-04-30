"""
data_collector.py
Builds CollectedDoc objects from live government sources + cache fallback.

CHANGES:
  - Added retry logic with exponential backoff for SEC API calls
  - Prioritized SEC EDGAR (government-regulated) over company websites
  - Added CIK lookup fallback from SEC company tickers API
  - Reduced aggressive cache fallback - only use when live sources truly fail
  - Added rate limiting to avoid SEC throttling
"""

import logging
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional

import requests
from bs4 import BeautifulSoup

from modules import data_cache as cache_module
from modules.config import COMPANY_IR_URLS, COMPANY_TICKERS, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

# ── HTTP Configuration ───────────────────────────────────────────────────────
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

# ── Retry Configuration ───────────────────────────────────────────────────────
# Tuned for fast wall-time on a UI-driven workload. The verified-fallback
# cache catches anything that genuinely fails, so paying 30+ s of retry
# wait per failed search term wasn't buying us better data.
MAX_RETRIES = 2
INITIAL_DELAY = 0.5  # seconds
BACKOFF_FACTOR = 2.0
RATE_LIMIT_DELAY = 0.3  # seconds between SEC requests


# ── Retry and Rate-Limiting Helpers ──────────────────────────────────────────

def _get_with_retry(url: str, headers: dict, max_retries: int = MAX_RETRIES) -> Optional[requests.Response]:
    """GET request with exponential backoff retry."""
    delay = INITIAL_DELAY
    last_error = None
    
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if r.status_code == 200:
                return r
            elif r.status_code == 429:  # Rate limited
                logger.debug("Rate limited, backing off %.1fs", delay)
                time.sleep(delay)
                delay *= BACKOFF_FACTOR
                continue
            elif r.status_code == 404:
                return None  # Not found, don't retry
            else:
                logger.debug("HTTP %s for %s", r.status_code, url)
        except requests.exceptions.Timeout:
            last_error = "timeout"
            logger.debug("Timeout on attempt %d for %s", attempt + 1, url)
        except requests.exceptions.RequestException as e:
            last_error = str(e)
            logger.debug("Request error %s: %s", url, e)
        
        if attempt < max_retries - 1:
            # Add jitter to avoid thundering herd
            sleep_time = delay + random.uniform(0, 0.5)
            time.sleep(sleep_time)
            delay *= BACKOFF_FACTOR
    
    logger.debug("All retries failed for %s: %s", url, last_error)
    return None


def _lookup_cik_from_ticker(ticker: str) -> Optional[str]:
    """Look up CIK from SEC ticker API - government source."""
    try:
        url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&ticker={ticker}&type=&dateb=&owner=include&count=40"
        r = _get_with_retry(url, SEC_HEADERS, max_retries=2)
        if r:
            # Extract CIK from the response
            import re
            match = re.search(r'CIK=(\d{10})', r.text)
            if match:
                return match.group(1)
    except Exception as e:
        logger.debug("CIK lookup failed for %s: %s", ticker, e)
    return None


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
        "Foundation Assets ($M)": lambda v, y: f"Foundation total assets were ${v:.1f} million in {y.replace('FY','')}.",
        "Number of Grants Awarded": lambda v, y: f"The foundation awarded {int(v):,} grants in {y.replace('FY','')}.",
        "Giving as % of Revenue": lambda v, y: f"Charitable giving represented {v:.3f}% of revenue in {y.replace('FY','')}.",
    }
    for metric, tmpl in templates.items():
        entry = company_data.get(metric)
        if entry and entry.get("value") is not None:
            try:
                lines.append(tmpl(entry["value"], entry.get("year", "")))
            except Exception:
                pass

    return "\n".join(lines)


def _get(url: str, headers: dict) -> Optional[requests.Response]:
    """Simple GET with retry - delegates to _get_with_retry."""
    return _get_with_retry(url, headers, MAX_RETRIES)


def _sec_xbrl_revenue(company: str, ciks: dict) -> str | None:
    """
    Fetch revenue from SEC XBRL API - government-regulated source.
    Uses retry logic and CIK lookup fallback.
    """
    key = company.lower().strip()
    cik = ciks.get(key)
    
    # If CIK not in cache, try to look up from ticker
    if not cik and key in COMPANY_TICKERS:
        ticker = COMPANY_TICKERS[key]
        cik = _lookup_cik_from_ticker(ticker)
        if cik:
            logger.info("Looked up CIK %s for %s from ticker %s", cik, company, ticker)
    
    if not cik:
        logger.debug("No CIK found for %s", company)
        return None
    
    # Pad CIK to 10 digits
    cik_padded = cik.zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json"
    
    r = _get_with_retry(url, SEC_HEADERS, max_retries=MAX_RETRIES)
    if not r:
        return None
    
    try:
        data = r.json()
        us_gaap = data.get("facts", {}).get("us-gaap", {})
        
        # Try multiple revenue tags (ordered by most likely to have data)
        for tag in ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet", "OperatingRevenues"]:
            tag_data = us_gaap.get(tag, {})
            if not tag_data:
                continue
            
            units = tag_data.get("units", {}).get("USD", [])
            if not units:
                continue
            
            # Get 10-K filings for fiscal year
            entries = [e for e in units if e.get("form") == "10-K" and e.get("fp") == "FY"]
            if not entries:
                continue
            
            # Get the most recent
            latest = sorted(entries, key=lambda x: x.get("end", ""), reverse=True)[0]
            val_b = latest["val"] / 1e9
            year = latest["end"][:4]
            return f"{company} total revenue of ${val_b:.2f} billion for fiscal year {year}."
    except Exception as e:
        logger.debug("XBRL parse error for %s: %s", company, e)
    
    return None


def _sec_10k_snippets(company: str, search_term: str, ciks: dict) -> str | None:
    """
    Fetch snippets from SEC EDGAR full-text search - government-regulated.
    Uses retry logic and multiple search term variations.
    """
    key = company.lower().strip()
    cik = ciks.get(key)
    
    # Try ticker lookup if CIK not found
    if not cik and key in COMPANY_TICKERS:
        ticker = COMPANY_TICKERS[key]
        cik = _lookup_cik_from_ticker(ticker)
    
    entity = f"CIK{cik.zfill(10)}" if cik else company

    # Single recent date range — the older two ranges almost never had
    # additional hits and added 2× the wall time on cache-miss / 429 paths.
    start_dt, end_dt = "2024-01-01", "2026-12-31"
    url = (
        "https://efts.sec.gov/LATEST/search-index"
        f"?q={requests.utils.quote(search_term)}"
        f"&entity={requests.utils.quote(entity)}"
        f"&forms=10-K&dateRange=custom&startdt={start_dt}&enddt={end_dt}"
    )

    r = _get_with_retry(url, SEC_HEADERS, max_retries=MAX_RETRIES)
    if not r:
        return None

    try:
        data = r.json()
        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            return None

        snippets = []
        for hit in hits[:3]:  # Check up to 3 hits
            for fh in hit.get("highlight", {}).values():
                snippets.extend(s.replace("<em>", "").replace("</em>", "") for s in fh[:3])

        if snippets:
            return " ".join(snippets)
    except Exception as e:
        logger.debug("10K search error for %s (%s): %s", company, search_term, e)

    return None


def _scrape_esg(company: str, esg_urls: dict) -> str | None:
    """
    Scrape ESG/sustainability pages from company websites.
    Lower priority than government sources.
    """
    parser = _get_parser()
    key = company.lower().strip()
    
    for url in esg_urls.get(key, []):
        r = _get_with_retry(url, HEADERS, max_retries=2)
        if r:
            soup = BeautifulSoup(r.text, parser)
            for t in soup(["script", "style", "nav", "footer", "header", "aside"]):
                t.decompose()
            text = soup.get_text(" ", strip=True)[:8000]
            if len(text) > 500:
                return text
        time.sleep(RATE_LIMIT_DELAY)  # Rate limit to avoid blocking
    
    return None


# ── Priority-ordered metric search terms for government sources ──────────────
# Each tuple's terms are tried in order until one succeeds (break-on-hit).
# Trimmed to the 1–2 most-distinctive phrases per category — additional
# synonyms only ran on the failure path and burned 30–60s per company on
# SEC throttling. The verified-fallback cache catches anything that misses.
SEC_METRIC_TERMS = [
    (["total revenue"], "edgar-revenue"),                    # XBRL is primary
    (["SAIDI"], "edgar-saidi"),                              # unique acronym
    (["scope 1 emissions", "greenhouse gas emissions"], "edgar-carbon"),
    (["J.D. Power"], "edgar-jdpower"),
    (["renewable energy", "clean energy"], "edgar-renewable"),
    (["charitable contributions", "philanthropy"], "edgar-charitable"),
]


def collect_for_company(company: str, cache: dict,
                        ciks: dict, esg_urls: dict) -> CollectedDoc:
    """
    Collect data for a company with priority for government sources.
    
    Priority order:
    1. SEC EDGAR XBRL (revenue) - government-regulated
    2. SEC EDGAR 10-K full-text search - government-regulated
    3. Company ESG pages - lower priority
    4. Cache fallback - only when live sources fail
    """
    live_chunks: list[str] = []
    sources: list[str] = []
    
    # ── Tier 1: SEC XBRL (government source) ─────────────────────────────────
    rev = _sec_xbrl_revenue(company, ciks)
    if rev:
        live_chunks.append(rev)
        sources.append("sec-xbrl")
        logger.info("Got XBRL revenue for %s", company)
    
    # Rate limit between SEC requests
    time.sleep(RATE_LIMIT_DELAY)
    
    # ── Tier 2: SEC 10-K Full-Text Search (government source) ───────────────
    for terms, label in SEC_METRIC_TERMS:
        for term in terms:
            snippet = _sec_10k_snippets(company, term, ciks)
            if snippet:
                live_chunks.append(snippet)
                sources.append(label)
                logger.debug("Got %s for %s", label, company)
                break  # Found data for this metric category
        time.sleep(RATE_LIMIT_DELAY)  # Rate limit to avoid SEC throttling
    
    # ── Tier 3: Company ESG pages (lower priority) ─────────────────────────
    esg = _scrape_esg(company, esg_urls)
    if esg:
        live_chunks.append(esg)
        sources.append("company-esg")
        logger.info("Got ESG data for %s", company)
    
    # ── Tier 4: Cache fallback (always available, used per-metric) ───────────
    # Always populate fallback_text so individual metrics that the live
    # sources didn't surface fall back to verified data. The extractor
    # tries raw_text first, so a metric is only ever pulled from the
    # fallback when live extraction returns N/A for it.
    fallback = _cache_to_text(company, cache)

    return CollectedDoc(
        company=company,
        source_url=_real_url(company),
        source_type="+".join(sources) if sources else "cache",
        raw_text="\n\n".join(c.strip() for c in live_chunks if c.strip()),
        fallback_text=fallback,
    )


MAX_PARALLEL_COMPANIES = 4  # SEC tolerates a few concurrent clients with our UA


def collect_all(companies: list[str]) -> list[CollectedDoc]:
    """
    Collect data for all companies in parallel.

    Each per-company task is independent (its own HTTP calls + a read-only
    snapshot of the cache), so we can run them in a small thread pool.
    Cache mutations (add_company, save) happen on the main thread
    before/after the pool to keep the dict thread-safe.
    """
    from modules.data_updater import COMPANY_CIKS, ESG_URLS

    cache = cache_module.load()

    # Pre-register every company so per-thread reads see a stable cache.
    for company in companies:
        cache_module.add_company(cache, company)

    docs_by_index: dict[int, CollectedDoc] = {}

    def _task(idx: int, company: str) -> tuple[int, CollectedDoc]:
        try:
            doc = collect_for_company(company, cache, COMPANY_CIKS, ESG_URLS)
            logger.info("Collected %s from sources: %s", company, doc.source_type)
        except Exception as e:
            logger.error("Error collecting %s: %s", company, e)
            doc = CollectedDoc(
                company=company,
                source_url=_real_url(company),
                source_type="cache",
                raw_text="",
                fallback_text=_cache_to_text(company, cache),
            )
        return idx, doc

    workers = min(MAX_PARALLEL_COMPANIES, max(1, len(companies)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_task, i, c) for i, c in enumerate(companies)]
        for fut in as_completed(futures):
            idx, doc = fut.result()
            docs_by_index[idx] = doc

    cache_module.save(cache)
    return [docs_by_index[i] for i in range(len(companies))]


SIMULATED_DATA = cache_module.VERIFIED_DEFAULTS
