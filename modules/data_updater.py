"""
data_updater.py
Automatically fetches the latest data for every company in the cache.

CHANGES:
  - Added _fetch_charitable_giving() using ProPublica Nonprofit Explorer API
    (IRS Form 990 data — free, no key required) and CSR page scraping
  - Added "Charitable Giving ($M)" to the update_company() loop
  - Cloud-safe: all network calls wrapped in try/except, scheduler failures
    are logged but never crash the app
"""

import logging
import re
import threading
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from modules import data_cache as cache_module
from modules.config import COMPANY_FOUNDATIONS

logger = logging.getLogger(__name__)

# ── SEC identifiers ────────────────────────────────────────────────────────────
COMPANY_CIKS: dict[str, str] = {
    "con edison":               "0001047862",
    "consolidated edison":      "0001047862",
    "national grid":            "0001000694",
    "pacific gas and electric": "0001004440",
    "duke energy":              "0001326160",
    "eversource energy":        "0000072741",
    "southern company":         "0000092122",
}

COMPANY_TICKERS: dict[str, str] = {
    "con edison":               "ED",
    "consolidated edison":      "ED",
    "national grid":            "NGG",
    "pacific gas and electric": "PCG",
    "duke energy":              "DUK",
    "eversource energy":        "ES",
    "southern company":         "SO",
}

SEC_HEADERS = {
    "User-Agent": "UtilityBenchmark contact@example.com",
    "Accept":     "application/json",
}
SCRAPE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}
TIMEOUT = 12

# ── Helpers ───────────────────────────────────────────────────────────────────

def _get(url: str, headers: dict, timeout: int = TIMEOUT) -> requests.Response | None:
    try:
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        if r.status_code == 200:
            return r
        logger.debug("HTTP %s for %s", r.status_code, url)
    except Exception as e:
        logger.debug("Request failed %s: %s", url, e)
    return None


def _html_text(html: str) -> str:
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script", "style", "nav", "footer", "header"]):
        t.decompose()
    return soup.get_text(" ", strip=True)


def _fy_from_date(date_str: str) -> str:
    """'2025-12-31' -> 'FY2025'"""
    return f"FY{date_str[:4]}" if date_str else "FY?"


# ── Source A: SEC EDGAR XBRL — Revenue ────────────────────────────────────────

def _fetch_edgar_revenue(company: str) -> tuple[float, str] | None:
    cik = COMPANY_CIKS.get(company.lower().strip())
    if not cik:
        cik = _lookup_cik(company)
        if cik:
            COMPANY_CIKS[company.lower().strip()] = cik

    if not cik:
        return None

    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    r = _get(url, SEC_HEADERS)
    if not r:
        return None

    try:
        us_gaap = r.json().get("facts", {}).get("us-gaap", {})
        for tag in [
            "Revenues",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "RevenueFromContractWithCustomerIncludingAssessedTax",
            "SalesRevenueNet",
        ]:
            entries = [
                e for e in us_gaap.get(tag, {}).get("units", {}).get("USD", [])
                if e.get("form") == "10-K" and e.get("fp") == "FY"
            ]
            if not entries:
                continue
            latest = sorted(entries, key=lambda x: x.get("end", ""), reverse=True)[0]
            revenue_b = round(latest["val"] / 1e9, 2)
            fy = _fy_from_date(latest.get("end", ""))
            logger.info("EDGAR revenue %s: $%.2fB %s", company, revenue_b, fy)
            return revenue_b, fy
    except Exception as e:
        logger.warning("EDGAR XBRL parse failed for %s: %s", company, e)
    return None


def _lookup_cik(company: str) -> str | None:
    url = f'https://efts.sec.gov/LATEST/search-index?q=%22{requests.utils.quote(company)}%22&forms=10-K&dateRange=custom&startdt=2023-01-01&enddt=2026-01-01'
    r = _get(url, SEC_HEADERS)
    if not r:
        return None
    try:
        hits = r.json().get("hits", {}).get("hits", [])
        if hits:
            entity_id = hits[0].get("_source", {}).get("entity_id", "")
            if entity_id:
                return entity_id.zfill(10)
    except Exception:
        pass
    return None


# ── Source B: SEC EDGAR 10-K full-text ────────────────────────────────────────

EXTRACT_PATTERNS: dict[str, list[re.Pattern]] = {
    "Renewable Energy %": [
        re.compile(r'(?:renewable|clean)\s+energy\D{0,50}?(\d[\d,]*\.?\d*)\s*(%|percent)', re.I),
        re.compile(r'(\d[\d,]*\.?\d*)\s*(?:%|percent)\s+of\s+(?:electric\s+)?(?:supply|generation|electricity)', re.I),
    ],
    "Outage Frequency": [
        re.compile(r'SAIDI\D{0,50}?(\d[\d,]*\.?\d*)\s*(minutes?)?', re.I),
        re.compile(r'(?:system\s+average\s+interruption\s+duration)\D{0,50}?(\d[\d,]*\.?\d*)', re.I),
    ],
    "Customer Satisfaction Score": [
        re.compile(r'(?:J\.?\s*D\.?\s*Power|satisfaction\s+(?:score|index))\D{0,60}?(\d{3})\b', re.I),
        re.compile(r'(\d{3})\s*(?:out\s+of\s+1[,\s]?000|/\s*1[,\s]?000)', re.I),
    ],
    "Carbon Emissions (MT CO2)": [
        re.compile(r'(?:scope\s*1|greenhouse\s+gas|GHG|carbon)\s+emissions\D{0,50}?(\d[\d,]*\.?\d*)\s*(million\s+metric\s+tons?|million\s+MT|Mt\b|MMT)', re.I),
        re.compile(r'(\d[\d,]*\.?\d*)\s*(million\s+metric\s+tons?|million\s+MT|Mt\b)\s+(?:of\s+)?CO2', re.I),
    ],
    "Charitable Giving ($M)": [
        # "$34 million in charitable contributions"
        re.compile(r'\$\s*(\d[\d,]*\.?\d*)\s*(million|billion)\s+(?:in\s+)?(?:charitable|philanthropic|community)\s+(?:contributions?|giving|investments?|grants?)', re.I),
        # "charitable contributions of $34 million"
        re.compile(r'(?:charitable|philanthropic|community)\s+(?:contributions?|giving|investments?|grants?)\D{0,40}?\$\s*(\d[\d,]*\.?\d*)\s*(million|billion)?', re.I),
        # "donated $34 million" / "contributed $34 million"
        re.compile(r'(?:donated|contributed|invested|awarded)\s+(?:more\s+than\s+)?\$\s*(\d[\d,]*\.?\d*)\s*(million|billion)', re.I),
        # "foundation giving totaled $34 million"
        re.compile(r'(?:foundation|corporate)\s+(?:giving|grants?)\D{0,40}?\$\s*(\d[\d,]*\.?\d*)\s*(million|billion)?', re.I),
        # "grants paid" (from 990 language)
        re.compile(r'grants?\s+(?:paid|awarded|distributed)\D{0,40}?\$\s*(\d[\d,]*\.?\d*)\s*(million|billion)?', re.I),
    ],
}

SCALE = {"billion": 1e9, "million": 1e6, "thousand": 1e3}
DIVISOR = {
    "revenue":                      1e9,
    "renewable energy %":           1.0,
    "outage frequency":             1.0,
    "customer satisfaction score":  10.0,
    "carbon emissions (mt co2)":    1e6,
    "charitable giving ($m)":       1e6,
}


def _parse_raw(raw: str, metric: str) -> float | None:
    NUM = re.compile(r"\d[\d,]*\.?\d*")
    m = NUM.search(raw)
    if not m:
        return None
    num = float(m.group().replace(",", ""))
    for w, mult in SCALE.items():
        if w in raw.lower():
            num *= mult
            break
    div = DIVISOR.get(metric.lower().strip(), 1.0)
    return round(num / div, 4)


def _sec_10k_snippet(company: str, search_term: str) -> tuple[str, str] | None:
    cik = COMPANY_CIKS.get(company.lower().strip())
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
        filing_date = hits[0].get("_source", {}).get("file_date", "")
        return ("|".join(snippets), filing_date) if snippets else None
    except Exception as e:
        logger.debug("EDGAR snippet parse error: %s", e)
    return None


def _fetch_edgar_esg_metric(company: str, metric: str) -> tuple[float, str, str] | None:
    search_terms = {
        "Renewable Energy %":          "renewable energy percentage",
        "Outage Frequency":            "SAIDI",
        "Customer Satisfaction Score": "J.D. Power",
        "Carbon Emissions (MT CO2)":   "scope 1 emissions",
        "Charitable Giving ($M)":      "charitable contributions",
    }
    term = search_terms.get(metric)
    if not term:
        return None

    result = _sec_10k_snippet(company, term)
    if not result:
        return None

    snippet_text, file_date = result
    for pat in EXTRACT_PATTERNS.get(metric, []):
        m = pat.search(snippet_text)
        if m:
            num  = m.group(1)
            unit = m.group(2) if m.lastindex and m.lastindex >= 2 and m.group(2) else ""
            raw  = f"{num} {unit}".strip()
            val  = _parse_raw(raw, metric)
            if val is not None:
                year = f"FY{file_date[:4]}" if file_date else "FY?"
                logger.info("EDGAR 10-K %s / %s: %s -> %.4f (%s)", company, metric, raw, val, year)
                return val, year, "edgar-10k"
    return None


# ── Source C: ESG page scrape ─────────────────────────────────────────────────

ESG_URLS: dict[str, list[str]] = {
    "con edison": [
        "https://lite.conedison.com/ehs/2024-sustainability-report/our-commodities/electric/",
        "https://lite.conedison.com/ehs/2024-sustainability-report/clean-energy-environment/managing-our-emissions/",
        "https://lite.conedison.com/ehs/2024-sustainability-report/community/",
    ],
    "consolidated edison": [
        "https://lite.conedison.com/ehs/2024-sustainability-report/our-commodities/electric/",
        "https://lite.conedison.com/ehs/2024-sustainability-report/community/",
    ],
    "duke energy":              [
        "https://p-micro.duke-energy.com/impact/esg-resources",
        "https://p-micro.duke-energy.com/impact/community",
    ],
    "pacific gas and electric": [
        "https://www.pgecorp.com/corp_responsibility/reports/2024/en/index.html",
        "https://www.pgecorp.com/corp_responsibility/reports/2024/en/community.html",
    ],
    "national grid":            [
        "https://www.nationalgrid.com/responsibility/environment",
        "https://www.nationalgrid.com/responsibility/community",
    ],
    "eversource energy":        [
        "https://www.eversource.com/content/residential/about/sustainability/reporting-and-disclosures",
        "https://www.eversource.com/content/residential/about/community",
    ],
    "southern company":         [
        "https://www.southerncompany.com/our-values/environment.html",
        "https://www.southerncompany.com/our-values/communities.html",
    ],
}


def _fetch_esg_scrape(company: str, metric: str) -> tuple[float, str, str] | None:
    """Scrape company ESG pages for metric values."""
    for url in ESG_URLS.get(company.lower().strip(), []):
        r = _get(url, SCRAPE_HEADERS)
        if not r:
            continue
        text = _html_text(r.text)
        for pat in EXTRACT_PATTERNS.get(metric, []):
            m = pat.search(text)
            if m:
                num  = m.group(1)
                unit = m.group(2) if m.lastindex and m.lastindex >= 2 and m.group(2) else ""
                raw  = f"{num} {unit}".strip()
                val  = _parse_raw(raw, metric)
                if val is not None:
                    year = f"FY{datetime.now().year - 1}"
                    logger.info("ESG scrape %s / %s: %.4f (%s)", company, metric, val, year)
                    return val, year, "esg-scrape"
    return None


# ── Source D: J.D. Power press release scrape ─────────────────────────────────

def _fetch_jdpower_score(company: str) -> tuple[float, str, str] | None:
    url = "https://www.jdpower.com/business/press-releases/2025-us-electric-utility-residential-customer-satisfaction-study"
    r = _get(url, SCRAPE_HEADERS)
    if not r:
        return None

    text = _html_text(r.text)

    company_patterns = [
        re.compile(rf'{re.escape(company.split()[0])}\D{{0,80}}?(\d{{3}})\b', re.I),
        re.compile(rf'(\d{{3}})\D{{0,30}}{re.escape(company.split()[0])}', re.I),
    ]
    for pat in company_patterns:
        m = pat.search(text)
        if m:
            raw_score = int(m.group(1))
            if 300 <= raw_score <= 1000:
                val = round(raw_score / 10, 1)
                logger.info("JD Power score for %s: %d/1000 -> %.1f/100", company, raw_score, val)
                return val, "2025", "jdpower-scrape"

    avg_pat = re.compile(r'overall\s+satisfaction\D{0,30}?(\d{3})\b', re.I)
    m = avg_pat.search(text)
    if m:
        avg = int(m.group(1))
        if 300 <= avg <= 1000:
            val = round(avg / 10, 1)
            logger.info("JD Power avg (no company score found for %s): %.1f/100", company, val)
            return val, "2025", "jdpower-industry-avg"

    return None


# ── Source E: ProPublica Nonprofit Explorer — Foundation Giving ────────────────

def _fetch_propublica_990(company: str) -> tuple[float, str, str] | None:
    """
    Look up the company's foundation on ProPublica's Nonprofit Explorer API
    (free, no key required). Extracts total grants paid from the most recent
    IRS Form 990.

    API docs: https://projects.propublica.org/nonprofits/api
    """
    foundation_info = COMPANY_FOUNDATIONS.get(company.lower().strip())
    if not foundation_info:
        return None

    ein = foundation_info.get("ein")
    if not ein:
        return None

    # Try the specific organization endpoint first (most reliable)
    url = f"https://projects.propublica.org/nonprofits/api/v2/organizations/{ein}.json"
    r = _get(url, SCRAPE_HEADERS, timeout=15)
    if not r:
        return None

    try:
        data = r.json()
        org = data.get("organization", {})
        filings = data.get("filings_with_data", [])

        if not filings:
            return None

        # Get most recent filing
        latest = filings[0]
        tax_year = latest.get("tax_prd_yr")

        # Try totfuncexpns (total functional expenses) or totgftgrntspd
        # (total grants/gifts paid) as a proxy for charitable giving
        grants = None
        for field in ["totgftgrntspd", "grsrcptspublcuse", "totfuncexpns"]:
            val = latest.get(field)
            if val and val > 0:
                grants = val
                break

        if not grants:
            # Fall back to total revenue as an approximation for foundations
            # whose primary purpose is grantmaking
            grants = latest.get("totrevenue")

        if grants and grants > 0:
            val_m = round(grants / 1e6, 2)
            year = f"FY{tax_year}" if tax_year else "FY?"
            logger.info(
                "ProPublica 990 %s: $%.2fM grants (%s)",
                foundation_info["foundation_name"], val_m, year,
            )
            return val_m, year, "propublica-990"

    except Exception as e:
        logger.debug("ProPublica parse failed for %s: %s", company, e)

    return None


def _fetch_charitable_csr_scrape(company: str) -> tuple[float, str, str] | None:
    """
    Scrape CSR / community pages for charitable giving figures.
    Uses the same ESG_URLS list — community pages often have giving totals.
    """
    for url in ESG_URLS.get(company.lower().strip(), []):
        # Prefer community/philanthropy URLs
        if not any(kw in url.lower() for kw in ("community", "philanthrop", "giving", "foundation")):
            continue
        r = _get(url, SCRAPE_HEADERS)
        if not r:
            continue
        text = _html_text(r.text)
        for pat in EXTRACT_PATTERNS.get("Charitable Giving ($M)", []):
            m = pat.search(text)
            if m:
                num  = m.group(1)
                unit = m.group(2) if m.lastindex and m.lastindex >= 2 and m.group(2) else ""
                raw  = f"{num} {unit}".strip()
                val  = _parse_raw(raw, "Charitable Giving ($M)")
                if val is not None and val > 0:
                    year = f"FY{datetime.now().year - 1}"
                    logger.info("CSR scrape charitable %s: $%.2fM (%s)", company, val, year)
                    return val, year, "csr-scrape"
    return None


# ── yfinance Revenue fallback ─────────────────────────────────────────────────

def _fetch_yfinance_revenue(company: str) -> tuple[float, str] | None:
    ticker = COMPANY_TICKERS.get(company.lower().strip())
    if not ticker:
        return None
    try:
        import yfinance as yf
        fin = yf.Ticker(ticker).financials
        if fin is None or fin.empty:
            return None
        rev_rows = [i for i in fin.index if "revenue" in str(i).lower()]
        if not rev_rows:
            return None
        val = fin.loc[rev_rows[0]].iloc[0]
        if val and not (isinstance(val, float) and val != val):
            rev_b = round(val / 1e9, 2)
            year = f"FY{fin.columns[0].year}"
            logger.info("yfinance revenue %s: $%.2fB %s", company, rev_b, year)
            return rev_b, year
    except Exception as e:
        logger.debug("yfinance failed for %s: %s", company, e)
    return None


# ── Main update logic ─────────────────────────────────────────────────────────

def _is_newer(new_year: str, cached_year: str) -> bool:
    def extract_year(y: str) -> int:
        m = re.search(r'\d{4}', str(y))
        return int(m.group()) if m else 0
    return extract_year(new_year) > extract_year(cached_year)


def update_company(company: str, cache: dict) -> bool:
    changed = False
    cache_module.add_company(cache, company)

    # ── Revenue ───────────────────────────────────────────────────────────────
    result = _fetch_edgar_revenue(company)
    if not result:
        result_yf = _fetch_yfinance_revenue(company)
        if result_yf:
            result = result_yf

    if result:
        new_val, new_year = result
        cached = cache_module.get_value(cache, company, "Revenue")
        if not cached or _is_newer(new_year, cached.get("year", "FY0")):
            cache_module.set_value(cache, company, "Revenue", new_val, new_year, "edgar-xbrl")
            logger.info("Updated Revenue for %s: $%.2fB (%s)", company, new_val, new_year)
            changed = True

    # ── ESG metrics ───────────────────────────────────────────────────────────
    esg_metrics = [
        "Renewable Energy %",
        "Outage Frequency",
        "Carbon Emissions (MT CO2)",
    ]
    for metric in esg_metrics:
        result = _fetch_edgar_esg_metric(company, metric)
        if not result:
            result = _fetch_esg_scrape(company, metric)

        if result:
            new_val, new_year, source = result
            cached = cache_module.get_value(cache, company, metric)
            if not cached or _is_newer(new_year, cached.get("year", "FY0")):
                cache_module.set_value(cache, company, metric, new_val, new_year, source)
                logger.info("Updated %s for %s: %.4f (%s)", metric, company, new_val, new_year)
                changed = True

    # ── Customer Satisfaction ─────────────────────────────────────────────────
    result = _fetch_jdpower_score(company)
    if result:
        new_val, new_year, source = result
        cached = cache_module.get_value(cache, company, "Customer Satisfaction Score")
        if not cached or _is_newer(new_year, cached.get("year", "0")):
            cache_module.set_value(cache, company, "Customer Satisfaction Score", new_val, new_year, source)
            logger.info("Updated Customer Sat for %s: %.1f (%s)", company, new_val, new_year)
            changed = True

    # ── Charitable Giving ─────────────────────────────────────────────────────
    # Priority: 1) ProPublica 990  2) EDGAR 10-K  3) CSR page scrape
    result = _fetch_propublica_990(company)
    if not result:
        result = _fetch_edgar_esg_metric(company, "Charitable Giving ($M)")
    if not result:
        result = _fetch_charitable_csr_scrape(company)

    if result:
        new_val, new_year, source = result
        cached = cache_module.get_value(cache, company, "Charitable Giving ($M)")
        if not cached or _is_newer(new_year, cached.get("year", "FY0")):
            cache_module.set_value(cache, company, "Charitable Giving ($M)", new_val, new_year, source)
            logger.info("Updated Charitable Giving for %s: $%.2fM (%s from %s)", company, new_val, new_year, source)
            changed = True

    return changed


def check_for_updates(companies: list[str], force: bool = False) -> dict:
    cache = cache_module.load()

    if not force:
        last = cache.get("last_checked", "never")
        if last != "never":
            try:
                last_dt = datetime.fromisoformat(last)
                hours_ago = (datetime.now(timezone.utc) - last_dt).total_seconds() / 3600
                if hours_ago < 12:
                    logger.info("Cache fresh (%.1fh ago) — skipping update", hours_ago)
                    return cache
            except Exception:
                pass

    logger.info("Checking for updates for %d companies...", len(companies))
    any_changed = False

    for company in companies:
        try:
            changed = update_company(company, cache)
            if changed:
                any_changed = True
        except Exception as e:
            logger.error("Update failed for %s: %s", company, e)
        time.sleep(0.5)

    cache_module.mark_checked(cache)
    cache_module.save(cache)

    if any_changed:
        logger.info("Cache updated with new data.")
    else:
        logger.info("No new data found.")

    return cache


def start_background_scheduler(companies: list[str]) -> None:
    """
    Start a background scheduler for daily updates.
    Fully wrapped in try/except so it never crashes the app — this is
    critical for cloud deployments where apscheduler may not be installed
    or threading may behave differently.
    """
    def _run():
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            scheduler = BackgroundScheduler()
            scheduler.add_job(
                lambda: check_for_updates(companies),
                trigger="interval",
                hours=24,
                id="daily_update",
                replace_existing=True,
            )
            scheduler.start()
            logger.info("Daily update scheduler started.")
        except ImportError:
            logger.info("apscheduler not installed — daily auto-update disabled. "
                        "Updates still run on each app startup.")
        except Exception as e:
            logger.warning("Scheduler failed to start: %s — daily updates disabled.", e)

    try:
        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
    except Exception as e:
        logger.warning("Could not start scheduler thread: %s", e)
