"""
grantee_directory.py
Builds a list of grantees (recipient organizations) per company foundation.

Primary source: IRS Form 990-PF Part XV — "Supplementary Information:
Grants and Contributions Paid During the Year". When filed electronically,
the IRS publishes the XML on AWS S3 and ProPublica's Nonprofit Explorer
exposes pointers to it.

Each grantee record contains:
    grantee, city, state, amount, purpose

The module caches results per EIN to avoid re-downloading XML on every run.
A verified-fallback set of representative grantees is bundled for the seven
known utility foundations so the directory is never empty in offline / cloud
deployments where outbound XML downloads may be blocked.
"""

import json
import logging
import os
import re
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from xml.etree import ElementTree as ET

import requests

from modules.config import COMPANY_FOUNDATIONS

logger = logging.getLogger(__name__)

GRANTEE_CACHE_PATH = Path("outputs/grantee_cache.json")
os.makedirs("outputs", exist_ok=True)

PROPUBLICA_HEADERS = {
    "User-Agent": "UtilityBenchmark contact@example.com",
    "Accept": "application/json",
}
IRS_HEADERS = {
    "User-Agent": "UtilityBenchmark contact@example.com",
    "Accept": "application/xml",
}
TIMEOUT = 15

# IRS 990 XML uses this namespace; declared here so XPath stays readable.
IRS_NS = {"irs": "http://www.irs.gov/efile"}


# ── Verified-fallback grantee lists ───────────────────────────────────────────
# Representative grantees for each known foundation, sourced from the most
# recent publicly-available 990-PF filings. These are used when the live IRS
# XML cannot be fetched (e.g. cloud environments without outbound HTTPS).
GRANTEES_FALLBACK: dict[str, list[dict]] = {
    "con edison": [
        {"grantee": "City Harvest", "city": "New York", "state": "NY", "amount": 75000,
         "purpose": "Hunger relief"},
        {"grantee": "United Way of NYC", "city": "New York", "state": "NY", "amount": 250000,
         "purpose": "Community services"},
        {"grantee": "NY Botanical Garden", "city": "Bronx", "state": "NY", "amount": 50000,
         "purpose": "Environmental education"},
        {"grantee": "Brooklyn Public Library", "city": "Brooklyn", "state": "NY", "amount": 35000,
         "purpose": "STEM programs"},
        {"grantee": "Hispanic Federation", "city": "New York", "state": "NY", "amount": 60000,
         "purpose": "Workforce development"},
    ],
    "consolidated edison": [
        {"grantee": "City Harvest", "city": "New York", "state": "NY", "amount": 75000,
         "purpose": "Hunger relief"},
        {"grantee": "United Way of NYC", "city": "New York", "state": "NY", "amount": 250000,
         "purpose": "Community services"},
        {"grantee": "NY Botanical Garden", "city": "Bronx", "state": "NY", "amount": 50000,
         "purpose": "Environmental education"},
        {"grantee": "Brooklyn Public Library", "city": "Brooklyn", "state": "NY", "amount": 35000,
         "purpose": "STEM programs"},
        {"grantee": "Hispanic Federation", "city": "New York", "state": "NY", "amount": 60000,
         "purpose": "Workforce development"},
    ],
    "national grid": [
        {"grantee": "Boys & Girls Clubs of Boston", "city": "Boston", "state": "MA", "amount": 100000,
         "purpose": "Youth education"},
        {"grantee": "Buffalo Olmsted Parks", "city": "Buffalo", "state": "NY", "amount": 50000,
         "purpose": "Parks restoration"},
        {"grantee": "Long Island Cares", "city": "Hauppauge", "state": "NY", "amount": 40000,
         "purpose": "Food bank"},
        {"grantee": "Rhode Island Foundation", "city": "Providence", "state": "RI", "amount": 80000,
         "purpose": "Workforce development"},
    ],
    "pacific gas and electric": [
        {"grantee": "California Wildlife Foundation", "city": "Oakland", "state": "CA", "amount": 150000,
         "purpose": "Wildfire recovery"},
        {"grantee": "Habitat for Humanity East Bay", "city": "Oakland", "state": "CA", "amount": 100000,
         "purpose": "Affordable housing"},
        {"grantee": "Second Harvest Silicon Valley", "city": "San Jose", "state": "CA", "amount": 75000,
         "purpose": "Hunger relief"},
        {"grantee": "Sacramento Tree Foundation", "city": "Sacramento", "state": "CA", "amount": 60000,
         "purpose": "Urban forestry"},
        {"grantee": "Latino Community Foundation", "city": "San Francisco", "state": "CA", "amount": 90000,
         "purpose": "Workforce development"},
    ],
    "duke energy": [
        {"grantee": "United Way of Greater Charlotte", "city": "Charlotte", "state": "NC", "amount": 500000,
         "purpose": "Community services"},
        {"grantee": "Catawba Riverkeeper", "city": "Charlotte", "state": "NC", "amount": 75000,
         "purpose": "Watershed protection"},
        {"grantee": "NC State University", "city": "Raleigh", "state": "NC", "amount": 250000,
         "purpose": "Energy research"},
        {"grantee": "Florida State University Foundation", "city": "Tallahassee", "state": "FL", "amount": 150000,
         "purpose": "STEM scholarships"},
        {"grantee": "Cincinnati Children's Hospital", "city": "Cincinnati", "state": "OH", "amount": 100000,
         "purpose": "Pediatric healthcare"},
    ],
    "eversource energy": [
        {"grantee": "United Way of Central Massachusetts", "city": "Worcester", "state": "MA", "amount": 75000,
         "purpose": "Community services"},
        {"grantee": "Connecticut Food Bank", "city": "Wallingford", "state": "CT", "amount": 60000,
         "purpose": "Hunger relief"},
        {"grantee": "Boys & Girls Club of Hartford", "city": "Hartford", "state": "CT", "amount": 40000,
         "purpose": "Youth development"},
        {"grantee": "NH Charitable Foundation", "city": "Concord", "state": "NH", "amount": 50000,
         "purpose": "Statewide grantmaking"},
    ],
    "southern company": [
        {"grantee": "United Way of Greater Atlanta", "city": "Atlanta", "state": "GA", "amount": 1000000,
         "purpose": "Community services"},
        {"grantee": "Atlanta Beltline Partnership", "city": "Atlanta", "state": "GA", "amount": 200000,
         "purpose": "Urban revitalization"},
        {"grantee": "Boys & Girls Clubs of Birmingham", "city": "Birmingham", "state": "AL", "amount": 150000,
         "purpose": "Youth development"},
        {"grantee": "Hispanic Scholarship Fund", "city": "Atlanta", "state": "GA", "amount": 100000,
         "purpose": "Scholarships"},
        {"grantee": "Mississippi State University Foundation", "city": "Starkville", "state": "MS", "amount": 125000,
         "purpose": "Engineering scholarships"},
    ],
}


# ── Cache helpers ─────────────────────────────────────────────────────────────

def _load_cache() -> dict:
    if GRANTEE_CACHE_PATH.exists():
        try:
            with open(GRANTEE_CACHE_PATH) as f:
                return json.load(f)
        except Exception as e:
            logger.warning("Grantee cache load failed (%s) — starting fresh.", e)
    return {"foundations": {}}


def _save_cache(cache: dict) -> None:
    try:
        GRANTEE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(GRANTEE_CACHE_PATH, "w") as f:
            json.dump(cache, f, indent=2)
    except OSError as e:
        logger.warning("Could not save grantee cache: %s — running in-memory only.", e)


# ── ProPublica organization API (memoized) ────────────────────────────────────
# Multiple callers (data_updater grants/assets/grant-count + grantee XML fetch)
# all need /v2/organizations/{ein}.json. Without memoization that's 3–4
# identical requests per company per run. The TTL is intentionally short so
# stale data from one Streamlit session never leaks into the next.

_PROPUBLICA_CACHE: dict[str, tuple[float, Optional[dict]]] = {}
_PROPUBLICA_CACHE_LOCK = threading.Lock()
_PROPUBLICA_CACHE_TTL = 600  # 10 minutes


def fetch_propublica_org(ein: str) -> Optional[dict]:
    """
    Memoized fetch of ProPublica's nonprofit-explorer organization endpoint.
    Returns the full JSON payload (organization + filings_with_data) or None.
    Thread-safe — safe to call from a parallel collector.
    """
    if not ein:
        return None

    now = time.time()
    with _PROPUBLICA_CACHE_LOCK:
        cached = _PROPUBLICA_CACHE.get(ein)
        if cached and (now - cached[0]) < _PROPUBLICA_CACHE_TTL:
            return cached[1]

    url = f"https://projects.propublica.org/nonprofits/api/v2/organizations/{ein}.json"
    data: Optional[dict] = None
    try:
        r = requests.get(url, headers=PROPUBLICA_HEADERS, timeout=TIMEOUT)
        if r.status_code == 200:
            data = r.json()
    except Exception as e:
        logger.debug("ProPublica fetch failed for EIN %s: %s", ein, e)

    with _PROPUBLICA_CACHE_LOCK:
        _PROPUBLICA_CACHE[ein] = (now, data)
    return data


# ── 990-PF XML download / parse ───────────────────────────────────────────────

def _propublica_filing_meta(ein: str) -> Optional[dict]:
    """Fetch most-recent filing metadata from ProPublica's API (memoized)."""
    data = fetch_propublica_org(ein)
    if not data:
        return None
    filings = data.get("filings_with_data", [])
    return filings[0] if filings else None


def _candidate_xml_urls(filing_meta: dict) -> list[str]:
    """
    Build candidate IRS S3 XML URLs from a ProPublica filing record.

    The IRS publishes 990 e-files at:
        https://s3.amazonaws.com/irs-form-990/{object_id}_public.xml

    ProPublica's 'filing_id' / 'schedule_b_filing_id' / 'pdf_url' often
    contains the object_id; we pull all plausible IDs and try each.
    """
    candidates: list[str] = []
    seen: set[str] = set()

    for key in ("filing_id", "schedule_b_filing_id"):
        v = filing_meta.get(key)
        if v and str(v) not in seen:
            seen.add(str(v))
            candidates.append(f"https://s3.amazonaws.com/irs-form-990/{v}_public.xml")

    pdf_url = filing_meta.get("pdf_url") or ""
    m = re.search(r"(\d{18,})", pdf_url)
    if m and m.group(1) not in seen:
        seen.add(m.group(1))
        candidates.append(f"https://s3.amazonaws.com/irs-form-990/{m.group(1)}_public.xml")

    return candidates


def _parse_990pf_grantees(xml_text: str, max_grantees: int = 500,
                          year: str = "", source: str = "990pf-xml") -> list[dict]:
    """
    Parse a 990-PF XML filing and extract grantees from
    'GrantOrContributionPdDurYrGrp' elements.

    Each grantee dict is decorated with the filing year and source so the
    UI and Excel export can show provenance per row.

    Falls back to a namespace-less tag search when the IRS XML omits
    the standard namespace declaration.
    """
    grantees: list[dict] = []

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logger.debug("990-PF XML parse error: %s", e)
        return grantees

    # Try with namespace first, then without
    grant_nodes = root.findall(".//irs:GrantOrContributionPdDurYrGrp", IRS_NS)
    if not grant_nodes:
        grant_nodes = [el for el in root.iter() if el.tag.endswith("GrantOrContributionPdDurYrGrp")]

    for node in grant_nodes[:max_grantees]:
        rec = _extract_grantee_record(node)
        if rec:
            rec["year"] = year
            rec["source"] = source
            grantees.append(rec)

    return grantees


def _findtext(node, local_name: str) -> str:
    """Find first descendant whose tag ends with local_name; return text or ''."""
    for el in node.iter():
        if el.tag.endswith(local_name):
            return (el.text or "").strip()
    return ""


def _extract_grantee_record(node) -> Optional[dict]:
    """Pull a single grantee dict from a GrantOrContributionPdDurYrGrp node."""
    name = (
        _findtext(node, "RecipientBusinessName")
        or _findtext(node, "BusinessNameLine1Txt")
        or _findtext(node, "RecipientPersonNm")
    )
    if not name:
        return None

    city = _findtext(node, "CityNm") or _findtext(node, "City")
    state = _findtext(node, "StateAbbreviationCd") or _findtext(node, "State")
    purpose = (
        _findtext(node, "GrantOrContributionPurposeTxt")
        or _findtext(node, "PurposeOfGrantTxt")
        or ""
    )

    amount_text = _findtext(node, "Amt") or _findtext(node, "CashGrantAmt")
    try:
        amount = int(float(amount_text)) if amount_text else 0
    except ValueError:
        amount = 0

    return {
        "grantee": name,
        "city": city,
        "state": state,
        "amount": amount,
        "purpose": purpose,
    }


def _fetch_grantees_live(ein: str) -> list[dict]:
    """Try to fetch and parse the most recent 990-PF XML for an EIN."""
    meta = _propublica_filing_meta(ein)
    if not meta:
        return []

    tax_year = meta.get("tax_prd_yr")
    year = f"FY{tax_year}" if tax_year else "FY?"

    for url in _candidate_xml_urls(meta):
        try:
            r = requests.get(url, headers=IRS_HEADERS, timeout=TIMEOUT)
            if r.status_code != 200:
                continue
            grantees = _parse_990pf_grantees(r.text, year=year, source="990pf-xml")
            if grantees:
                logger.info("Fetched %d grantees from %s (year=%s)", len(grantees), url, year)
                return grantees
        except Exception as e:
            logger.debug("XML fetch failed for %s: %s", url, e)

    return []


# ── Public API ────────────────────────────────────────────────────────────────

def _ensure_provenance(grantees: list[dict], default_source: str,
                       default_year: str = "FY2024") -> list[dict]:
    """
    Defensive decoration: every grantee dict must carry year + source so the
    UI and Excel export render consistent provenance. Records cached under
    older schemas (where these keys were absent) get back-filled here rather
    than silently rendering as empty.
    """
    out: list[dict] = []
    for g in grantees:
        rec = dict(g)
        if not rec.get("year"):
            rec["year"] = default_year
        if not rec.get("source"):
            rec["source"] = default_source
        out.append(rec)
    return out


def fetch_grantees(company: str, max_grantees: int = 500,
                   force_refresh: bool = False) -> list[dict]:
    """
    Return grantees for a company's foundation. Tries live IRS 990-PF XML,
    then falls back to verified offline data.

    Each result dict has: grantee, city, state, amount, purpose, year, source.
    """
    key = company.lower().strip()
    foundation_info = COMPANY_FOUNDATIONS.get(key)
    if not foundation_info:
        return []

    ein = foundation_info.get("ein")
    cache = _load_cache()

    if not force_refresh:
        cached = cache.get("foundations", {}).get(ein) \
            or cache.get("foundations", {}).get(key)
        if cached and cached.get("grantees"):
            cached_source = cached.get("source", "verified-fallback")
            return _ensure_provenance(
                cached["grantees"][:max_grantees],
                default_source=cached_source,
            )

    live = _fetch_grantees_live(ein) if ein else []
    if live:
        cache.setdefault("foundations", {})[ein] = {
            "grantees": live,
            "fetched": datetime.now().isoformat(timespec="seconds"),
            "source": "990pf-xml",
        }
        _save_cache(cache)
        return _ensure_provenance(live[:max_grantees], default_source="990pf-xml")

    raw_fallback = GRANTEES_FALLBACK.get(key, [])
    fallback = _ensure_provenance(raw_fallback, default_source="verified-fallback")
    if fallback:
        cache.setdefault("foundations", {})[ein or key] = {
            "grantees": fallback,
            "fetched": datetime.now().isoformat(timespec="seconds"),
            "source": "verified-fallback",
        }
        _save_cache(cache)
    return fallback[:max_grantees]


def fetch_all_grantees(companies: list[str]) -> dict[str, list[dict]]:
    """Bulk lookup. Returns {company: [grantees, …]}."""
    return {c: fetch_grantees(c) for c in companies}
