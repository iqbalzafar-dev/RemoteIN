#!/usr/bin/env python3
"""
RemoteIN bot: collects remote / WFH jobs where India (or APAC) is eligible.

Design notes
- Official public job-board APIs only: Greenhouse, Lever, Ashby, Remotive, Himalayas.
- Workday is optional and only runs for tenants you add to companies.json. The bot
  reads each tenant's robots.txt first and skips it if the API path is disallowed.
- Every source has its own schedule. Results are cached in state.json, so an hourly
  run only hits the sources that are due.
- Job descriptions are scanned in memory for remote / hybrid signals and never stored.
- Stored per job: title, company, location, apply link (plus source credit where required).
"""
import asyncio
import html
import json
import logging
import math
import os
import random
import re
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import aiohttp

# ==============================================================================
# 1. CONFIG
# ==============================================================================
REPO = os.environ.get("GITHUB_REPOSITORY", "")

CONFIG = {
    "COMPANIES_FILE": "companies.json",
    "JOBS_FILE": "jobs.json",
    "STATE_FILE": "state.json",
    "USER_AGENT": f"RemoteINBot/1.0 (+https://github.com/{REPO})" if REPO else "RemoteINBot/1.0",
    "MAX_CONCURRENT_REQUESTS": 8,
    "REQUEST_TIMEOUT_SECONDS": 25,
    "RUN_BUDGET_SECONDS": 12 * 60,       # stop starting new fetches after this
    "CUTOFF_DAYS": None,                 # None = no age filter (set a number later to enable)
    # False: single APAC countries (Singapore, Australia...) are NOT accepted unless India
    # is also listed. True: they are accepted and tagged APAC.
    "ALLOW_APAC_COUNTRIES": False,
    # Refresh intervals (hours)
    "BOARD_INTERVAL_HOURS": {"default": 3, "workday": 6},
    "REMOTIVE_INTERVAL_HOURS": 6,        # Remotive allows ~4 fetches/day
    "HIMALAYAS_INTERVAL_HOURS": 24,      # Himalayas refreshes its data every 24h
    "MIN_BATCH": 40,                     # min boards refreshed per run (besides never-fetched)
    "HIMALAYAS_MAX_PAGES": 25,
    "WORKDAY_MAX_PAGES": 10,
    "WORKDAY_MAX_DETAILS": 60,
    # Added in memory on every run (not written to companies.json)
    "ENSURE_COMPANIES": {"greenhouse": ["airbnb"]},
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("RemoteIN")

# ==============================================================================
# 2. TEXT, DATE HELPERS
# ==============================================================================
def now_utc():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def parse_dt(raw):
    if not raw:
        return None
    if isinstance(raw, (int, float)):
        try:
            ts = raw / 1000 if raw > 10_000_000_000 else raw
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            return None
    if isinstance(raw, str):
        val = raw.strip()
        if val.isdigit():
            return parse_dt(int(val))
        try:
            dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
        except Exception:
            pass
        for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(val, fmt)
                return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
            except Exception:
                continue
    return None


TAG_RE = re.compile(r"<[^>]+>")


def clean_text(raw, limit=25000):
    """HTML (even entity-escaped HTML) -> lowercase plain text, only used in memory."""
    if not raw:
        return ""
    t = html.unescape(str(raw))
    t = TAG_RE.sub(" ", t)
    t = html.unescape(t)
    return re.sub(r"\s+", " ", t).strip().lower()[:limit]


def term_re(terms):
    parts = [r"[\s\-]+".join(re.escape(w) for w in t.split()) for t in terms]
    return r"\b(?:" + "|".join(parts) + r")\b"


# ==============================================================================
# 3. ELIGIBILITY RULES
# ==============================================================================
INDIA_TERMS = [
    "india", "bangalore", "bengaluru", "gurgaon", "gurugram", "delhi", "new delhi", "ncr",
    "noida", "greater noida", "mumbai", "navi mumbai", "thane", "hyderabad", "secunderabad",
    "pune", "chennai", "kolkata", "ahmedabad", "kochi", "cochin", "chandigarh", "mohali",
    "indore", "jaipur", "coimbatore", "trivandrum", "thiruvananthapuram", "lucknow",
    "nagpur", "vadodara", "bhubaneswar",
]
APAC_REGION_TERMS = ["apac", "asia pacific", "asia-pacific", "asia", "south asia", "southeast asia",
                     "south-east asia", "asean", "apj"]
APAC_COUNTRY_TERMS = ["singapore", "australia", "new zealand", "philippines", "malaysia", "indonesia",
                      "vietnam", "thailand", "japan", "korea", "hong kong", "taiwan", "sri lanka",
                      "bangladesh", "nepal"]
WORLD_TERMS = ["worldwide", "anywhere", "global", "work from anywhere", "anywhere in the world",
               "any location", "international"]
OTHER_TERMS = [
    "us", "usa", "u.s.", "u.s.a.", "united states", "america", "americas", "north america",
    "uk", "u.k.", "united kingdom", "england", "scotland", "wales", "ireland", "canada",
    "mexico", "brazil", "argentina", "colombia", "chile", "peru", "latam", "latin america",
    "emea", "europe", "eu", "germany", "france", "spain", "italy", "portugal", "netherlands",
    "belgium", "switzerland", "austria", "poland", "czech", "czechia", "romania", "bulgaria",
    "hungary", "greece", "sweden", "norway", "denmark", "finland", "estonia", "latvia",
    "lithuania", "ukraine", "serbia", "croatia", "turkey", "israel", "uae", "dubai", "saudi",
    "egypt", "nigeria", "kenya", "south africa", "virgin islands", "puerto rico", "guam",
]

INDIA_RE = re.compile(term_re(INDIA_TERMS), re.I)
APAC_REGION_RE = re.compile(term_re(APAC_REGION_TERMS), re.I)
APAC_COUNTRY_RE = re.compile(term_re(APAC_COUNTRY_TERMS), re.I)
WORLD_RE = re.compile(term_re(WORLD_TERMS), re.I)
OTHER_RE = re.compile(term_re(OTHER_TERMS), re.I)

# Hybrid / office signals in location or title (always a reject)
HYBRID_LOC_RE = re.compile(
    term_re(["hybrid", "onsite", "on-site", "on site", "wfo", "in-office", "in office",
             "work from office", "office based", "office-based"]), re.I)

# Hybrid / office signals in descriptions (strong phrases only, to avoid false rejects)
HYBRID_DESC = [re.compile(p, re.I) for p in [
    r"\bhybrid (?:work|role|position|model|schedule|setup|set-up|environment|basis|working|opportunity)",
    r"\b(?:this|the) (?:role|position|job) is (?:a )?hybrid",
    r"\bhybrid\b[^.]{0,30}\b(?:office|onsite|on-site|days)\b",
    r"\bdays? (?:a|per) week (?:in|at) (?:the |our )?(?:office|on-?site)",
    r"\b(?:\d|one|two|three|four|five)\+? days?[^.]{0,20}\b(?:in|at) (?:the |our )?office\b",
    r"\b(?:in[- ]office|on-?site|office[- ]based) (?:role|position|job|presence|requirement|attendance|work)\b",
    r"\b(?:required|expected|must|need) to (?:work|be|come) (?:from|in|at|into) (?:the |our |a )?(?:office|on-?site)",
    r"\bwork from (?:the )?office\b",
    r"\bwfo\b",
    r"\boccasional(?:ly)?\b[^.]{0,30}\b(?:office|on-?site)\b",
]]

# Remote signals
REMOTE_LOC_RE = re.compile(
    r"\bremote\b|\bwork from home\b|\bwfh\b|\bwork from anywhere\b|\btelecommut\w*|"
    r"\bhome[- ]based\b|\bvirtual (?:role|position|job|assistant)\b", re.I)
REMOTE_DESC_RE = re.compile(
    r"\b(?:fully|100%|completely|entirely|totally)[ -]remote\b|"
    r"\bremote[- ](?:first|role|position|job|work|opportunity|based)\b|"
    r"\bwork(?:ing)? (?:remotely|from home)\b|\bwork from home\b|\bwfh\b|"
    r"\b(?:this|the) (?:role|position|job) is (?:fully |100% )?remote\b|"
    r"\bwork from anywhere\b|\bremote within\b|"
    r"\bremote\b[^.]{0,20}\bindia\b|\bindia\b[^.]{0,20}\bremote\b", re.I)
TRAINING_RE = re.compile(
    r"\bremote after training\b|"
    r"\b(?:remote|work from home|wfh|work remotely)\b[^.]{0,50}\b(?:after|post|once|following|upon)\b[^.]{0,30}\btraining\b|"
    r"\b(?:after|post|once)\b[^.]{0,30}\btraining\b[^.]{0,60}\b(?:remote|work from home|wfh|work remotely)\b|"
    r"\btraining\b[^.]{0,80}\b(?:then|followed by|after which|thereafter|post which|following which)\b[^.]{0,40}\b(?:remote|work from home|wfh)\b",
    re.I)

# Eligibility hints for jobs whose location is only "Remote"
_ELIG_VERBS = (r"(?:open to|hiring|hire|based in|located in|residing in|reside in|candidates|applicants|"
               r"eligible|work(?:ing)? from|remote (?:in|within|from|across|-)|available in|apply from|location)")
ELIG_INDIA_RE = re.compile(_ELIG_VERBS + r"\b[^.\n]{0,70}" + term_re(INDIA_TERMS) +
                           r"|\bindia\b[^.\n]{0,30}\b(?:eligible|welcome|based candidates)\b", re.I)
ELIG_APAC_RE = re.compile(_ELIG_VERBS + r"\b[^.\n]{0,70}\b(?:apac|asia[- ]pacific)\b", re.I)
ELIG_WORLD_RE = re.compile(
    r"\bwork from anywhere\b|\banywhere in the world\b|"
    r"\b(?:hire|hiring|candidates?|applicants?|team) (?:from )?(?:anywhere|worldwide|globally)\b|"
    r"\bglobal(?:ly)? remote\b", re.I)

TECH_TITLE_RE = re.compile(
    r"\b(?:engineers?|engineering|developers?|software|architects?|devops|sre|qa|sdet|ml|ai|"
    r"machine learning|data scientists?|data engineers?|data analysts?|data analytics|analytics engineers?|"
    r"backend|back-end|frontend|front-end|full[- ]?stack|cloud|security|cybersecurity|infosec|"
    r"mobile|ios|android|product managers?|designers?|ux|ui)\b", re.I)
SUPPORT_TITLE_RE = re.compile(r"\bsupport\b", re.I)
HARD_TECH_RE = re.compile(r"\b(?:software|backend|frontend|platform|devops|sre|machine learning)\b", re.I)


def classify_role(title):
    t = title or ""
    if SUPPORT_TITLE_RE.search(t) and not HARD_TECH_RE.search(t):
        return "Non-Tech"          # all Support roles (incl. Technical Support) are Non-Tech
    return "Tech" if TECH_TITLE_RE.search(t) else "Non-Tech"


def region_verdict(loc):
    """Judge ONLY the location text (never the job title)."""
    if not loc or not loc.strip():
        return "empty"
    if INDIA_RE.search(loc):
        return "india"
    if APAC_REGION_RE.search(loc):
        return "apac"
    if CONFIG["ALLOW_APAC_COUNTRIES"] and APAC_COUNTRY_RE.search(loc):
        return "apac"
    if OTHER_RE.search(loc):
        return "other"
    if WORLD_RE.search(loc):
        return "worldwide"
    return "unspecified"


def evaluate(title, loc, get_desc, remote_flag=False, nonremote_flag=False):
    """
    Returns (ok, reason, remote_type, display_location).
    ok only when the job is remote/WFH (strict, no hybrid/office) AND India/APAC/worldwide eligible.
    """
    loc = (loc or "").strip()
    region = region_verdict(loc)
    if region == "other":
        return False, "geo_other_country", None, loc
    if nonremote_flag or HYBRID_LOC_RE.search(f"{loc} {title}"):
        return False, "hybrid_or_office", None, loc

    desc = get_desc() or ""
    if desc and any(p.search(desc) for p in HYBRID_DESC):
        return False, "hybrid_or_office", None, loc

    display = loc
    if region in ("unspecified", "empty"):
        if ELIG_INDIA_RE.search(desc):
            display = "Remote - India"
        elif ELIG_APAC_RE.search(desc):
            display = "Remote - APAC"
        elif ELIG_WORLD_RE.search(desc):
            display = "Remote - Worldwide"
        else:
            return False, "region_not_confirmed", None, loc

    if remote_flag or REMOTE_LOC_RE.search(f"{loc} {title}"):
        rtype = "confirmed"
    elif desc and TRAINING_RE.search(desc):
        rtype = "after-training"
    elif desc and REMOTE_DESC_RE.search(desc):
        rtype = "confirmed"
    else:
        return False, "no_remote_signal", None, loc
    return True, "ok", rtype, display


# ==============================================================================
# 4. HTTP LAYER (polite: honest UA, concurrency cap, jitter, cooldown friendly)
# ==============================================================================
class Ctx:
    def __init__(self, session, deadline):
        self.session = session
        self.sem = asyncio.Semaphore(CONFIG["MAX_CONCURRENT_REQUESTS"])
        self.timeout = aiohttp.ClientTimeout(total=CONFIG["REQUEST_TIMEOUT_SECONDS"])
        self.stats = Counter()
        self.rej = Counter()
        self.deadline = deadline
        self.wd_map = {}
        self.robots_cache = {}

    async def json(self, method, url, tag, **kw):
        """Returns (status, data). status 0 = network/parse failure."""
        for attempt in range(2):
            try:
                async with self.sem:
                    await asyncio.sleep(random.uniform(0.05, 0.3))
                    async with self.session.request(method, url, timeout=self.timeout, **kw) as r:
                        if r.status == 200:
                            return 200, await r.json(content_type=None)
                        if r.status >= 500 and attempt == 0:
                            self.stats[f"{tag}:http_{r.status}_retry"] += 1
                            await asyncio.sleep(2 + random.random() * 2)
                            continue
                        self.stats[f"{tag}:http_{r.status}"] += 1
                        return r.status, None
            except asyncio.TimeoutError:
                self.stats[f"{tag}:timeout"] += 1
                if attempt == 0:
                    await asyncio.sleep(2)
                    continue
                return 0, None
            except (aiohttp.ClientError, ValueError) as e:
                self.stats[f"{tag}:error"] += 1
                log.debug("%s %s failed: %s", tag, url, e)
                return 0, None
        return 0, None

    async def robots_allowed(self, url):
        p = urlparse(url)
        origin = f"{p.scheme}://{p.netloc}"
        if origin not in self.robots_cache:
            rp = RobotFileParser()
            try:
                async with self.sem:
                    async with self.session.get(origin + "/robots.txt", timeout=self.timeout) as r:
                        if r.status == 200:
                            rp.parse((await r.text()).splitlines())
                            self.robots_cache[origin] = rp
                        elif 400 <= r.status < 500:
                            self.robots_cache[origin] = None      # no robots rules -> allowed
                        else:
                            self.robots_cache[origin] = False     # server error -> be conservative
            except Exception:
                self.robots_cache[origin] = False
        rp = self.robots_cache[origin]
        if rp is None:
            return True
        if rp is False:
            return False
        return rp.can_fetch("RemoteINBot", url)


def pretty_company(slug):
    return str(slug).replace("-", " ").replace("_", " ").title()


def uniq_join(parts):
    seen, out = set(), []
    for p in parts:
        p = (p or "").strip()
        if p and p.lower() not in seen:
            seen.add(p.lower())
            out.append(p)
    return "; ".join(out)


def make_job(job_id, title, company, location, url, posted, rtype, source=None, source_url=None):
    job = {
        "id": str(job_id),
        "title": (title or "").strip(),
        "company": company,
        "location": (location or "").strip() or "Remote",
        "category": classify_role(title),
        "remoteType": rtype,
        "url": url,
        "posted": iso(posted) if posted else None,
    }
    if source:
        job["source"] = source
        job["sourceUrl"] = source_url or url
    return job


# ==============================================================================
# 5. SCRAPERS (each returns (status, jobs or None))
# ==============================================================================
async def scrape_greenhouse(ctx, slug):
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
    status, data = await ctx.json("GET", url, "greenhouse")
    if status != 200 or not isinstance(data, dict):
        return status, None
    jobs = []
    for item in data.get("jobs", []):
        title = item.get("title") or ""
        loc = uniq_join([(item.get("location") or {}).get("name", "")] +
                        [(o or {}).get("name", "") for o in (item.get("offices") or [])])
        ok, reason, rtype, dloc = evaluate(title, loc, lambda i=item: clean_text(i.get("content")))
        ctx.rej[f"greenhouse:{reason}"] += 1
        if ok and item.get("absolute_url"):
            jobs.append(make_job(f"gh-{slug}-{item.get('id')}", title, pretty_company(slug), dloc,
                                 item["absolute_url"], parse_dt(item.get("first_published")), rtype))
    return 200, jobs


async def scrape_lever(ctx, slug):
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    status, data = await ctx.json("GET", url, "lever")
    if status != 200 or not isinstance(data, list):
        return status, None
    jobs = []
    for item in data:
        title = item.get("text") or ""
        cats = item.get("categories") or {}
        locs = [cats.get("location") or ""] + list(cats.get("allLocations") or [])
        if str(item.get("country") or "").upper() == "IN":
            locs.append("India")
        loc = uniq_join(locs)
        wp = str(item.get("workplaceType") or "").lower()

        def desc(i=item):
            lists = " ".join(str(x.get("content", "")) for x in (i.get("lists") or []) if isinstance(x, dict))
            return clean_text(f"{i.get('descriptionPlain') or i.get('description') or ''} "
                              f"{i.get('additionalPlain') or ''} {lists}")

        ok, reason, rtype, dloc = evaluate(title, loc, desc, remote_flag=(wp == "remote"),
                                           nonremote_flag=(wp in ("hybrid", "on-site", "onsite")))
        ctx.rej[f"lever:{reason}"] += 1
        if ok and item.get("hostedUrl"):
            jobs.append(make_job(f"lv-{slug}-{item.get('id')}", title, pretty_company(slug), dloc,
                                 item["hostedUrl"], parse_dt(item.get("createdAt")), rtype))
    return 200, jobs


async def scrape_ashby(ctx, slug):
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    status, data = await ctx.json("GET", url, "ashby")
    if status != 200 or not isinstance(data, dict):
        return status, None
    jobs = []
    for item in data.get("jobs", []):
        if item.get("isListed") is False:
            continue
        title = item.get("title") or ""
        locs = [item.get("location") or ""]
        locs += [(s or {}).get("location", "") for s in (item.get("secondaryLocations") or []) if isinstance(s, dict)]
        country = (((item.get("address") or {}).get("postalAddress")) or {}).get("addressCountry") or ""
        if country.strip().lower() in ("india", "in"):
            locs.append("India")
        loc = uniq_join(locs)
        wp = str(item.get("workplaceType") or "").lower()
        ok, reason, rtype, dloc = evaluate(
            title, loc, lambda i=item: clean_text(i.get("descriptionPlain") or i.get("descriptionHtml")),
            remote_flag=bool(item.get("isRemote")) or wp == "remote",
            nonremote_flag=(wp in ("hybrid", "onsite", "on-site")))
        ctx.rej[f"ashby:{reason}"] += 1
        link = item.get("jobUrl") or item.get("applyUrl")
        if ok and link:
            jobs.append(make_job(f"ab-{slug}-{item.get('id') or link}", title, pretty_company(slug), dloc,
                                 link, parse_dt(item.get("publishedAt")), rtype))
    return 200, jobs


async def 