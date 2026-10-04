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


async def scrape_workday(ctx, key):
    cfg = ctx.wd_map[key]
    tenant, dc, site = cfg["tenant"], cfg["dc"], cfg["site"]
    base = f"https://{tenant}.{dc}.myworkdayjobs.com"
    api = f"{base}/wday/cxs/{tenant}/{site}"
    if not await ctx.robots_allowed(f"{api}/jobs"):
        ctx.stats["workday:robots_disallowed"] += 1
        log.warning("Workday %s skipped: robots.txt does not allow this API path", key)
        return 403, None

    postings, offset, total = [], 0, None
    for _ in range(CONFIG["WORKDAY_MAX_PAGES"]):
        status, data = await ctx.json("POST", f"{api}/jobs", "workday",
                                      json={"appliedFacets": {}, "limit": 20, "offset": offset,
                                            "searchText": "remote"})
        if status != 200 or not isinstance(data, dict):
            if not postings:
                return status, None
            break
        page = data.get("jobPostings") or []
        if not page:
            break
        if total is None:
            total = data.get("total") or 0
        postings += page
        offset += 20
        if total and offset >= total:
            break

    cands = [p for p in postings
             if region_verdict(p.get("locationsText") or "") != "other" and p.get("externalPath")]
    jobs = []
    for p in cands[:CONFIG["WORKDAY_MAX_DETAILS"]]:
        status, d = await ctx.json("GET", f"{api}{p['externalPath']}", "workday")
        info = (d or {}).get("jobPostingInfo") if isinstance(d, dict) else None
        if status != 200 or not info:
            continue
        title = info.get("title") or p.get("title") or ""
        loc = uniq_join([info.get("location") or p.get("locationsText") or ""] +
                        list(info.get("additionalLocations") or []) +
                        [((info.get("country") or {}).get("descriptor")) or ""])
        rt = str(info.get("remoteType") or "").lower()
        ok, reason, rtype, dloc = evaluate(
            title, loc, lambda i=info: clean_text(i.get("jobDescription")),
            remote_flag=("remote" in rt and "hybrid" not in rt),
            nonremote_flag=any(x in rt for x in ("hybrid", "onsite", "on-site", "on site")))
        ctx.rej[f"workday:{reason}"] += 1
        if ok:
            link = info.get("externalUrl") or f"{base}/{site}{p['externalPath']}"
            jobs.append(make_job(f"wd-{key}-{p['externalPath']}", title,
                                 cfg.get("name") or pretty_company(tenant), dloc, link,
                                 parse_dt(info.get("startDate")), rtype))
    return 200, jobs


async def scrape_remotive(ctx, _ident=None):
    status, data = await ctx.json("GET", "https://remotive.com/api/remote-jobs", "remotive")
    if status != 200 or not isinstance(data, dict):
        return status, None
    jobs = []
    for j in data.get("jobs", []):
        title = j.get("title") or ""
        loc = (j.get("candidate_required_location") or "").strip() or "Worldwide"
        ok, reason, rtype, dloc = evaluate(title, loc, lambda x=j: clean_text(x.get("description")),
                                           remote_flag=True)
        ctx.rej[f"remotive:{reason}"] += 1
        if ok and j.get("url"):
            jobs.append(make_job(f"rm-{j.get('id')}", title, j.get("company_name") or "Remotive", dloc,
                                 j["url"], parse_dt(j.get("publication_date")), rtype,
                                 source="Remotive", source_url=j["url"]))
    return 200, jobs


async def scrape_himalayas(ctx, _ident=None):
    base = "https://himalayas.app/jobs/api/search"
    jobs, complete = [], False
    for page in range(1, CONFIG["HIMALAYAS_MAX_PAGES"] + 1):
        status, data = await ctx.json("GET", base, "himalayas",
                                      params={"country": "IN", "sort": "recent", "page": page})
        if status != 200 or not isinstance(data, dict):
            if page == 1:
                return status, None
            return status, None          # incomplete walk: keep the previous cache
        batch = data.get("jobs") or []
        if not batch:
            complete = True
            break
        for j in batch:
            title = j.get("title") or ""
            restr = [r for r in (j.get("locationRestrictions") or []) if isinstance(r, dict)]
            if not restr:
                loc = "Worldwide"
            elif any(str(r.get("alpha2", "")).upper() == "IN" for r in restr):
                loc = "India"
            else:
                loc = ", ".join(str(r.get("name", "")) for r in restr)
            ok, reason, rtype, dloc = evaluate(title, loc, lambda x=j: clean_text(x.get("description")),
                                               remote_flag=True)
            ctx.rej[f"himalayas:{reason}"] += 1
            link = j.get("applicationLink")
            if ok and link:
                jobs.append(make_job(f"hm-{j.get('guid') or link}", title, j.get("companyName") or "Himalayas",
                                     dloc, link, parse_dt(j.get("pubDate")), rtype,
                                     source="Himalayas", source_url=link))
        await asyncio.sleep(1.5)
    if not complete:
        log.info("Himalayas: reached page cap (%d pages)", CONFIG["HIMALAYAS_MAX_PAGES"])
    return 200, jobs


BOARD_SCRAPERS = {"greenhouse": scrape_greenhouse, "lever": scrape_lever,
                  "ashby": scrape_ashby, "workday": scrape_workday}

# ==============================================================================
# 6. STATE, SCHEDULING, REGISTRY
# ==============================================================================
def load_state():
    st = {"boards": {}, "sources": {}, "first_seen": {}}
    try:
        with open(CONFIG["STATE_FILE"], "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            for k in st:
                if isinstance(data.get(k), dict):
                    st[k] = data[k]
    except FileNotFoundError:
        pass
    except Exception as e:
        log.error("Could not read %s (starting fresh): %s", CONFIG["STATE_FILE"], e)
    return st


def write_json(path, data, indent=None):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)
    os.replace(tmp, path)


def load_companies():
    raw = {}
    try:
        with open(CONFIG["COMPANIES_FILE"], "r", encoding="utf-8") as f:
            content = json.load(f)
        if isinstance(content, dict):
            raw = content
    except FileNotFoundError:
        log.warning("%s not found, using only built-in companies", CONFIG["COMPANIES_FILE"])
    except Exception as e:
        log.error("Error loading %s: %s", CONFIG["COMPANIES_FILE"], e)

    out = {}
    for ats in ("greenhouse", "lever", "ashby"):
        slugs = list(raw.get(ats, [])) + list(CONFIG["ENSURE_COMPANIES"].get(ats, []))
        out[ats] = sorted({str(s).strip().lower() for s in slugs if str(s).strip()})
    wd = []
    for c in raw.get("workday", []) or []:
        if isinstance(c, dict) and all(c.get(k) for k in ("tenant", "dc", "site")):
            wd.append(c)
    out["workday"] = wd
    return out


def is_due(entry, hours, now, force):
    entry = entry or {}
    cu = parse_dt(entry.get("cooldown_until"))
    if cu and cu > now:
        return False
    if force:
        return True
    lf = parse_dt(entry.get("last_fetch"))
    return lf is None or now - lf >= timedelta(hours=hours)


def apply_result(entry, status, jobs):
    now = now_utc()
    if status == 200 and jobs is not None:
        entry["jobs"] = jobs
        entry["last_fetch"] = iso(now)
        entry.pop("cooldown_until", None)
    elif status == 404:
        entry["jobs"] = []
        entry["last_fetch"] = iso(now)
        entry["cooldown_until"] = iso(now + timedelta(days=7))
    elif status in (401, 403, 429):
        entry["cooldown_until"] = iso(now + timedelta(hours=24))
    else:
        entry["cooldown_until"] = iso(now + timedelta(hours=1))


def pick_boards(state, boards, now, force):
    never, stale = [], []
    for ats, ident in boards:
        key = f"{ats}:{ident}"
        entry = state["boards"].get(key, {})
        hours = CONFIG["BOARD_INTERVAL_HOURS"].get(ats, CONFIG["BOARD_INTERVAL_HOURS"]["default"])
        if not is_due(entry, hours, now, force):
            continue
        lf = parse_dt(entry.get("last_fetch"))
        (never if lf is None else stale).append((lf or now, ats, ident))
    stale.sort()
    default_h = CONFIG["BOARD_INTERVAL_HOURS"]["default"]
    cap = len(boards) if force else max(CONFIG["MIN_BATCH"], math.ceil(len(boards) / default_h))
    return [(a, i) for _, a, i in never] + [(a, i) for _, a, i in stale[:cap]]


async def run_board(ctx, state, ats, ident):
    if time.monotonic() > ctx.deadline:
        ctx.stats["skipped_time_budget"] += 1
        return
    key = f"{ats}:{ident}"
    entry = state["boards"].setdefault(key, {})
    try:
        status, jobs = await BOARD_SCRAPERS[ats](ctx, ident)
    except Exception as e:
        ctx.stats[f"{ats}:crash"] += 1
        log.error("%s crashed: %s", key, e)
        status, jobs = 0, None
    apply_result(entry, status, jobs)
    if status == 404:
        log.warning("%s: board not found (will retry in 7 days)", key)
    elif status in (401, 403, 429):
        log.warning("%s: blocked/limited (HTTP %s), cooling down 24h", key, status)


async def run_source(ctx, state, name, fn):
    entry = state["sources"].setdefault(name, {})
    try:
        status, jobs = await fn(ctx)
    except Exception as e:
        ctx.stats[f"{name}:crash"] += 1
        log.error("%s crashed: %s", name, e)
        status, jobs = 0, None
    apply_result(entry, status, jobs)
    if jobs is not None:
        log.info("%s: %d eligible jobs", name, len(jobs))
    else:
        log.warning("%s: fetch failed (HTTP %s), keeping cached jobs", name, status)


# ==============================================================================
# 7. ASSEMBLY
# ==============================================================================
def assemble(state, boards):
    now = now_utc()
    cutoff = now - timedelta(days=CONFIG["CUTOFF_DAYS"]) if CONFIG["CUTOFF_DAYS"] else None
    pool = []
    for name in ("himalayas", "remotive"):
        pool += (state["sources"].get(name) or {}).get("jobs", [])
    for ats, ident in boards:
        pool += (state["boards"].get(f"{ats}:{ident}") or {}).get("jobs", [])

    seen_url, seen_key, out, first_seen = set(), set(), [], {}
    for j in pool:
        url = j.get("url")
        if not url or url in seen_url:
            continue
        key = (j["company"].lower(), j["title"].lower(), j["location"].lower())
        if key in seen_key:
            continue
        seen_url.add(url)
        seen_key.add(key)
        fs = state["first_seen"].get(url) or iso(now)
        first_seen[url] = fs
        posted = parse_dt(j.get("posted"))
        if posted and posted > now + timedelta(days=1):
            posted = now
        date_dt = posted or parse_dt(fs) or now
        if cutoff and date_dt < cutoff:
            continue
        rec = {k: v for k, v in j.items() if k != "posted"}
        rec["date"] = rec["dateAdded"] = iso(date_dt)
        out.append(rec)
    out.sort(key=lambda r: r["date"], reverse=True)
    state["first_seen"] = first_seen
    return out


def log_summary(ctx, jobs, started):
    by_ats = {}
    for k, v in ctx.rej.items():
        ats, reason = k.split(":", 1)
        by_ats.setdefault(ats, Counter())[reason] += v
    for ats, c in sorted(by_ats.items()):
        total = sum(c.values())
        parts = ", ".join(f"{r}={n}" for r, n in c.most_common())
        log.info("[%s] checked %d jobs -> %s", ats, total, parts)
    errs = {k: v for k, v in ctx.stats.items()}
    if errs:
        log.info("Request issues: %s", ", ".join(f"{k}={v}" for k, v in sorted(errs.items())))
    log.info("Done in %.1fs. Total jobs in %s: %d", time.monotonic() - started, CONFIG["JOBS_FILE"], len(jobs))


# ==============================================================================
# 8. MAIN
# ==============================================================================
async def main():
    started = time.monotonic()
    force = os.environ.get("FORCE_ALL", "").strip().lower() in ("1", "true", "yes")
    now = now_utc()
    cfg = load_companies()
    state = load_state()

    boards = [(a, s) for a in ("greenhouse", "lever", "ashby") for s in cfg[a]]
    wd_map = {}
    for c in cfg["workday"]:
        key = f"{c['tenant']}.{c['dc']}.{c['site']}"
        wd_map[key] = c
        boards.append(("workday", key))

    headers = {"User-Agent": CONFIG["USER_AGENT"], "Accept": "application/json"}
    async with aiohttp.ClientSession(headers=headers) as session:
        ctx = Ctx(session, started + CONFIG["RUN_BUDGET_SECONDS"])
        ctx.wd_map = wd_map

        tasks = []
        if is_due(state["sources"].get("himalayas"), CONFIG["HIMALAYAS_INTERVAL_HOURS"], now, force):
            tasks.append(run_source(ctx, state, "himalayas", scrape_himalayas))
        if is_due(state["sources"].get("remotive"), CONFIG["REMOTIVE_INTERVAL_HOURS"], now, force):
            tasks.append(run_source(ctx, state, "remotive", scrape_remotive))

        batch = pick_boards(state, boards, now, force)
        log.info("Boards: %d registered, %d due this run%s", len(boards), len(batch),
                 " (forced)" if force else "")
        tasks += [run_board(ctx, state, a, i) for a, i in batch]
        await asyncio.gather(*tasks)

    live = {f"{a}:{i}" for a, i in boards}
    state["boards"] = {k: v for k, v in state["boards"].items() if k in live}

    jobs = assemble(state, boards)
    write_json(CONFIG["JOBS_FILE"], jobs, indent=1)
    write_json(CONFIG["STATE_FILE"], state)
    log_summary(ctx, jobs, started)


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
