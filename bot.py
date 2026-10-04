import asyncio
import aiohttp
import json
import logging
import os
import random
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta
from duckduckgo_search import DDGS

# ==============================================================================
# 1. CONFIGURATION & COMPREHENSIVE FILTER RULES
# ==============================================================================
CONFIG = {
    "COMPANIES_FILE": "companies.json",
    "JOBS_FILE": "jobs.json",
    "MAX_CONCURRENT_REQUESTS": 25,
    "REQUEST_TIMEOUT_SECONDS": 15,
    "CUTOFF_DAYS": 30,
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    
    # Auto-discovery queries across target ATS platforms
    "DISCOVERY_QUERIES": [
        'site:boards.greenhouse.io "India" "Remote"',
        'site:boards.greenhouse.io "APAC" "Remote"',
        'site:jobs.lever.co "India" "Remote"',
        'site:jobs.lever.co "APAC" "Remote"',
        'site:jobs.ashbyhq.com "India" "Remote"',
        'site:jobs.ashbyhq.com "APAC" "Remote"'
    ],

    # Smart Category Mapping
    "ROLE_KEYWORDS": {
        "Payroll & Compliance": ["payroll", "compensation", "benefits", "comp & ben"],
        "HR & People Ops": ["hr", "recruiter", "talent acquisition", "people ops", "human resources", "talent", "onboarding"],
        "Finance & Accounting": ["finance", "accountant", "accounting", "billing", "audit", "tax", "fp&a", "treasury"],
        "Operations & Support": ["operations", "vendor ops", "customer support", "customer success", "ops associate", "support specialist"],
        "Tech & Engineering": ["engineer", "developer", "data", "software", "product", "designer", "architect", "devops", "qa", "ml", "ai"]
    },

    # Strict WFO / In-Office Red Flags
    "RED_FLAGS": [
        "hybrid", "onsite", "on-site", "wfo", "in-office", 
        "work from office", "office based"
    ],

    # Non-India / Non-APAC regional locks
    "GEO_LOCKS": [
        "us only", "uk only", "emea only", "europe only", 
        "canada only", "latam only", "remote - us", "remote (us)",
        "remote - uk", "remote - europe", "remote - north america", "germany only"
    ],

    # Mandatory Remote Signals
    "REMOTE_SIGNALS": [
        "remote", "work from home", "wfh", "anywhere", 
        "distributed", "telecommute", "virtual role", "work from anywhere"
    ],

    # Tier-1 & Indian Tech Hubs + APAC + Global
    "ELIGIBLE_LOCATIONS": [
        "india", "bangalore", "bengaluru", "gurgaon", "gurugram", 
        "delhi", "new delhi", "ncr", "noida", "greater noida", 
        "mumbai", "navi mumbai", "thane", "hyderabad", "secunderabad", 
        "pune", "chennai", "kolkata", "ahmedabad", "kochi", "cochin", 
        "chandigarh", "mohali", "indore", "jaipur", "apac", "asia", 
        "singapore", "australia", "philippines", "malaysia", "indonesia", 
        "worldwide", "global", "anywhere"
    ]
}

BROWSER_HEADERS = {
    "User-Agent": CONFIG["USER_AGENT"],
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9"
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("RemoteIN")
CUTOFF_TIMESTAMP = datetime.now(timezone.utc) - timedelta(days=CONFIG["CUTOFF_DAYS"])

# ==============================================================================
# 2. DATE & TEXT HELPERS
# ==============================================================================
def parse_iso_datetime(raw_val):
    if not raw_val:
        return None
    if isinstance(raw_val, (int, float)):
        try:
            ts = raw_val / 1000 if raw_val > 10000000000 else raw_val
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            return None
    if isinstance(raw_val, str):
        val = raw_val.strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(val)
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
        except Exception:
            pass
        for fmt in (
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%d"
        ):
            try:
                dt = datetime.strptime(raw_val, fmt)
                return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
            except Exception:
                continue
    return None

def is_recent(dt_obj):
    return dt_obj and dt_obj >= CUTOFF_TIMESTAMP

def classify_role(title):
    t = title.lower()
    for cat, keywords in CONFIG["ROLE_KEYWORDS"].items():
        if any(kw in t for kw in keywords):
            return cat
    return "Operations & Support"

def is_eligible_remote(title, location, extra=""):
    blob = f"{title} {location} {extra}".lower()
    
    # 1. Reject WFO / Hybrid
    if any(rf in blob for rf in CONFIG["RED_FLAGS"]):
        return False
        
    # 2. Reject Geo-Locks (unless India is explicitly mentioned)
    if any(gl in blob for gl in CONFIG["GEO_LOCKS"]) and "india" not in blob:
        return False
        
    # 3. Mandatory Remote Signal
    has_remote = any(rs in blob for rs in CONFIG["REMOTE_SIGNALS"])
    if not has_remote:
        return False

    # 4. Location Eligibility Check
    if not location or any(place in blob for place in CONFIG["ELIGIBLE_LOCATIONS"]):
        return True

    return False

# ==============================================================================
# 3. COMPANY REGISTRY (COMPATIBLE WITH EXISTING 300+ COMPANIES)
# ==============================================================================
class CompanyRegistry:
    def __init__(self, filepath):
        self.filepath = filepath
        self.data = self._load()
        self.slug_sets = {
            "greenhouse": set(self.data.get("greenhouse", [])),
            "lever": set(self.data.get("lever", [])),
            "ashby": set(self.data.get("ashby", []))
        }

    def _load(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, 'r') as f:
                    content = json.load(f)
                    if isinstance(content, dict):
                        return content
            except Exception as e:
                log.error(f"Error loading {self.filepath}: {e}")
        return {"greenhouse": [], "lever": [], "ashby": []}

    def save(self):
        output = {
            "greenhouse": sorted(list(self.slug_sets["greenhouse"])),
            "lever": sorted(list(self.slug_sets["lever"])),
            "ashby": sorted(list(self.slug_sets["ashby"]))
        }
        with open(self.filepath, 'w') as f:
            json.dump(output, f, indent=4)

    def extract_ats_from_url(self, url):
        if not url:
            return None, None
        parsed = urlparse(url)
        parts = [p for p in parsed.path.strip("/").split("/") if p]
        if "greenhouse.io" in parsed.netloc and parts:
            return "greenhouse", parts[0]
        if "lever.co" in parsed.netloc and parts:
            return "lever", parts[0]
        if "ashbyhq.com" in parsed.netloc and parts:
            return "ashby", parts[0]
        return None, None

    async def add_discovered_company(self, session, ats_type, slug):
        slug = slug.strip().lower()
        if ats_type not in self.slug_sets or slug in self.slug_sets[ats_type]:
            return False

        test_url = ""
        if ats_type == "greenhouse":
            test_url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
        elif ats_type == "lever":
            test_url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
        elif ats_type == "ashby":
            test_url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"

        try:
            async with session.get(test_url, timeout=8) as r:
                if r.status != 200:
                    return False
        except Exception:
            return False

        self.slug_sets[ats_type].add(slug)
        log.info(f"✨ Discovered and validated new company: {slug} ({ats_type.upper()})")
        return True

# ==============================================================================
# 4. DISCOVERY VIA DUCKDUCKGO
# ==============================================================================
async def run_search_discovery(session, registry):
    log.info("🔎 Searching for new companies via ATS indexing...")
    ddgs = DDGS()
    discovered_urls = []
    for query in CONFIG["DISCOVERY_QUERIES"]:
        try:
            await asyncio.sleep(random.uniform(1.0, 1.5))
            for res in ddgs.text(query, max_results=12):
                discovered_urls.append(res.get("href"))
        except Exception:
            pass

    for url in discovered_urls:
        ats, slug = registry.extract_ats_from_url(url)
        if ats and slug:
            await registry.add_discovered_company(session, ats, slug)

# ==============================================================================
# 5. SCRAPING ENGINE (PUBLIC ATS APIS + MASSIVE FEEDS)
# ==============================================================================
jobs_output = []
seen_urls = set()

def record_job(job_id, title, company, location, category, url, dt_obj):
    if not is_recent(dt_obj) or url in seen_urls:
        return
    seen_urls.add(url)
    
    iso_date = dt_obj.isoformat()
    jobs_output.append({
        "id": str(job_id),
        "title": title,
        "company": company.replace("-", " ").title(),
        "location": location if location else "Remote (India/APAC)",
        "category": category,
        "url": url,
        "date": iso_date,
        "dateAdded": iso_date
    })

async def scrape_greenhouse(session, slug, sem):
    async with sem:
        url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
        try:
            async with session.get(url, timeout=CONFIG["REQUEST_TIMEOUT_SECONDS"]) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for item in data.get("jobs", []):
                        title = item.get("title", "")
                        loc = (item.get("location") or {}).get("name", "")
                        dt = parse_iso_datetime(item.get("updated_at"))
                        if is_eligible_remote(title, loc):
                            record_job(item["id"], title, slug, loc, classify_role(title), item["absolute_url"], dt)
        except Exception:
            pass

async def scrape_lever(session, slug, sem):
    async with sem:
        url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
        try:
            async with session.get(url, timeout=CONFIG["REQUEST_TIMEOUT_SECONDS"]) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for item in data:
                        title = item.get("text", "")
                        cats = item.get("categories", {})
                        loc = cats.get("location", "")
                        commit = cats.get("commitment", "")
                        dt = parse_iso_datetime(item.get("createdAt"))
                        if is_eligible_remote(title, loc, commit):
                            record_job(item["id"], title, slug, loc, classify_role(title), item.get("hostedUrl", ""), dt)
        except Exception:
            pass

async def scrape_ashby(session, slug, sem):
    async with sem:
        url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
        try:
            async with session.get(url, timeout=CONFIG["REQUEST_TIMEOUT_SECONDS"]) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for item in data.get("jobs", []):
                        title = item.get("title", "")
                        loc = item.get("location", "")
                        dt = parse_iso_datetime(item.get("publishedAt"))
                        is_remote = item.get("isRemote", False)
                        check_meta = f"{loc} {'remote' if is_remote else ''}"
                        if is_eligible_remote(title, loc, check_meta):
                            record_job(item["id"], title, slug, loc, classify_role(title), item.get("jobUrl", ""), dt)
        except Exception:
            pass

async def scrape_aggregators_and_discover(session, registry):
    log.info("🌐 Fetching bulk verified remote feeds...")

    # 1. Remotive (Cloudflare Safe + Worldwide/India Open Filter)
    try:
        async with session.get("https://remotive.com/api/remote-jobs", headers=BROWSER_HEADERS, timeout=20) as resp:
            if resp.status == 200:
                data = await resp.json()
                remotive_jobs = data.get("jobs", [])
                log.info(f"Remotive responded with {len(remotive_jobs)} raw jobs.")
                for j in remotive_jobs:
                    title = j.get("title", "")
                    loc = (j.get("candidate_required_location") or "Worldwide").strip()
                    loc_lower = loc.lower()
                    url = j.get("url", "")
                    comp_name = j.get("company_name", "Global Remote")
                    dt = parse_iso_datetime(j.get("publication_date"))

                    # Discovery check
                    ats, slug = registry.extract_ats_from_url(url)
                    if ats and slug:
                        await registry.add_discovered_company(session, ats, slug)

                    # Remotive is 100% remote: verify it is open to India/APAC/Worldwide
                    if not any(gl in loc_lower for gl in CONFIG["GEO_LOCKS"]):
                        if any(k in loc_lower for k in ["worldwide", "anywhere", "india", "apac", "asia", "global"]) or loc_lower == "":
                            cat = classify_role(title)
                            record_job(j.get("id", url), title, comp_name, loc if loc else "Worldwide (Remote)", cat, url, dt)
    except Exception as e:
        log.warning(f"Remotive feed error: {e}")

    # 2. Jobicy
    try:
        async with session.get("https://jobicy.com/api/v2/remote-jobs?count=100", headers=BROWSER_HEADERS, timeout=15) as resp:
            if resp.status == 200:
                data = await resp.json()
                for j in data.get("jobs", []):
                    title = j.get("jobTitle", "")
                    loc = (j.get("jobGeo") or "Anywhere").strip()
                    loc_lower = loc.lower()
                    url = j.get("url", "")
                    comp = j.get("companyName", "Direct Company")
                    dt = parse_iso_datetime(j.get("pubDate"))
                    
                    ats, slug = registry.extract_ats_from_url(url)
                    if ats and slug:
                        await registry.add_discovered_company(session, ats, slug)

                    if not any(gl in loc_lower for gl in CONFIG["GEO_LOCKS"]):
                        if any(k in loc_lower for k in ["worldwide", "anywhere", "india", "apac", "asia", "global"]) or loc_lower == "":
                            record_job(j.get("id", url), title, comp, loc, classify_role(title), url, dt)
    except Exception as e:
        log.warning(f"Jobicy error: {e}")

    # 3. Arbeitnow
    try:
        async with session.get("https://www.arbeitnow.com/api/job-board-api", headers=BROWSER_HEADERS, timeout=15) as resp:
            if resp.status == 200:
                data = await resp.json()
                for j in data.get("data", []):
                    if j.get("remote"):
                        title = j.get("title", "")
                        loc = j.get("location", "Remote")
                        url = j.get("url", "")
                        comp = j.get("company_name", "Tech Startup")
                        dt = parse_iso_datetime(j.get("created_at"))
                        if is_eligible_remote(title, loc):
                            record_job(j.get("slug", url), title, comp, loc, classify_role(title), url, dt)
    except Exception as e:
        pass

# ==============================================================================
# 6. MAIN ENGINE EXECUTION
# ==============================================================================
async def main():
    start = datetime.now()
    registry = CompanyRegistry(CONFIG["COMPANIES_FILE"])
    sem = asyncio.Semaphore(CONFIG["MAX_CONCURRENT_REQUESTS"])

    async with aiohttp.ClientSession(headers=BROWSER_HEADERS) as session:
        # Step 1: Auto-discover new companies and update companies.json
        await run_search_discovery(session, registry)
        await scrape_aggregators_and_discover(session, registry)
        registry.save()

        # Step 2: Scrape live boards across all registered companies
        tasks = []
        for slug in registry.slug_sets["greenhouse"]:
            tasks.append(scrape_greenhouse(session, slug, sem))
        for slug in registry.slug_sets["lever"]:
            tasks.append(scrape_lever(session, slug, sem))
        for slug in registry.slug_sets["ashby"]:
            tasks.append(scrape_ashby(session, slug, sem))

        total_companies = len(tasks)
        log.info(f"⚡ Ingesting live boards across {total_companies} companies...")
        await asyncio.gather(*tasks)

    # Sort descending by newest dateAdded first
    jobs_output.sort(key=lambda x: x.get("dateAdded", ""), reverse=True)

    with open(CONFIG["JOBS_FILE"], "w") as f:
        json.dump(jobs_output, f, indent=2)

    elapsed = (datetime.now() - start).total_seconds()
    log.info(f"✅ Finished in {elapsed:.2f}s.")
    log.info(f"🎯 Total verified active jobs saved in jobs.json: {len(jobs_output)}")

if __name__ == "__main__":
    import sys
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
