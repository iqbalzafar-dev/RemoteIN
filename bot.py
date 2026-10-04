import asyncio
import aiohttp
import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

print("🚀 INITIATING STRICT FRESHNESS BOT: Eliminating Zombie Jobs...\n")

# --- LOAD DATABASE ---
COMPANIES_FILE = 'companies.json'
try:
    with open(COMPANIES_FILE, 'r') as f:
        ats_slugs = json.load(f)
except FileNotFoundError:
    ats_slugs = {
        "greenhouse": ["razorpay", "gitlab", "postman", "automattic"],
        "lever": ["figma", "atlassian"],
        "ashby": ["deel", "multiplier", "linear", "ramp"]
    }

filtered_jobs = []
seen_job_urls = set()

# --- STRICT 30-DAY FRESHNESS WINDOW ---
CUTOFF_DATE = datetime.now(timezone.utc) - timedelta(days=30)

def parse_any_date(raw_val):
    """Multiple date formats ko safely datetime object me convert karta hai"""
    if not raw_val:
        return None
        
    # Case 1: Millisecond integer / epoch timestamp (Lever & HackerNews)
    if isinstance(raw_val, (int, float)):
        try:
            # Agar milliseconds me hai (> 10 digits)
            ts = raw_val / 1000 if raw_val > 10000000000 else raw_val
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            return None
            
    # Case 2: ISO string (Greenhouse / Ashby)
    if isinstance(raw_val, str):
        raw_val = raw_val.strip()
        try:
            clean_str = raw_val.replace("Z", "+00:00")
            dt = datetime.fromisoformat(clean_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            pass
            
        # Case 3: RFC 2822 format (Jobicy / RSS: "Sun, 04 Oct 2026 12:00:00 +0000")
        for fmt in (
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%d"
        ):
            try:
                dt = datetime.strptime(raw_val, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                continue
                
    return None

def is_recent(dt_obj):
    """Agar date missing hai ya 30 din se purani hai toh STRICTLY REJECT"""
    if not dt_obj:
        return False
    return dt_obj >= CUTOFF_DATE

# --- SMART CATEGORY CLASSIFIER ---
def classify_role(title):
    t = title.lower()
    if any(k in t for k in ["payroll", "compensation", "benefits", "comp & ben"]):
        return "Payroll & Compliance"
    elif any(k in t for k in ["hr", "recruiter", "talent acquisition", "people ops", "human resources", "talent"]):
        return "HR & People Ops"
    elif any(k in t for k in ["finance", "accountant", "accounting", "billing", "audit", "tax", "fp&a", "treasury"]):
        return "Finance & Accounting"
    elif any(k in t for k in ["operations", "vendor ops", "customer support", "customer success", "ops associate", "support specialist"]):
        return "Operations & Support"
    elif any(k in t for k in ["engineer", "developer", "data", "software", "product", "designer", "architect", "devops"]):
        return "Tech & Engineering"
    return "General Operations"

# --- ULTRA-STRICT REMOTE APAC / INDIA FILTER ---
def is_strictly_remote_apac(title, location, commitment=""):
    text = f"{title} {location} {commitment}".lower()
    
    red_flags = ['hybrid', 'on-site', 'onsite', 'wfo', 'in-office']
    if any(word in text for word in red_flags): 
        return False 
        
    western_flags = ['us only', 'uk only', 'europe only', 'usa', 'united states', 'canada', 'latam', 'emea', 'new york', 'london', 'san francisco', 'berlin', 'remote - us', 'remote (us)']
    if any(word in text for word in western_flags) and 'india' not in text:
        return False
        
    green_flags = ['remote', 'work from home', 'wfh', 'anywhere', 'distributed']
    if not any(word in text for word in green_flags): 
        return False 
        
    apac_keywords = ['india', 'apac', 'asia', 'singapore', 'australia', 'philippines', 'worldwide', 'global', 'anywhere']
    if not any(word in text for word in apac_keywords): 
        return False
        
    return True 

def add_job_record(job_id, title, company, location, category, url, dt_obj):
    if not is_recent(dt_obj):
        return  # Strictly block any expired job from entering
        
    if url in seen_job_urls:
        return
    seen_job_urls.add(url)
    
    filtered_jobs.append({
        "id": str(job_id),
        "title": title,
        "company": company.capitalize(),
        "location": location if location else "Remote (India/Global)",
        "category": category,
        "url": url,
        "dateAdded": dt_obj.strftime("%Y-%m-%d")
    })

# --- ASYNC ATS FETCHERS ---
async def fetch_greenhouse(session, company, semaphore):
    async with semaphore:
        url = f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs"
        try:
            async with session.get(url, timeout=12) as response:
                if response.status == 200:
                    data = await response.json()
                    for job in data.get('jobs', []):
                        title = job.get('title', '')
                        loc = job.get('location', {}).get('name', '')
                        dt_obj = parse_any_date(job.get('updated_at'))
                        
                        if is_recent(dt_obj) and is_strictly_remote_apac(title, loc):
                            cat = classify_role(title)
                            add_job_record(job['id'], title, company, loc, cat, job['absolute_url'], dt_obj)
        except Exception:
            pass

async def fetch_lever(session, company, semaphore):
    async with semaphore:
        url = f"https://api.lever.co/v0/postings/{company}?mode=json"
        try:
            async with session.get(url, timeout=12) as response:
                if response.status == 200:
                    jobs = await response.json()
                    for job in jobs:
                        title = job.get('text', '')
                        loc = job.get('categories', {}).get('location', '')
                        commit = job.get('categories', {}).get('commitment', '')
                        dt_obj = parse_any_date(job.get('createdAt'))
                        
                        if is_recent(dt_obj) and is_strictly_remote_apac(title, loc, commit):
                            cat = classify_role(title)
                            add_job_record(job['id'], title, company, loc, cat, job.get('hostedUrl', ''), dt_obj)
        except Exception:
            pass

async def fetch_ashby(session, company, semaphore):
    async with semaphore:
        api_url = f"https://api.ashbyhq.com/posting-api/job-board/{company}"
        try:
            async with session.get(api_url, timeout=12) as response:
                if response.status == 200:
                    data = await response.json()
                    postings = data.get('jobs', [])
                    for job in postings:
                        title = job.get('title', '')
                        loc = job.get('location', '')
                        is_remote = job.get('isRemote', False)
                        dt_obj = parse_any_date(job.get('publishedAt'))
                        
                        check_str = f"{title} {loc} {'remote' if is_remote else ''}"
                        if is_recent(dt_obj) and is_strictly_remote_apac(title, loc, check_str):
                            cat = classify_role(title)
                            add_job_record(job['id'], title, company, loc, cat, job.get('jobUrl', ''), dt_obj)
        except Exception:
            pass

# --- BULK VOLUME ENGINE ---
async def fetch_bulk_remote_feeds(session):
    print("🌐 Pulling high-volume verified feeds...")
    
    # 1. Jobicy
    jobicy_url = "https://jobicy.com/api/v2/remote-jobs?count=50&geo=apac"
    try:
        async with session.get(jobicy_url, timeout=12) as resp:
            if resp.status == 200:
                data = await resp.json()
                for job in data.get('jobs', []):
                    title = job.get('jobTitle', '')
                    loc = job.get('jobGeo', '')
                    dt_obj = parse_any_date(job.get('pubDate'))
                    if is_recent(dt_obj) and is_strictly_remote_apac(title, loc):
                        cat = classify_role(title)
                        add_job_record(job.get('id', title), title, job.get('companyName', 'Global Remote'), loc, cat, job.get('url'), dt_obj)
    except Exception:
        pass

    # 2. Arbeitnow
    arbeit_url = "https://www.arbeitnow.com/api/job-board-api"
    try:
        async with session.get(arbeit_url, timeout=12) as resp:
            if resp.status == 200:
                data = await resp.json()
                for job in data.get('data', []):
                    if job.get('remote') is True:
                        title = job.get('title', '')
                        loc = job.get('location', 'Remote')
                        dt_obj = parse_any_date(job.get('created_at'))
                        if is_recent(dt_obj) and is_strictly_remote_apac(title, loc):
                            cat = classify_role(title)
                            add_job_record(job.get('slug', title), title, job.get('company_name', 'Tech Co'), loc, cat, job.get('url'), dt_obj)
    except Exception:
        pass

# --- HIDDEN GEM: HACKER NEWS SCOUT ---
async def fetch_hackernews(session):
    print("🕵️‍♂️ Scouting Hacker News 'Who is Hiring' threads...")
    try:
        search_url = "https://hn.algolia.com/api/v1/search?tags=story,author_whoishiring&query=Who%20is%20hiring"
        async with session.get(search_url, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                hits = data.get('hits', [])
                if not hits: return
                latest_story_id = hits[0]['objectID']
                
                item_url = f"https://hacker-news.firebaseio.com/v0/item/{latest_story_id}.json"
                async with session.get(item_url, timeout=10) as item_resp:
                    if item_resp.status == 200:
                        story_data = await item_resp.json()
                        kids = story_data.get('kids', [])[:40]
                        comment_tasks = [session.get(f"https://hacker-news.firebaseio.com/v0/item/{kid}.json", timeout=5) for kid in kids]
                        comment_resps = await asyncio.gather(*comment_tasks, return_exceptions=True)
                        
                        for cres in comment_resps:
                            try:
                                if not isinstance(cres, Exception) and cres.status == 200:
                                    c_data = await cres.json()
                                    text = c_data.get('text', '')
                                    dt_obj = parse_any_date(c_data.get('time'))
                                    
                                    if is_recent(dt_obj) and text and any(w in text.lower() for w in ['india', 'apac', 'remote']):
                                        cat = classify_role(text[:100])
                                        add_job_record(c_data.get('id'), "Direct Founder Role (HN Startup)", "Hacker News Startup", "Remote / India", cat, f"https://news.ycombinator.com/item?id={c_data.get('id')}", dt_obj)
                            except:
                                pass
    except Exception:
        pass

# --- THE MAIN ASYNC ENGINE ---
async def main():
    start_time = datetime.now()
    semaphore = asyncio.Semaphore(50) 
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for company in ats_slugs.get('greenhouse', []):
            tasks.append(fetch_greenhouse(session, company, semaphore))
        for company in ats_slugs.get('lever', []):
            tasks.append(fetch_lever(session, company, semaphore))
        for company in ats_slugs.get('ashby', []):
            tasks.append(fetch_ashby(session, company, semaphore))
            
        tasks.append(fetch_bulk_remote_feeds(session))
        tasks.append(fetch_hackernews(session))
            
        await asyncio.gather(*tasks)

    # Sort descending by date (Most recent first)
    filtered_jobs.sort(key=lambda x: x.get('dateAdded', ''), reverse=True)

    # Save Results
    with open('jobs.json', 'w') as f:
        json.dump(filtered_jobs, f, indent=4)

    duration = datetime.now() - start_time
    print(f"\n🔥 STRICT SCAN COMPLETE in {duration.total_seconds():.2f} seconds.")
    print(f"🎯 Total 100% VERIFIED FRESH (Last 30 Days) Jobs found: {len(filtered_jobs)}")

if __name__ == "__main__":
    import sys
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
