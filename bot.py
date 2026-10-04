import asyncio
import aiohttp
import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

print("🚀 INITIATING ULTIMATE SUPERCHARGED BOT: Multi-ATS + High Volume Streams...\n")

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

# --- FRESHNESS WINDOW (30 DAYS CUTOFF) ---
# Sirf pichle 30 din ke andar post hui active jobs hi accept hongi
CUTOFF_DATE = datetime.now(timezone.utc) - timedelta(days=30)

def parse_iso_date(date_str):
    if not date_str:
        return None
    try:
        clean_str = date_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(clean_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        try:
            return datetime.strptime(date_str.split('T')[0], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except Exception:
            return None

def is_recent_enough(dt_obj):
    if not dt_obj:
        return True # agar date mention nahi hai toh skip na karein
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
    if url in seen_job_urls:
        return
    seen_job_urls.add(url)
    
    formatted_date = dt_obj.strftime("%Y-%m-%d") if dt_obj else datetime.now().strftime("%Y-%m-%d")
    filtered_jobs.append({
        "id": str(job_id),
        "title": title,
        "company": company.capitalize(),
        "location": location if location else "Remote (India/Global)",
        "category": category,
        "url": url,
        "dateAdded": formatted_date
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
                        updated_at = job.get('updated_at')
                        dt_obj = parse_iso_date(updated_at)
                        
                        # 30 din se purani jobs ko drop karein
                        if not is_recent_enough(dt_obj):
                            continue
                            
                        if is_strictly_remote_apac(title, loc):
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
                        
                        # Lever createdAt is Unix timestamp (milliseconds)
                        created_at_ms = job.get('createdAt')
                        dt_obj = datetime.fromtimestamp(created_at_ms / 1000, tz=timezone.utc) if created_at_ms else None
                        
                        if not is_recent_enough(dt_obj):
                            continue
                            
                        if is_strictly_remote_apac(title, loc, commit):
                            cat = classify_role(title)
                            add_job_record(job['id'], title, company, loc, cat, job.get('hostedUrl', ''), dt_obj)
        except Exception:
            pass

async def fetch_ashby(session, company, semaphore):
    async with semaphore:
        # First try official public API endpoint (supports Deel, Multiplier etc directly)
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
                        published_at = job.get('publishedAt')
                        dt_obj = parse_iso_date(published_at)
                        
                        if not is_recent_enough(dt_obj):
                            continue
                            
                        check_str = f"{title} {loc} {'remote' if is_remote else ''}"
                        if is_strictly_remote_apac(title, loc, check_str):
                            cat = classify_role(title)
                            add_job_record(job['id'], title, company, loc, cat, job.get('jobUrl', ''), dt_obj)
                    return
        except Exception:
            pass

# --- BULK VOLUME ENGINE (Free High-Yield Verified Feeds) ---
async def fetch_bulk_remote_feeds(session):
    print("🌐 Pulling high-volume verified feeds (Jobicy & Arbeitnow)...")
    
    # 1. Jobicy Feed (HR, Finance, Support, Tech)
    jobicy_url = "https://jobicy.com/api/v2/remote-jobs?count=50&geo=apac"
    try:
        async with session.get(jobicy_url, timeout=12) as resp:
            if resp.status == 200:
                data = await resp.json()
                for job in data.get('jobs', []):
                    title = job.get('jobTitle', '')
                    loc = job.get('jobGeo', '')
                    pub_date = job.get('pubDate')
                    dt_obj = parse_iso_date(pub_date)
                    if is_recent_enough(dt_obj):
                        cat = classify_role(title)
                        add_job_record(job.get('id', title), title, job.get('companyName', 'Global Remote'), loc, cat, job.get('url'), dt_obj)
    except Exception:
        pass

    # 2. Arbeitnow API
    arbeit_url = "https://www.arbeitnow.com/api/job-board-api"
    try:
        async with session.get(arbeit_url, timeout=12) as resp:
            if resp.status == 200:
                data = await resp.json()
                for job in data.get('data', []):
                    if job.get('remote') is True:
                        title = job.get('title', '')
                        loc = job.get('location', 'Remote')
                        created_at = datetime.fromtimestamp(job.get('created_at', 0), tz=timezone.utc)
                        if is_recent_enough(created_at) and is_strictly_remote_apac(title, loc):
                            cat = classify_role(title)
                            add_job_record(job.get('slug', title), title, job.get('company_name', 'Tech Co'), loc, cat, job.get('url'), created_at)
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
                                    c_time = c_data.get('time')
                                    dt_obj = datetime.fromtimestamp(c_time, tz=timezone.utc) if c_time else None
                                    
                                    if is_recent_enough(dt_obj) and text and any(w in text.lower() for w in ['india', 'apac', 'remote']):
                                        cat = classify_role(text[:100])
                                        add_job_record(c_data.get('id'), "Direct Founder Role (HN Startup)", "Hacker News Startup", "Remote / India", cat, f"https://news.ycombinator.com/item?id={c_data.get('id')}", dt_obj)
                            except:
                                pass
    except Exception:
        pass

# --- MONEY TRACKER: AUTO-INJECT NEWLY FUNDED COMPANIES ---
async def fetch_funding_and_expand_database(session):
    print("💰 Scanning startup funding news to auto-expand company list...")
    rss_url = "https://inc42.com/feed/"
    new_slugs_added = 0
    try:
        async with session.get(rss_url, timeout=10) as resp:
            if resp.status == 200:
                xml_content = await resp.text()
                root = ET.fromstring(xml_content)
                
                for item in root.findall('.//item')[:20]:
                    title = item.find('title').text if item.find('title') is not None else ""
                    if any(word in title.lower() for word in ['raises', 'funding', 'seed', 'series', 'million']):
                        words = title.split()
                        if words:
                            potential_slug = words[0].lower().strip(",.-")
                            if len(potential_slug) > 3 and potential_slug not in ats_slugs.get('greenhouse', []):
                                ats_slugs.setdefault('greenhouse', []).append(potential_slug)
                                new_slugs_added += 1
                                
        if new_slugs_added > 0:
            with open(COMPANIES_FILE, 'w') as f:
                json.dump(ats_slugs, f, indent=4)
            print(f"✅ Auto-expanded database: Added {new_slugs_added} newly funded startups!")
    except Exception as e:
        print(f"Funding tracker note: {e}")

# --- THE MAIN ASYNC ENGINE ---
async def main():
    start_time = datetime.now()
    semaphore = asyncio.Semaphore(50) 
    
    async with aiohttp.ClientSession() as session:
        await fetch_funding_and_expand_database(session)
        
        tasks = []
        # Greenhouse
        for company in ats_slugs.get('greenhouse', []):
            tasks.append(fetch_greenhouse(session, company, semaphore))
        # Lever
        for company in ats_slugs.get('lever', []):
            tasks.append(fetch_lever(session, company, semaphore))
        # Ashby (Includes Deel, Multiplier, etc.)
        for company in ats_slugs.get('ashby', []):
            tasks.append(fetch_ashby(session, company, semaphore))
            
        # Additional Bulk & Community Feeds
        tasks.append(fetch_bulk_remote_feeds(session))
        tasks.append(fetch_hackernews(session))
            
        await asyncio.gather(*tasks)

    # Date ke mutabiq descending sort karein (Newest first)
    filtered_jobs.sort(key=lambda x: x.get('dateAdded', ''), reverse=True)

    # Save Clean Results
    with open('jobs.json', 'w') as f:
        json.dump(filtered_jobs, f, indent=4)

    duration = datetime.now() - start_time
    print(f"\n🔥 SCAN COMPLETE in {duration.total_seconds():.2f} seconds.")
    print(f"🎯 Total verified fresh remote jobs found: {len(filtered_jobs)}")

if __name__ == "__main__":
    import sys
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
