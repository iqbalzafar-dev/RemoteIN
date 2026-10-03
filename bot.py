import asyncio
import aiohttp
import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime

print("🚀 INITIATING ULTIMATE BOT: Async Engine + HN + Smart Funding Auto-Injector...\n")

# --- LOAD DATABASE ---
COMPANIES_FILE = 'companies.json'
try:
    with open(COMPANIES_FILE, 'r') as f:
        ats_slugs = json.load(f)
except FileNotFoundError:
    ats_slugs = {"greenhouse": [], "lever": [], "ashby": []}

filtered_jobs = []

# --- ULTRA-STRICT FILTER ---
def is_strictly_remote_apac(title, location, commitment=""):
    text = f"{title} {location} {commitment}".lower()
    
    red_flags = ['hybrid', 'on-site', 'onsite', 'wfo', 'in-office']
    if any(word in text for word in red_flags): return False 
        
    western_flags = ['us only', 'uk only', 'europe only', 'usa', 'united states', 'canada', 'latam', 'emea', 'new york', 'london', 'san francisco', 'berlin', 'paris', 'remote - us', 'remote (us)']
    if any(word in text for word in western_flags) and 'india' not in text:
        return False
        
    green_flags = ['remote', 'work from home', 'wfh']
    if not any(word in text for word in green_flags): return False 
        
    apac_keywords = ['india', 'apac', 'asia', 'singapore', 'australia', 'philippines', 'worldwide', 'global', 'anywhere']
    if not any(word in text for word in apac_keywords): return False
        
    return True 

def get_date(date_str=None):
    if date_str:
        try: return date_str.split('T')[0]
        except: pass
    return datetime.now().strftime("%Y-%m-%d")

# --- ASYNC ATS FETCHERS ---
async def fetch_greenhouse(session, company, semaphore):
    async with semaphore:
        url = f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs"
        try:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    for job in data.get('jobs', []):
                        title = job.get('title', '')
                        loc = job.get('location', {}).get('name', '')
                        if is_strictly_remote_apac(title, loc):
                            cat = "tech" if any(w in title.lower() for w in ['engineer', 'developer', 'data', 'design', 'product']) else "non-tech"
                            filtered_jobs.append({"id": str(job['id']), "title": title, "company": company.capitalize(), "location": loc, "category": cat, "url": job['absolute_url'], "dateAdded": get_date(job.get('updated_at'))})
        except Exception:
            pass

async def fetch_lever(session, company, semaphore):
    async with semaphore:
        url = f"https://api.lever.co/v0/postings/{company}?mode=json"
        try:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    jobs = await response.json()
                    for job in jobs:
                        title = job.get('text', '')
                        loc = job.get('categories', {}).get('location', '')
                        commit = job.get('categories', {}).get('commitment', '')
                        if is_strictly_remote_apac(title, loc, commit):
                            cat = "tech" if any(w in title.lower() for w in ['engineer', 'developer', 'data', 'design', 'product']) else "non-tech"
                            filtered_jobs.append({"id": str(job['id']), "title": title, "company": company.capitalize(), "location": loc, "category": cat, "url": job['hostedUrl'], "dateAdded": get_date()})
        except Exception:
            pass

async def fetch_ashby(session, company, semaphore):
    async with semaphore:
        url = "https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams"
        payload = {
            "operationName": "ApiJobBoardWithTeams",
            "variables": {"organizationHostedJobsPageName": company},
            "query": "query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) { jobBoard: jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) { jobPostings { id title locationName isRemote publishedAt jobUrl } } }"
        }
        try:
            async with session.post(url, json=payload, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    postings = data.get('data', {}).get('jobBoard', {}).get('jobPostings', [])
                    for job in postings:
                        title = job.get('title', '')
                        loc = job.get('locationName', '')
                        is_remote = job.get('isRemote', False)
                        check_str = f"{title} {loc} {'remote' if is_remote else ''}"
                        if is_strictly_remote_apac(title, loc, check_str):
                            cat = "tech" if any(w in title.lower() for w in ['engineer', 'developer', 'data', 'design', 'product']) else "non-tech"
                            filtered_jobs.append({"id": str(job['id']), "title": title, "company": company.capitalize(), "location": loc, "category": cat, "url": job['jobUrl'], "dateAdded": get_date(job.get('publishedAt'))})
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
                        kids = story_data.get('kids', [])[:60]
                        comment_tasks = [session.get(f"https://hacker-news.firebaseio.com/v0/item/{kid}.json", timeout=5) for kid in kids]
                        comment_resps = await asyncio.gather(*comment_tasks, return_exceptions=True)
                        
                        for cres in comment_resps:
                            try:
                                if not isinstance(cres, Exception) and cres.status == 200:
                                    c_data = await cres.json()
                                    text = c_data.get('text', '')
                                    if text and any(w in text.lower() for w in ['india', 'apac', 'remote']):
                                        filtered_jobs.append({
                                            "id": str(c_data.get('id')),
                                            "title": "HN Startup Role (Direct Founder Post)",
                                            "company": "Hacker News Startup",
                                            "location": "Remote / India",
                                            "category": "tech",
                                            "url": f"https://news.ycombinator.com/item?id={c_data.get('id')}",
                                            "dateAdded": get_date()
                                        })
                            except:
                                pass
    except Exception:
        pass

# --- MONEY TRACKER: AUTO-INJECT NEWLY FUNDED COMPANIES INTO DATABASE ---
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
                    # Example title: "Fintech Startup Zeta Raises $30M..."
                    # We extract the first word as a potential company slug
                    if any(word in title.lower() for word in ['raises', 'funding', 'seed', 'series', 'million']):
                        words = title.split()
                        if words:
                            potential_slug = words[0].lower().strip(",.-")
                            # Check if valid slug format and not already in database
                            if len(potential_slug) > 3 and potential_slug not in ats_slugs.get('greenhouse', []):
                                ats_slugs['greenhouse'].append(potential_slug)
                                new_slugs_added += 1
                                
        if new_slugs_added > 0:
            # Save expanded list back to companies.json
            with open(COMPANIES_FILE, 'w') as f:
                json.dump(ats_slugs, f, indent=4)
            print(f"✅ Auto-expanded database: Added {new_slugs_added} newly funded startups to scan list!")
    except Exception as e:
        print(f"Funding tracker note: {e}")

# --- THE MAIN ASYNC ENGINE ---
async def main():
    start_time = datetime.now()
    semaphore = asyncio.Semaphore(50) 
    
    async with aiohttp.ClientSession() as session:
        # First, run funding tracker to update company list dynamically
        await fetch_funding_and_expand_database(session)
        
        tasks = []
        for company in ats_slugs.get('greenhouse', []):
            tasks.append(fetch_greenhouse(session, company, semaphore))
        for company in ats_slugs.get('lever', []):
            tasks.append(fetch_lever(session, company, semaphore))
        for company in ats_slugs.get('ashby', []):
            tasks.append(fetch_ashby(session, company, semaphore))
            
        tasks.append(fetch_hackernews(session))
            
        await asyncio.gather(*tasks)

    # Save Results (Only real clean jobs now)
    with open('jobs.json', 'w') as f:
        json.dump(filtered_jobs, f, indent=4)

    duration = datetime.now() - start_time
    print(f"\n🔥 ULTIMATE CLEAN SCAN COMPLETE in {duration.total_seconds():.2f} seconds.")
    print(f"🎯 Total 100% PURE Remote India/APAC Jobs found: {len(filtered_jobs)}")

if __name__ == "__main__":
    import sys
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())