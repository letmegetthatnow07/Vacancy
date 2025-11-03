#!/usr/bin/env python3
# collector.py — COMPLETE HYBRID with CloudScraper for SSL bypass
# Uses cloudscraper to handle SSL certificate verification issues

import cloudscraper
import json, sys, re, time, os, hashlib, pathlib
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import defaultdict

# Create cloudflare-aware scraper (handles SSL automatically!)
scraper = cloudscraper.create_scraper()

UA = {"User-Agent": "Mozilla/5.0"}

# Load rules.json
try:
    rules_content = pathlib.Path("rules.json").read_text(encoding="utf-8")
    RULES = json.loads(rules_content)
except FileNotFoundError:
    print("[WARN] rules.json not found, using defaults", file=sys.stderr)
    RULES = {}
except json.JSONDecodeError as e:
    print(f"[ERROR] rules.json invalid JSON: {e}, using defaults", file=sys.stderr)
    RULES = {}
except Exception as e:
    print(f"[ERROR] Unexpected error loading rules.json: {e}", file=sys.stderr)
    RULES = {}

AGG_SCORES = RULES.get("aggregatorScores", {})

# OFFICIAL SITES - with domicile tracking
OFFICIAL_SITES = [
    ("https://ssc.gov.in/", "a[href]", "SSC", "All India"),
    ("https://bpsc.bihar.gov.in/", "a[href]", "BPSC", "Bihar"),
    ("https://bssc.bihar.gov.in/", "a[href]", "BSSC", "Bihar"),
    ("https://www.ibps.in/", "a[href]", "IBPS", "All India"),
    ("https://opportunities.rbi.org.in/Scripts/Vacancies.aspx", "a[href]", "RBI", "All India"),
    ("https://www.isro.gov.in/Careers.html", "a[href]", "ISRO", "All India"),
    ("https://www.rrbcdg.gov.in/", "a[href]", "RRB", "All India"),
    ("https://www.rrbapply.gov.in/", "a[href]", "RRB", "All India"),
    ("https://bssc.bihar.gov.in/advertisement/", "a[href]", "BSSC", "Bihar"),
    ("https://dda.gov.in/", "a[href]", "DDA", "Delhi"),
    ("https://dsssb.delhi.gov.in/", "a[href]", "DSSSB", "Delhi"),
    ("https://nests.tribal.gov.in/", "a[href]", "EMRS", "All India"),
    ("https://ccras.nic.in/", "a[href]", "CCRAS", "All India"),
    ("https://www.upsc.gov.in/", "a[href]", "UPSC", "All India"),
]

# AGGREGATORS
AGGREGATORS = [
    "https://www.freejobalert.com/",
    "https://www.freejobalert.com/articles/",
    "https://sarkarijobfind.com/",
    "https://www.resultbharat.com/",
    "https://www.rojgarresult.com/",
    "https://www.adda247.com/jobs/"
]

# SIMPLE negative keywords (permissive)
NEG_KEYWORDS = ["result", "cutoff", "admit card", "syllabus", "answer key", "merit list"]

# POSITIVE keywords (legitimacy)
POS_KEYWORDS = ["recruitment", "vacancy", "job", "opening", "notification", "application", "register", "apply"]

# BLOCK regex for obvious blocked positions
BLOCK = re.compile(r"(teacher|tgt|pgt|prt|b\.?ed|ctet|tet|b\.?tech|m\.?tech|b\.e|m\.e|mca|bca|developer|architect|analyst|nursing|pharma|iti|polytechnic|diploma|mba|msc|m\.sc|phd|post\s*graduate)", re.I)

# Posts pattern
POSTS_PAT = re.compile(r"(\d{1,6})\s*(posts?|vacanc(?:y|ies)|seats?)", re.I)

def clean(s): 
    return re.sub(r"\s+", " ", (s or "").strip())

def host(u):
    try: 
        return urlparse(u or "").netloc.lower()
    except: 
        return ""

def stable_id(url):
    try:
        norm_url = (url or "").lower().strip()
        return f"job_{hashlib.sha1(norm_url.encode()).hexdigest()[:12]}"
    except:
        return f"job_{hashlib.md5((url or '').lower().encode()).hexdigest()[:12]}"

def detect_qualification(title):
    """Extract qualification level from job title"""
    title_lower = (title or "").lower()
    
    if re.search(r"(graduate|degree|university|b\.sc|b\.a|b\.com)", title_lower):
        return "Any graduate"
    elif re.search(r"(12th|hsc|intermediate|inter-?level)", title_lower):
        return "12th Pass"
    elif re.search(r"(10th|matric|ssc\b)", title_lower):
        return "10th Pass"
    
    return "Any graduate"

def posts_from_text(txt):
    """Extract number of posts from text"""
    m = POSTS_PAT.search(txt or "")
    if not m: 
        return None
    try: 
        return int(m.group(1))
    except: 
        return None

def extract_pdf_link(job_url, base_url):
    """Extract PDF link from job posting page"""
    try:
        if not job_url or not isinstance(job_url, str):
            return None, False
        
        # FIX: Use cloudscraper instead of requests
        r = scraper.get(job_url, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        
        for link in soup.find_all('a'):
            href = link.get('href', '').lower()
            if '.pdf' in href:
                full_url = href if href.startswith('http') else urljoin(base_url, href)
                print(f"[PDF_FOUND] {job_url[:60]} → {full_url[:60]}", file=sys.stderr)
                return full_url, False
        
        return None, False
    
    except Exception as e:
        print(f"[PDF_ERR] {job_url[:60]}: {type(e).__name__}", file=sys.stderr)
        return None, False

def is_relevant(title):
    """Simple relevance check"""
    title_lower = (title or "").lower()
    
    has_positive = any(kw in title_lower for kw in POS_KEYWORDS)
    if not has_positive:
        return False
    
    has_negative = any(kw in title_lower for kw in NEG_KEYWORDS)
    if has_negative:
        return False
    
    if BLOCK.search(title):
        return False
    
    return True

def fetch_site(url, selector):
    """Fetch jobs from a site"""
    try:
        # FIX: Use cloudscraper instead of requests
        r = scraper.get(url, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        
        jobs = []
        for a in soup.select(selector):
            title = clean(a.get_text(" ", strip=True))
            href = a.get("href", "")
            
            if not title or not href or len(title) < 5:
                continue
            
            if not is_relevant(title):
                continue
            
            full_url = href if href.startswith("http") else urljoin(url, href)
            jobs.append((title, full_url))
        
        print(f"[FETCH_OK] {url[:50]}: found {len(jobs)} jobs", file=sys.stderr)
        return jobs
    
    except Exception as e:
        print(f"[FETCH_ERR] {url[:50]}: {type(e).__name__}", file=sys.stderr)
        return []

def collect():
    """Collect from all sources"""
    all_jobs = []
    agg_counts = defaultdict(int)
    
    print("[COLLECT] Starting official sites scrape...", file=sys.stderr)
    for url, sel, org, domicile in OFFICIAL_SITES:
        print(f"[FETCH] {url[:50]}...", file=sys.stderr)
        for title, link in fetch_site(url, sel):
            qual = detect_qualification(title)
            posts = posts_from_text(title)
            
            pdf_link = None
            has_posts = posts is not None
            has_explicit_qual = qual != "Any graduate"
            
            if not has_posts and not has_explicit_qual:
                pdf_link, _ = extract_pdf_link(link, url)
            
            job = {
                "id": stable_id(link),
                "title": title,
                "url": link,
                "source": "official",
                "org": org,
                "domicile": domicile,
                "qual": qual,
                "posts": posts,
                "pdf_link": pdf_link,
                "agg_count": 0
            }
            
            all_jobs.append(job)
        
        time.sleep(0.5)
    
    print("[COLLECT] Starting aggregators scrape...", file=sys.stderr)
    for agg_url in AGGREGATORS:
        print(f"[FETCH] {agg_url[:50]}...", file=sys.stderr)
        for title, link in fetch_site(agg_url, "a[href]"):
            norm_title = title.lower().strip()
            agg_counts[norm_title] += 1
            
            qual = detect_qualification(title)
            posts = posts_from_text(title)
            agg_host = host(agg_url)
            
            job = {
                "id": stable_id(link),
                "title": title,
                "url": link,
                "source": "aggregator",
                "domicile": "All India",
                "qual": qual,
                "posts": posts,
                "agg_host": agg_host,
                "agg_score": AGG_SCORES.get(agg_host, 0.6),
                "agg_count": agg_counts[norm_title]
            }
            
            all_jobs.append(job)
        
        time.sleep(0.3)
    
    return all_jobs, agg_counts

def dedup_and_rank(items, agg_counts):
    """
    COMPLETE dedup logic:
    - Keep ALL official jobs
    - Keep aggregator jobs IF found in 2+ aggregators or from high-scoring aggregator
    """
    bykey = {}
    
    for j in items:
        key = (j["title"].lower(), urlparse(j["url"]).path.lower())
        
        if key not in bykey:
            bykey[key] = j
            continue
        
        a = bykey[key]
        b = j
        
        if a["source"] == "official" and b["source"] != "official": 
            continue
        if b["source"] == "official" and a["source"] != "official": 
            bykey[key] = b
            continue
        
        if a["source"] == "aggregator" and b["source"] == "aggregator":
            sa = AGG_SCORES.get(a["agg_host"], 0.6)
            sb = AGG_SCORES.get(b["agg_host"], 0.6)
            if sb > sa:
                bykey[key] = b
        
        bykey[key].setdefault("flags", {})["corroborated"] = True
    
    final = []
    for job in bykey.values():
        if job["source"] == "official":
            final.append(job)
        elif job.get("agg_count", 0) >= 2:
            final.append(job)
            job["corroborated"] = True
    
    return final

if __name__ == "__main__":
    out, agg_counts = collect()
    out = dedup_and_rank(out, agg_counts)
    
    for j in out:
        j.setdefault("domicile", "All India")
    
    print(f"[DONE] Collected {len(out)} total jobs", file=sys.stderr)
    
    for j in out:
        rec = {
            "id": j["id"],
            "title": j["title"],
            "applyLink": j["url"],
            "detailLink": j["url"],
            "source": j["source"],
            "domicile": j.get("domicile", "All India"),
            "type": "VACANCY",
            "qualificationLevel": j.get("qual", "Any graduate")
        }
        
        if j.get("posts"):
            rec["numberOfPosts"] = j["posts"]
        
        if j.get("pdf_link"):
            rec["pdfLink"] = j["pdf_link"]
            rec.setdefault("flags", {})["needs_pdf_review"] = True
        
        if j.get("corroborated"):
            rec.setdefault("flags", {})["corroborated"] = True
        
        print(json.dumps(rec, ensure_ascii=False))
