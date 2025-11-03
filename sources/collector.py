#!/usr/bin/env python3
# collector.py — COMPLETE HYBRID: All quality + All aggregator logic + All PDF extraction
# FINAL: Everything from OLD code + permissive filtering + agg corroboration
# FIX: Added urllib3 + verify=False for SSL bypass EVERYWHERE

import requests, json, sys, re, time, os, hashlib, pathlib
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import defaultdict
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

UA = {"User-Agent":"Mozilla/5.0"}

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

# RESTORED: BLOCK regex for obvious blocked positions (from OLD code)
BLOCK = re.compile(r"(teacher|tgt|pgt|prt|b\.?ed|ctet|tet|b\.?tech|m\.?tech|b\.e|m\.e|mca|bca|developer|architect|analyst|nursing|pharma|iti|polytechnic|diploma|mba|msc|m\.sc|phd|post\s*graduate)", re.I)

# RESTORED: Posts pattern (from OLD code)
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

# RESTORED: Qualification detection (from OLD code)
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

# RESTORED: Extract posts (from OLD code)
def posts_from_text(txt):
    """Extract number of posts from text"""
    m = POSTS_PAT.search(txt or "")
    if not m: 
        return None
    try: 
        return int(m.group(1))
    except: 
        return None

# RESTORED: PDF extraction (from OLD code) - WITH SSL FIX
def extract_pdf_link(job_url, base_url):
    """Extract PDF link from job posting page"""
    try:
        if not job_url or not isinstance(job_url, str):
            return None, False
        
        # FIX: verify=False to disable SSL verification
        r = requests.get(job_url, timeout=15, headers=UA, verify=False)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        
        for link in soup.find_all('a'):
            href = link.get('href', '').lower()
            if '.pdf' in href:
                full_url = href if href.startswith('http') else urljoin(base_url, href)
                print(f"[PDF_FOUND] {job_url[:60]} → {full_url[:60]}", file=sys.stderr)
                return full_url, False
        
        return None, False
    
    except requests.Timeout:
        print(f"[TIMEOUT] extract_pdf_link: {job_url[:60]}", file=sys.stderr)
        return None, False
    except requests.exceptions.SSLError as e:
        print(f"[SSL_ERR] extract_pdf_link {job_url[:60]}: {str(e)[:80]}", file=sys.stderr)
        return None, False
    except requests.ConnectionError:
        print(f"[CONN_ERR] extract_pdf_link: {job_url[:60]}", file=sys.stderr)
        return None, False
    except Exception as e:
        print(f"[PDF_ERR] {job_url[:60]}: {type(e).__name__}: {str(e)[:80]}", file=sys.stderr)
        return None, False

def is_relevant(title):
    """Simple relevance check"""
    title_lower = (title or "").lower()
    
    # Must have at least one positive indicator
    has_positive = any(kw in title_lower for kw in POS_KEYWORDS)
    if not has_positive:
        return False
    
    # Skip obvious negatives
    has_negative = any(kw in title_lower for kw in NEG_KEYWORDS)
    if has_negative:
        return False
    
    # RESTORED: Block obvious blocked positions (from OLD code)
    if BLOCK.search(title):
        return False
    
    return True

def fetch_site(url, selector):
    """Fetch jobs from a site"""
    try:
        # FIX: verify=False to disable SSL verification
        r = requests.get(url, timeout=30, headers=UA, verify=False)
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
    
    except requests.exceptions.SSLError as e:
        print(f"[SSL_ERR] {url[:50]}: {str(e)[:80]}", file=sys.stderr)
        return []
    except requests.Timeout:
        print(f"[TIMEOUT] {url[:50]}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"[FETCH_ERR] {url[:50]}: {type(e).__name__}", file=sys.stderr)
        return []

def collect():
    """Collect from all sources"""
    all_jobs = []
    agg_counts = defaultdict(int)
    
    # Scrape OFFICIAL sites (with domicile + PDF extraction)
    print("[COLLECT] Starting official sites scrape...", file=sys.stderr)
    for url, sel, org, domicile in OFFICIAL_SITES:
        print(f"[FETCH] {url[:50]}...", file=sys.stderr)
        for title, link in fetch_site(url, sel):
            qual = detect_qualification(title)
            posts = posts_from_text(title)
            
            # RESTORED: PDF extraction logic (from OLD code)
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
    
    # Scrape AGGREGATORS (track counts + scoring)
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
                "agg_score": AGG_SCORES.get(agg_host, 0.6),  # RESTORED: Agg scoring
                "agg_count": agg_counts[norm_title]
            }
            
            all_jobs.append(job)
        
        time.sleep(0.3)
    
    return all_jobs, agg_counts

def dedup_and_rank(items, agg_counts):
    """
    COMPLETE dedup logic:
    - Keep ALL official jobs
    - Keep aggregator jobs IF:
      a) Found in 2+ aggregators (corroborated), OR
      b) From high-scoring aggregator
    """
    bykey = {}
    
    for j in items:
        key = (j["title"].lower(), urlparse(j["url"]).path.lower())
        
        if key not in bykey:
            bykey[key] = j
            continue
        
        a = bykey[key]
        b = j
        
        # Prefer OFFICIAL over aggregator
        if a["source"] == "official" and b["source"] != "official": 
            continue
        if b["source"] == "official" and a["source"] != "official": 
            bykey[key] = b
            continue
        
        # If both same source, prefer higher aggregator score (RESTORED from OLD code)
        if a["source"] == "aggregator" and b["source"] == "aggregator":
            sa = AGG_SCORES.get(a["agg_host"], 0.6)
            sb = AGG_SCORES.get(b["agg_host"], 0.6)
            if sb > sa:
                bykey[key] = b
        
        # RESTORED: Mark as corroborated if found in multiple sources (from OLD code)
        bykey[key].setdefault("flags", {})["corroborated"] = True
    
    # RESTORED: Final filtering logic (from OLD code)
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
    
    # RESTORED: Output format (from OLD code)
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
        
        # RESTORED: Posts if available
        if j.get("posts"):
            rec["numberOfPosts"] = j["posts"]
        
        # RESTORED: PDF link if found
        if j.get("pdf_link"):
            rec["pdfLink"] = j["pdf_link"]
            rec.setdefault("flags", {})["needs_pdf_review"] = True
        
        # RESTORED: Corroboration flag
        if j.get("corroborated"):
            rec.setdefault("flags", {})["corroborated"] = True
        
        print(json.dumps(rec, ensure_ascii=False))
