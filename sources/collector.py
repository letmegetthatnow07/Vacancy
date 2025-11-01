#!/usr/bin/env python3
# collector.py — official-first hybrid with all 5 aggregators, PDF detection, and auto-queue for OCR
import requests, json, sys, re, time, os, hashlib, pathlib
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

UA = {"User-Agent":"Mozilla/5.0"}
try:
    RULES = json.loads(pathlib.Path("rules.json").read_text(encoding="utf-8"))
except Exception:
    RULES = {}
AGG_SCORES = RULES.get("aggregatorScores", {})

OFFICIAL_SITES = [
    # existing hints are read dynamically by scraper; here we keep a compact cross-check set
    ("https://ssc.gov.in/", "a[href]", "SSC", "All India"),
    ("https://bpsc.bihar.gov.in/", "a[href]", "BPSC", "Bihar"),
    ("https://bssc.bihar.gov.in/", "a[href]", "BSSC", "Bihar"),
    ("https://www.ibps.in/", "a[href]", "IBPS", "All India"),
    ("https://opportunities.rbi.org.in/Scripts/Vacancies.aspx", "a[href]", "RBI", "All India"),
    ("https://www.isro.gov.in/Careers.html", "a[href]", "ISRO", "All India"),
    ("https://www.vssc.gov.in/careers.html", "a[href]", "ISRO/VSSC", "All India"),
    ("https://apps.ursc.gov.in/", "a[href]", "ISRO/URSC", "All India"),
    ("https://careers.sac.gov.in/", "a[href]", "ISRO/SAC", "All India"),
    # NEW: Major Indian Government Recruitment Sites
    ("https://www.onlinebssc.com/", "a[href]", "BSSC", "Bihar"),
    ("https://www.rrbapply.gov.in/#/auth/landing", "a[href]", "RRB", "All India"),
    ("https://nests.tribal.gov.in/show_content.php?lang=1&level=1&ls_id=949&lid=550", "a[href]", "EMRS/ESSE", "All India"),
    ("https://dda.gov.in/latest-jobs", "a[href]", "DDA", "Delhi"),
    ("https://dda.gov.in/", "a[href]", "DDA", "Delhi"),
    ("https://www.mha.gov.in/en/notifications/vacancies", "a[href]", "MHA", "All India"),
    ("https://www.westbengalssc.com/otr/recruitment/", "a[href]", "West Bengal SSC", "West Bengal"),
    ("https://www.csir.res.in/en/notification", "a[href]", "CSIR", "All India"),
    ("https://uppsc.up.nic.in/CandidatePages/Notifications.aspx", "a[href]", "UPPSC", "Uttar Pradesh"),
    ("https://www.upsc.gov.in/", "a[href]", "UPSC", "All India"),
]

AGGREGATORS = [
    ("https://www.freejobalert.com/", "a[href]"),
    ("https://sarkarijobfind.com/", "a[href]"),
    ("https://www.resultbharat.com/", "a[href]"),
    ("https://www.rojgarresult.com/", "a[href]"),
    ("https://www.adda247.com/jobs/", "a[href]")
]

NEG_TOK = re.compile(r"\b(result|cutoff|exam\s*date|admit\s*card|syllabus|answer\s*key)\b", re.I)
ALLOW_UPDATE = re.compile(r"\b(corrigendum|extension|extended|addendum|amendment|revised|rectified|last\s*date|re-?open|re-?opened|reopening)\b", re.I)
ALLOW_EDU = re.compile(r"(10th|matric|ssc\b|12th|intermediate|hsc|any\s+graduate|graduate\b)", re.I)
BLOCK = re.compile(r"(teacher|tgt|pgt|prt|b\.?ed|ctet|tet|b\.?tech|m\.?tech|b\.e|m\.e|mca|bca|developer|architect|analyst|nursing|pharma|iti|polytechnic|diploma|mba|msc|m\.sc|phd|post\s*graduate)", re.I)

POSTS_PAT = re.compile(r"(\d{1,6})\s*(posts?|vacanc(?:y|ies)|seats?)", re.I)
DATE_PAT  = re.compile(r"(\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b)", re.I)

def clean(s): return re.sub(r"\s+"," ", (s or "").strip())
def host(u):
    try: return urlparse(u or "").netloc.lower()
    except: return ""

def is_official(url):
    h=host(url)
    return (h.endswith(".gov.in") or h.endswith(".nic.in") or h.endswith(".gov") or h.endswith(".go.in") or "rbi.org.in" in h or "isro.gov.in" in h)

def extract_pdf_link(job_url, base_url):
    """Extract PDF link from job posting page"""
    try:
        r = requests.get(job_url, timeout=15, headers=UA)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        
        # Look for PDF links in page
        for link in soup.find_all('a'):
            href = link.get('href', '').lower()
            if '.pdf' in href:
                full_url = href if href.startswith('http') else urljoin(base_url, href)
                return full_url
        return None
    except:
        return None

def fetch(base, selector):
    try:
        r = requests.get(base, timeout=30, headers=UA)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        out=[]
        for a in soup.select(selector):
            t = a.get_text(" ", strip=True); h=a.get("href","")
            if not t or not h: continue
            if NEG_TOK.search(t) and not ALLOW_UPDATE.search(t): continue
            if not (ALLOW_EDU.search(t) or ALLOW_UPDATE.search(t)): continue
            url = h if h.startswith("http") else urljoin(base, h)
            out.append({"title":clean(t),"url":url,"isOfficial":is_official(url)})
        return out
    except:
        return []

def posts_from_text(txt):
    m=POSTS_PAT.search(txt or "")
    if not m: return None
    try: return int(m.group(1))
    except: return None

def stable_id(url):
    """Generate stable ID for job based on URL"""
    try:
        return f"job_{hashlib.sha1(url.lower().encode()).hexdigest()[:12]}"
    except:
        return f"job_{abs(hash(url))%10**9}"

def collect():
    res=[]
    for base,sel,org,dom in OFFICIAL_SITES:
        for it in fetch(base, sel):
            if BLOCK.search(it["title"]) and not ALLOW_UPDATE.search(it["title"]): continue
            rec={
                "id": stable_id(it["url"]),
                "title":it["title"], "applyLink":it["url"], "detailLink":it["url"],
                "source":"official","domicile":dom,"type":"UPDATE" if ALLOW_UPDATE.search(it["title"]) else "VACANCY",
                "qualificationLevel":"Any graduate"
            }
            p=posts_from_text(it["title"]); 
            if p: rec["numberOfPosts"]=p
            
            # NEW: Check if job is unclear (needs PDF review)
            # Mark if: no numberOfPosts AND qualification is generic
            if not rec.get("numberOfPosts") or rec.get("qualificationLevel") == "Any graduate":
                pdf_link = extract_pdf_link(it["url"], base)
                if pdf_link:
                    rec["pdfLink"] = pdf_link
                    rec.setdefault("flags", {})["needs_pdf_review"] = True
                    print(f"[PDF] {rec['title'][:50]} → {pdf_link[:50]}", file=sys.stderr)
            
            res.append(rec)
        time.sleep(0.25)
    
    # all five aggregators
    for base,sel in AGGREGATORS:
        for it in fetch(base, sel):
            if BLOCK.search(it["title"]) and not ALLOW_UPDATE.search(it["title"]): continue
            rec={
                "id": stable_id(it["url"]),
                "title":it["title"], "applyLink":it["url"], "detailLink":it["url"],
                "source":"aggregator","domicile":"All India","type":"UPDATE" if ALLOW_UPDATE.search(it["title"]) else "VACANCY",
                "qualificationLevel":"Any graduate",
                "flags":{"fromAggregator":host(base)}
            }
            p=posts_from_text(it["title"]); 
            if p: rec["numberOfPosts"]=p
            res.append(rec)
        time.sleep(0.2)
    return res

def dedup_and_rank(items):
    bykey={}
    for j in items:
        key=(j["title"].lower(), urlparse(j["applyLink"]).path.lower())
        if key not in bykey:
            bykey[key]=j; continue
        a=bykey[key]; b=j
        if a["source"]=="official" and b["source"]!="official": continue
        if b["source"]=="official" and a["source"]!="official": bykey[key]=b; continue
        sa=AGG_SCORES.get(host(a["detailLink"]), 0.6)
        sb=AGG_SCORES.get(host(b["detailLink"]), 0.6)
        # keep the one with higher aggregator score
        if sb>sa: bykey[key]=b
        # if both present, mark corroborated to boost later learning
        bykey[key].setdefault("flags",{})["corroborated"]=True
    return list(bykey.values())

if __name__=="__main__":
    out = collect()
    out = dedup_and_rank(out)
    for j in out:
        j.setdefault("domicile","All India")
    print("\n".join(json.dumps(j, ensure_ascii=False) for j in out))
