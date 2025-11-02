#!/usr/bin/env python3
# schema_merge.py — promote numberOfPosts reliably and normalize to int
# PLUS: Preserve applied/other jobs, filter eligibility
# FIXES: C-002, C-005, H-003, H-006, A-002 (ID consistency), A-003 (dedup)

import json, sys, re, hashlib, os
from datetime import datetime, timedelta, timezone

def norm_spaces(s): 
    return re.sub(r"\s+"," ", (s or "").strip())

def fuzzy_title(s):
    s = (s or "").lower()
    s = re.sub(r"[\(\)\[\]\{\}]", " ", s)
    s = re.sub(r"[^\w\s/:-]", " ", s)
    s = re.sub(r"\b(notice|notification|advertisement|advt|recruitment|online\s*form|apply\s*online)\b", " ", s)
    s = re.sub(r"\b(corrigendum|extension|extended|addendum|amendment|revised|rectified)\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_date(s):
    s = (s or "").strip()
    for fmt in ("%d/%m/%Y","%d-%m-%Y","%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%d/%m/%Y")
        except Exception:
            pass
    return s if s else "N/A"

def make_key(item):
    title = fuzzy_title(item.get("title",""))
    link  = (item.get("detailLink") or item.get("applyLink") or "").lower()
    link  = re.sub(r"[?#].*$", "", link)
    date  = norm_date(item.get("deadline","")).lower()
    raw = f"{title}|{link}|{date}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]

def norm_url(u):
    """Normalize URL for deduplication"""
    try:
        p = __import__('urllib.parse').parse.urlparse(u or "")
        base = p._replace(query="", fragment="")
        s = __import__('urllib.parse').parse.urlunparse(base)
        return s.rstrip("/").lower()
    except: 
        return (u or "").rstrip("/").lower()

def stable_id(url):
    """
    FIX A-002: Generate DETERMINISTIC stable ID using SHA1
    Matches collector.py and qc_and_learn.py
    """
    try:
        norm = norm_url(url)
        return f"job_{hashlib.sha1(norm.encode()).hexdigest()[:12]}"
    except:
        return f"job_{hashlib.md5((url or '').lower().encode()).hexdigest()[:12]}"

def compute_days_left(deadline_ddmmyyyy):
    """
    FIX H-003: Timezone-aware comparison
    Always compare in IST (UTC+5:30) since jobs are India-centric
    """
    try:
        d = datetime.strptime(deadline_ddmmyyyy, "%d/%m/%Y")
        
        # Get today in IST (UTC+5:30)
        ist_tz = timezone(timedelta(hours=5, minutes=30))
        today_ist = datetime.now(ist_tz).date()
        
        days_diff = (d.date() - today_ist).days
        return max(days_diff, 0)  # Never negative
    except Exception:
        return None

POSTS_PAT = re.compile(r"(\d{1,6})\s*(posts?|vacanc(?:y|ies)|seats?)", re.I)

def posts_from_text(txt):
    if not txt: 
        return None
    m = POSTS_PAT.search(txt)
    if m:
        try: 
            return int(m.group(1))
        except: 
            return None
    return None

def to_int(n):
    if n is None: 
        return None
    if isinstance(n,int): 
        return n
    if isinstance(n,str) and n.strip().isdigit(): 
        return int(n.strip())
    return None

def check_eligibility(job):
    """
    FIX C-001: ONLY 3 checks for removal:
    1. Hindi title (non-ASCII >30%)
    2. Invalid/corrupted title (too short)
    3. Wrong domicile (NOT Bihar or All India)
    
    DO NOT REMOVE FOR: Missing qualification, missing deadline, missing posts
    """
    title = job.get('title', '')
    domicile = (job.get('domicile', '') or 'N/A').upper()
    
    # Check 1: Title must be English (not corrupted Hindi)
    non_ascii = sum(1 for c in title if ord(c) > 127) / max(len(title), 1)
    if non_ascii > 0.3:
        return False, "Hindi_title"
    
    # Check 2: Title must be valid (optional but catches obvious garbage)
    if not title or len(title) < 5:
        return False, "Invalid_title"
    
    # Check 3: Domicile must be Bihar or All India
    if 'BIHAR' not in domicile and 'ALL' not in domicile:
        return False, f"Domicile_{domicile}"
    
    # ✅ DO NOT CHECK: qualification, deadline, numberOfPosts
    # These can be filled manually or by OCR later
    
    return True, "Eligible"

def validate(i):
    out = {
        "id": stable_id(i.get("applyLink") or i.get("detailLink")),
        "title": norm_spaces(i.get("title")),
        "qualificationLevel": norm_spaces(i.get("qualificationLevel") or ""),
        "domicile": norm_spaces(i.get("domicile") or ""),
        "deadline": norm_date(i.get("deadline") or ""),
        "applyLink": (i.get("applyLink") or "").strip(),
        "detailLink": (i.get("detailLink") or "").strip(),
        "source": i.get("source") or "official",
        "type": i.get("type") or "VACANCY",
        "flags": i.get("flags") or {},
    }
    p = to_int(i.get("numberOfPosts")) or posts_from_text(out["title"])
    if p: 
        out["numberOfPosts"]=p
    dl = compute_days_left(out["deadline"])
    if dl is not None: 
        out["daysLeft"] = dl
    return out

def merge(existing, candidates, applied_ids, other_ids):
    """
    FIX C-002, H-006, A-003: Merge candidates while preserving applied jobs
    
    Priority:
    1. PROTECT applied jobs from ANY filtering
    2. Keep other jobs (user marked)
    3. Enrich with new candidate data (dedup)
    """
    
    # FIX A-003: Dedup candidates first before adding to existing
    candidate_by_url = {}
    for cand in candidates:
        cand_url = norm_url(cand.get("applyLink") or cand.get("detailLink"))
        if cand_url not in candidate_by_url:
            candidate_by_url[cand_url] = cand
        else:
            # Keep candidate with more data
            existing_cand = candidate_by_url[cand_url]
            if len((cand.get("title") or "")) > len((existing_cand.get("title") or "")):
                candidate_by_url[cand_url] = cand
    
    candidates = list(candidate_by_url.values())
    
    idx = { make_key(x): x for x in existing }
    
    # FIX C-002: Preserve applied and other jobs BEFORE eligibility filtering
    preserved_applied = [j for j in existing if j.get('id') in applied_ids]
    preserved_other = [j for j in existing if j.get('id') in other_ids]
    
    print(f"✓ Preserving {len(preserved_applied)} applied jobs", file=sys.stderr)
    print(f"✓ Preserving {len(preserved_other)} other jobs", file=sys.stderr)
    
    added = 0
    rejected_hindi = 0
    rejected_ineligible = 0
    merged = 0
    
    # First pass: Check eligibility, then merge
    for raw in candidates:
        v = validate(raw)
        k = make_key(v)
        
        # FIX C-001: Check eligibility BEFORE adding (only 3 reasons)
        is_eligible, reason = check_eligibility(v)
        if not is_eligible:
            if "Hindi" in reason:
                rejected_hindi += 1
            else:
                rejected_ineligible += 1
            continue
        
        if k in idx:
            # Merge into existing (enrich with new data)
            ex = idx[k]
            for f in ["qualificationLevel","domicile","deadline","applyLink","detailLink","source","type"]:
                if v.get(f) and (not ex.get(f) or ex.get(f)=="N/A"):
                    ex[f] = v[f]
            if v.get("numberOfPosts") and not ex.get("numberOfPosts"):
                ex["numberOfPosts"]=v["numberOfPosts"]
            ex["flags"] = { **(ex.get("flags") or {}), **(v.get("flags") or {}) }
            if v.get("daysLeft") is not None: 
                ex["daysLeft"] = v["daysLeft"]
            merged += 1
        else:
            # Add new job
            existing.append(v)
            idx[k]=v
            added += 1
    
    # FIX H-006: Merge preserved jobs back (don't overwrite!)
    # Keep enriched versions but restore applied/other flags
    for job in preserved_applied:
        k = make_key(job)
        idx_job = idx.get(k)
        if idx_job and idx_job != job:
            # Merge: keep enriched data from idx_job, but restore original applied status
            idx_job.setdefault("flags", {})["applied_preserved"] = True
        elif not idx_job:
            existing.append(job)
            idx[k] = job
    
    for job in preserved_other:
        k = make_key(job)
        idx_job = idx.get(k)
        if idx_job and idx_job != job:
            idx_job.setdefault("flags", {})["other_preserved"] = True
        elif not idx_job:
            existing.append(job)
            idx[k] = job
    
    # Sort by deadline
    def sort_key(it):
        dd = it.get("deadline","N/A")
        try:
            dt = datetime.strptime(dd,"%d/%m/%Y")
            return (0, dt, it.get("title",""))
        except Exception:
            return (1, datetime.max, it.get("title",""))
    existing.sort(key=sort_key)
    
    print(f"\n✓ Added {added} new eligible jobs", file=sys.stderr)
    print(f"✓ Merged {merged} enrichments", file=sys.stderr)
    print(f"⊘ Rejected {rejected_hindi} Hindi titles", file=sys.stderr)
    print(f"⊘ Rejected {rejected_ineligible} ineligible jobs", file=sys.stderr)
    
    return existing, added

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python tools/schema_merge.py data.json tmp/candidates.jsonl data.json")
        sys.exit(2)
    
    data_path, cand_path, out_path = sys.argv[1], sys.argv[2], sys.argv[3]
    
    # Load existing data
    try:
        data = json.load(open(data_path,"r",encoding="utf-8"))
    except FileNotFoundError:
        print(f"[WARN] {data_path} not found, starting fresh", file=sys.stderr)
        data = {}
    except json.JSONDecodeError as e:
        print(f"[ERROR] {data_path} corrupted: {e}, starting fresh", file=sys.stderr)
        data = {}
    
    existing = data.get("jobListings") or []
    sections = data.get("sections") or {"applied":[],"other":[]}
    
    # FIX C-005: Check file existence properly
    cands = []
    if os.path.exists(cand_path) and os.path.isfile(cand_path):
        try:
            with open(cand_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        cands.append(json.loads(line))
                    except json.JSONDecodeError:
                        print(f"[WARN] Invalid JSON in {cand_path}: {line[:50]}", file=sys.stderr)
                        continue
        except Exception as e:
            print(f"[ERROR] Reading {cand_path}: {e}", file=sys.stderr)
    else:
        print(f"[WARN] {cand_path} not found or not readable", file=sys.stderr)
    
    # Get applied/other IDs from data structure
    applied_ids = set(data.get("sections", {}).get("applied", []))
    other_ids = set(data.get("sections", {}).get("other", []))
    
    merged, added = merge(existing, cands, applied_ids, other_ids)
    
    data["jobListings"] = merged
    data.setdefault("archivedListings", data.get("archivedListings") or [])
    data.setdefault("sections", data.get("sections") or {"applied":[],"other":[],"primary":[]})
    data.setdefault("transparencyInfo", {})
    data["transparencyInfo"]["totalListings"] = len(merged)
    data["transparencyInfo"]["appliedPreserved"] = len(applied_ids)
    data["transparencyInfo"]["otherPreserved"] = len(other_ids)
    
    # FIX A-001: Atomic write (write to temp file, then rename)
    temp_path = out_path + ".tmp"
    try:
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(temp_path, out_path)
        print(f"✓ Written {out_path} ({len(merged)} jobs)", file=sys.stderr)
    except Exception as e:
        print(f"[ERROR] Writing {out_path}: {e}", file=sys.stderr)
        if os.path.exists(temp_path):
            os.remove(temp_path)
        sys.exit(1)
    
    print(json.dumps({"added":added, "merged": len(cands) - added}))
