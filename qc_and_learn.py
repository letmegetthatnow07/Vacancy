#!/usr/bin/env python3
# qc_and_learn.py v2025-11-03-FIXED (Applied Jobs Inclusion)
# FIXES: C-003, H-001, H-002 + applied job preservation

import json, pathlib, re, argparse, urllib.parse, os, sys
from datetime import datetime, timedelta, date, timezone
from tools.eligibility import is_eligible  # FIX: Import comprehensive eligibility

P = pathlib.Path

def JLOAD(p, d):
    try:
        if P(p).exists():
            return json.loads(P(p).read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        print(f"[WARN] {p} corrupted: {e}, using default", file=sys.stderr)
    except Exception as e:
        print(f"[WARN] {p} error: {e}, using default", file=sys.stderr)
    return d

def JLOADL(p):
    out=[]
    if P(p).exists():
        try:
            for line in P(p).read_text(encoding="utf-8").splitlines():
                line=line.strip()
                if not line: 
                    continue
                try: 
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        except Exception:
            pass
    return out

def JWRITE(p, obj):
    """Atomic write with temp file"""
    try:
        temp_path = str(p) + ".tmp"
        P(temp_path).write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(temp_path, str(p))
    except Exception as e:
        print(f"[ERROR] Writing {p}: {e}", file=sys.stderr)

ap = argparse.ArgumentParser()
ap.add_argument("--mode", default="nightly")
RUN_MODE = (ap.parse_args().mode or "nightly").lower()

raw = JLOAD("data.json", {"jobListings":[], "archivedListings":[], "transparencyInfo":{}})
jobs = list(raw.get("jobListings") or [])
archived = list(raw.get("archivedListings") or [])
votes = JLOADL("votes.jsonl")
reports = JLOADL("reports.jsonl")
subs = JLOADL("submissions.jsonl")
rules = JLOAD("rules.json", {"captureHints":[], "aggregatorScores":{}})

# FIX A-004: Better user_state loading with fallback
user_state = JLOAD("user_state.json", {})
if not isinstance(user_state, dict):
    user_state = {}

learn = JLOAD("learn_registry.json", {})
if not isinstance(learn, dict): 
    learn={}
learn.setdefault("byHost", {})
learn.setdefault("bySlug", {})
learn.setdefault("patterns", {})
learn.setdefault("notes", [])

def note(ev):
    try:
        learn["notes"] = ([{**ev, "at": datetime.utcnow().isoformat()+"Z"}] + (learn.get("notes") or []))[:50]
    except:
        pass

def host(u):
    try: 
        return urllib.parse.urlparse(u or "").netloc.lower()
    except: 
        return ""

def path_tokens(u):
    try:
        p=urllib.parse.urlparse(u or "")
        return [s for s in (p.path or "").lower().split("/") if s]
    except: 
        return []

def title_tokens(t):
    return [x for x in re.split(r"[^a-z0-9]+",(t or "").lower()) if x]

def norm_url(u):
    try:
        p=urllib.parse.urlparse(u or "")
        base=p._replace(query="", fragment="")
        s=urllib.parse.urlunparse(base)
        return s.rstrip("/").lower()
    except: 
        return (u or "").rstrip("/").lower()

def slugify(text):
    t=(text or "").lower()
    t=re.sub(r"[^a-z0-9]+","-",t).strip("-")
    return t[:80] if t else ""

def parse_date_any(s):
    if not s or s.strip().upper()=="N/A": 
        return None
    s=s.strip()
    for f in ("%d/%m/%Y","%Y-%m-%d","%d-%m-%Y","%d %B %Y","%d %b %Y"):
        try: 
            return datetime.strptime(s,f).date()
        except: 
            pass
    return None

def stable_id(applyLink):
    """Generate DETERMINISTIC stable ID using SHA1"""
    import hashlib
    try:
        norm = norm_url(applyLink)
        return f"job_{hashlib.sha1(norm.encode()).hexdigest()[:12]}"
    except:
        return f"job_{hashlib.md5((applyLink or '').lower().encode()).hexdigest()[:12]}"

def check_eligibility(job):
    """
    FIX C-003: Use comprehensive eligibility module
    Delegates to tools/eligibility.py for sophisticated filtering
    """
    title = job.get('title', '')
    
    # Use the merged eligibility function
    is_eligible_result, reason = is_eligible(title)
    
    if not is_eligible_result:
        return False, reason
    
    return True, "Eligible"

UPD_TOK = [
    "corrigendum","extension","extended","addendum","amendment","revised","rectified",
    "notice","last date","reopen","re-open","reopened"
]
DATE_PAT = re.compile(r"(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{2,4})")

def is_update_title(t): 
    return any(k in (t or "").lower() for k in UPD_TOK)

def normalize_pdf_stem(u):
    try:
        p=urllib.parse.urlparse(u or "")
        fn=(p.path or "").rsplit("/",1)[-1].lower()
        fn=re.sub(r"(?i)(corrigendum|extension|extended|addendum|amendment|notice|revised|rectified|reopen|re-open|reopened)","",fn)
        return re.sub(r"[\W_]+","", fn)
    except: 
        return ""

def url_root(u):
    try:
        p=urllib.parse.urlparse(u or "")
        root=p._replace(query="", fragment="")
        path=(root.path or "/").rsplit("/",1)[0]
        return f"{root.scheme}://{root.netloc}{path}"
    except: 
        return u or ""

def adv_no(t):
    m=re.search(r"(advt|advertisement|notice)\s*(no\.?|number)?\s*[:\-]?\s*([A-Za-z0-9\/\-\._]+)", t or "", re.I)
    if m: 
        return m.group(3).lower()
    return ""

POSTS_PAT = re.compile(r"(\d{1,6})\s*(posts?|vacanc(?:y|ies)|seats?)", re.I)

def parse_posts_from_text(txt):
    if not txt: 
        return None
    m = POSTS_PAT.search(txt)
    if m:
        try: 
            return int(m.group(1))
        except: 
            return None
    return None

def learn_set_slug(slug, **kw):
    if not slug: 
        return
    if not isinstance(learn.get("bySlug"), dict): 
        learn["bySlug"]={}
    rec = learn["bySlug"].setdefault(slug, {})
    changed=False
    for k,v in kw.items():
        if v in (None,""): 
            continue
        if rec.get(k)!=v:
            rec[k]=v; 
            changed=True
    if changed:
        rec["updatedAt"]=datetime.utcnow().isoformat()+"Z"
        note({"slug_hint":slug, **kw})

def patterns_for_host(h):
    return (learn.get("patterns") or {}).get(h, [])

def mark_non_vacancy_pattern(h, title, url):
    if not h: 
        return
    tt = list(dict.fromkeys(title_tokens(title)))[:8]
    pt = [x for x in path_tokens(url) if len(x)<=40][:6]
    pat = {"kind":"non_vacancy","titleTokens":tt,"pathTokens":pt,"addedAt":datetime.utcnow().isoformat()+"Z"}
    learn["patterns"]=learn.get("patterns") or {}
    arr = learn["patterns"].setdefault(h, [])
    def same(a,b): 
        return a.get("kind")==b.get("kind") and a.get("titleTokens")==b.get("titleTokens") and a.get("pathTokens")==b.get("pathTokens")
    if not any(same(pat,p) for p in arr):
        arr.append(pat); 
        note({"learned":"non_vacancy_pattern","host":h,"titleTokens":tt,"pathTokens":pt})

def matches_non_vacancy_pattern(h, title, url):
    arr = patterns_for_host(h)
    if not arr: 
        return False
    tt = set(title_tokens(title)); 
    pt = set(path_tokens(url))
    for p in arr:
        if p.get("kind")!="non_vacancy": 
            continue
        need_tt = set(p.get("titleTokens",[])); 
        need_pt = set(p.get("pathTokens",[]))
        if need_pt and not need_pt.issubset(pt): 
            continue
        if not need_tt or len(tt.intersection(need_tt))>=max(1,len(need_tt)//2 or 1):
            return True
    return False

# FIX H-001: Extract applied_ids BEFORE processing jobs
applied_ids = []
other_marked_ids = []
today = date.today()

for jid, state_rec in user_state.items():
    if not state_rec or not isinstance(state_rec, dict): 
        continue
    action = state_rec.get("action")
    
    if action == "applied":
        applied_ids.append(jid)
    elif action == "other" or action == "not_interested":
        other_marked_ids.append(jid)
    elif action == "exam_done":
        ts = state_rec.get("ts")
        if ts:
            try:
                # FIX H-004: Better timestamp parsing with error handling
                done_date = datetime.fromisoformat(ts.replace("Z","")).date()
                days_since_done = (today - done_date).days
                
                # Keep in applied if <=7 days from exam_done
                if days_since_done <= 7:
                    applied_ids.append(jid)
                # Mark for removal if >7 days
                else:
                    user_state[jid]["should_archive"] = True
            except ValueError as e:
                print(f"[WARN] Bad timestamp for {jid}: {ts}", file=sys.stderr)
                # If timestamp parsing fails, keep job (don't lose it)
                applied_ids.append(jid)
        else:
            applied_ids.append(jid)

applied_ids = list(set(applied_ids))  # Deduplicate
other_marked_ids = list(set(other_marked_ids))

print(f"[QC] Loaded {len(applied_ids)} applied IDs, {len(other_marked_ids)} other marked", file=sys.stderr)

# FIX A-003: Dedup jobs by URL first, assign stable IDs
url_to_job = {}

for j in jobs:
    url_key = norm_url(j.get("applyLink"))
    
    if not url_key:
        continue
    
    # Always use stable ID based on URL (deterministic)
    j["id"] = stable_id(j.get("applyLink"))
    
    if url_key not in url_to_job:
        url_to_job[url_key] = j
    else:
        existing = url_to_job[url_key]
        
        # Prefer official sources over aggregators
        if j.get("source") == "official" and existing.get("source") != "official":
            url_to_job[url_key] = j
        # Keep the one with more complete data
        elif len((j.get("title") or "")) > len((existing.get("title") or "")):
            url_to_job[url_key] = j
        
        # Merge flags from both
        existing_flags = existing.get("flags", {})
        new_flags = j.get("flags", {})
        url_to_job[url_key].setdefault("flags", {})
        url_to_job[url_key]["flags"].update(existing_flags)
        url_to_job[url_key]["flags"].update(new_flags)

jobs = list(url_to_job.values())

parents = []
updates = []
for j in jobs:
    if is_update_title(j.get("title")):
        updates.append(j)
    else:
        parents.append(j)

kept = []
merged_count = 0
for j in updates:
    best=None; 
    score=0.0
    for p in parents:
        s=0.0
        if url_root(j.get("applyLink"))==url_root(p.get("applyLink")): 
            s+=0.45
        if normalize_pdf_stem(j.get("applyLink")) and normalize_pdf_stem(j.get("applyLink"))==normalize_pdf_stem(p.get("applyLink")): 
            s+=0.35
        if adv_no(j.get("title")) and adv_no(j.get("title"))==adv_no(p.get("title")): 
            s+=0.25
        if s>score: 
            score, best = s, p
    if best and score>=0.6:
        best.setdefault("updates", []).append({"title": j.get("title"), "link": j.get("applyLink"), "capturedAt": datetime.utcnow().isoformat()+"Z"})
        dates = [m.group(1) for m in DATE_PAT.finditer(j.get("title") or "")]
        parsed = [parse_date_any(x.replace("-","/")) for x in dates if x]; 
        parsed = [d for d in parsed if d]
        if parsed:
            new_deadline = max(parsed)
            cur = parse_date_any(best.get("deadline"))
            if not cur or new_deadline > cur:
                best["deadline"] = new_deadline.strftime("%d/%m/%Y")
                learn_set_slug(slugify(best.get("title")), lastDate=best["deadline"])
        pcount = parse_posts_from_text(j.get("title"))
        if pcount and not best.get("numberOfPosts"):
            best["numberOfPosts"] = pcount
            learn_set_slug(slugify(best.get("title")), posts=pcount)
        merged_count+=1
    j["type"]="UPDATE"; 
    j.setdefault("flags",{})["no_parent_found"]=True

jobs = parents

seen_keys={norm_url(j.get("applyLink")) for j in jobs}
for s in subs:
    if s.get("type")!="missing": 
        continue
    title=(s.get("title") or "").strip()
    url=norm_url((s.get("url") or "").strip())
    site=(s.get("officialSite") or "").strip()
    last=(s.get("lastDate") or s.get("deadline") or "").strip() or "N/A"
    posts=s.get("posts")
    if site and site not in rules["captureHints"]: 
        rules["captureHints"].append(site)
    if not title or not url: 
        continue
    if url in seen_keys: 
        continue
    card={
        "id": stable_id(url),
        "title": title, 
        "qualificationLevel": "Any graduate", 
        "domicile": "All India",
        "deadline": last, 
        "applyLink": url, 
        "detailLink": url,
        "source": "official", 
        "type": "VACANCY",
        "flags": {"added_from_missing": True, "trusted": True}
    }
    try:
        if isinstance(posts,str) and posts.strip().isdigit(): 
            posts=int(posts.strip())
        if isinstance(posts,int) and posts>0: 
            card["numberOfPosts"]=posts
    except: 
        pass
    jobs.append(card)

def find_report(j):
    jid = j.get("id")
    rid_report = report_map.get(jid)
    if not rid_report:
        for rid, rep in report_map.items():
            if rep.get("url") and norm_url(j.get("applyLink")) == norm_url(rep.get("url")):
                return rep
    return rid_report

report_map = {}
for r in reports:
    if r.get("type")!="report": 
        continue
    jid=r.get("jobId") or ""
    report_map[jid]=r
    for k in ("lastDate","eligibility","evidenceUrl","posts"):
        v=r.get(k)
        if v: 
            report_map[jid][k]=v
    if r.get("reasonCode"):
        report_map[jid]["reasonCode"]=r["reasonCode"]
    else:
        print(f"[WARN] report_missing_reasonCode for {jid}", file=sys.stderr)

def keep_date(j):
    d=parse_date_any(j.get("deadline"))
    if d: 
        return d
    ku=j.get("flags",{}).get("keep_until")
    if ku:
        try: 
            return datetime.fromisoformat(ku).date()
        except: 
            return None
    return None

primary=[]
other=[]
rejected_hindi=0
rejected_teacher=0
rejected_tech=0
rejected_pg=0
rejected_skills=0
rejected_domicile=0
archived_exam_done=0

# FIX H-001 + H-002: Process jobs with applied_ids ALREADY extracted
for j in jobs:
    jid = j.get("id")
    h = host(j.get("applyLink"))

    # FIX H-001 + H-002: Check applied status FIRST (protection before filtering)
    if jid in applied_ids:
        # NEVER filter applied jobs (even if expired!)
        # But check if should be archived (exam_done >7 days)
        if jid in user_state:
            state_rec = user_state.get(jid)
            if state_rec and state_rec.get("action") == "exam_done":
                # Check if marked for archival
                if state_rec.get("should_archive"):
                    j.setdefault("flags",{})["removed_reason"]="auto_archived_exam_done_7d"
                    j.setdefault("flags",{})["archived_reason"]="exam_done_7d_expired"
                    archived.append(j)
                    archived_exam_done += 1
                    print(f"[QC] Archiving applied job (exam_done >7d): {jid[:16]}", file=sys.stderr)
                    continue
        
        # Keep applied job in primary (NEVER filter!)
        primary.append(j)
        print(f"[QC] Keeping applied job: {jid[:16]} - {j.get('title', '')[:50]}", file=sys.stderr)
        continue

    # ===== For non-applied jobs: apply eligibility checks =====
    
    # FIX C-003: Check eligibility using comprehensive module
    is_eligible_result, reason = check_eligibility(j)
    if not is_eligible_result:
        # Track rejection reasons
        if "Hindi" in reason:
            rejected_hindi += 1
        elif "Teacher" in reason:
            rejected_teacher += 1
        elif "Tech" in reason:
            rejected_tech += 1
        elif "Postgraduate" in reason:
            rejected_pg += 1
        elif "Specialty" in reason:
            rejected_skills += 1
        elif "Domicile" in reason:
            rejected_domicile += 1
        else:
            pass  # Other reasons
        
        j.setdefault("flags",{})["removed_reason"] = f"auto_filtered_{reason}"
        j.setdefault("flags",{})["auto_filtered"] = reason
        archived.append(j)
        print(f"[QC] Filtering: {j.get('title', '')[:50]} ({reason})", file=sys.stderr)
        continue

    if matches_non_vacancy_pattern(h, j.get("title",""), j.get("applyLink","")):
        if not (j.get("numberOfPosts") and parse_date_any(j.get("deadline"))):
            j.setdefault("flags",{})["removed_reason"]="auto_filtered_learn_non_vacancy"
            j.setdefault("flags",{})["auto_filtered"]="learn_non_vacancy"
            archived.append(j)
            continue

    info = find_report(j)
    if info:
        reasons = set([info.get("reasonCode") or info.get("reasons", "")]) if info.get("reasonCode") else set()
        if "wrong_last_date" in reasons and info.get("lastDate"):
            j["deadline"]=info["lastDate"]; 
            learn_set_slug(slugify(j.get("title")), lastDate=j["deadline"])
        if "wrong_eligibility" in reasons and info.get("eligibility"):
            j["qualificationLevel"]=info["eligibility"]; 
            learn_set_slug(slugify(j.get("title")), eligibility=j["qualificationLevel"])
        if "bad_link" in reasons and info.get("evidenceUrl"):
            j["applyLink"]=info["evidenceUrl"]
            j["detailLink"]=info["evidenceUrl"]
            j.setdefault("flags",{})["fixed_link"]=True
            learn_set_slug(slugify(j.get("title")), fixedLink=j["applyLink"])
        if "duplicate" in reasons or "not_vacancy" in reasons or "last_date_over" in reasons:
            j.setdefault("flags",{})["removed_reason"]="reported_"+("_".join(sorted(reasons)))
            if "not_vacancy" in reasons:
                mark_non_vacancy_pattern(h, j.get("title",""), j.get("applyLink",""))
            archived.append(j)
            continue
        if info.get("posts") and not j.get("numberOfPosts"):
            try:
                p=int(info["posts"])
                if p>0:
                    j["numberOfPosts"]=p
                    learn_set_slug(slugify(j.get("title")), posts=p)
            except: 
                pass

    last=keep_date(j)
    if last is not None:
        j["daysLeft"]=(last - today).days
    if not j.get("numberOfPosts"):
        c=parse_posts_from_text(j.get("title")) or j.get("flags",{}).get("posts")
        if c:
            j["numberOfPosts"]=c

    # KEEP all non-archived jobs (don't remove for expired deadline!)
    if last and last < today:
        other.append(j)
    else:
        primary.append(j)

def host_only(u):
    try: 
        return urllib.parse.urlparse(u or "").netloc.lower()
    except: 
        return ""

sources=set()
for h in (rules.get("captureHints") or []):
    try: 
        sources.add(urllib.parse.urlparse(h).netloc.lower())
    except: 
        pass
seen_hosts={}
for j in primary+other:
    seen_hosts.setdefault(host_only(j.get("applyLink")),0)
    seen_hosts[host_only(j.get("applyLink"))]+=1
sources_status=[{"host":h,"items":seen_hosts.get(h,0)} for h in sorted(sources)]

transp = raw.get("transparencyInfo") or {}
transp.update({
    "schemaVersion":"1.10",
    "runMode": RUN_MODE,
    "lastUpdated": datetime.utcnow().isoformat()+"Z",
    "mergedUpdates": merged_count,
    "totalListings": len(primary)+len(other),
    "sourcesByStatus": sources_status,
    "archivedCount": len(archived),
    "appliedCount": len(applied_ids),
    "rejectedHindi": rejected_hindi,
    "rejectedTeacher": rejected_teacher,
    "rejectedTech": rejected_tech,
    "rejectedPostgraduate": rejected_pg,
    "rejectedSpecialSkills": rejected_skills,
    "rejectedDomicile": rejected_domicile,
    "archivedExamDone": archived_exam_done,
    "learning": {
        "hosts": len(learn.get("byHost") or {}),
        "slugs": len(learn.get("bySlug") or {}),
        "patterns": { h: len(v) for h,v in (learn.get("patterns") or {}).items() }
    }
})

# ===== CRITICAL FIX: Include applied jobs IN jobListings + IDs in sections =====
out = {
    "jobListings": primary+other,  # ✅ ALL jobs (applied + primary + other)
    "archivedListings": archived,
    "sections": {
        "applied": applied_ids,  # ✅ IDs for quick lookup
        "other": other_marked_ids,
        "primary": [j.get("id") for j in primary if j.get("id") not in applied_ids]
    },
    "transparencyInfo": transp
}

JWRITE("data.json", out)
JWRITE("rules.json", rules)
JWRITE("learn_registry.json", learn)
JWRITE("learn.json", {"generatedAt": datetime.utcnow().isoformat()+"Z","runMode": RUN_MODE})
JWRITE("health.json", {"ok": True, **transp})

total_rejected = rejected_hindi + rejected_teacher + rejected_tech + rejected_pg + rejected_skills + rejected_domicile

print(f"✓ QC complete: {len(primary)+len(other)} active ({len(applied_ids)} applied), {len(archived)} archived", file=sys.stderr)
print(f"  Rejected: hindi={rejected_hindi}, teacher={rejected_teacher}, tech={rejected_tech}, pg={rejected_pg}, skills={rejected_skills}, domicile={rejected_domicile}", file=sys.stderr)
print(f"  Total rejected: {total_rejected}, mode={RUN_MODE}", file=sys.stderr)
