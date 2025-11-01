#!/usr/bin/env python3
# qc_and_learn.py v2025-11-01-final-removal-policy

import json, pathlib, re, argparse, urllib.parse
from datetime import datetime, timedelta, date

P = pathlib.Path

def JLOAD(p, d):
    try:
        if P(p).exists():
            return json.loads(P(p).read_text(encoding="utf-8"))
    except:
        pass
    return d

def JLOADL(p):
    out=[]
    if P(p).exists():
        for line in P(p).read_text(encoding="utf-8").splitlines():
            line=line.strip()
            if not line: continue
            try: out.append(json.loads(line))
            except: pass
    return out

def JWRITE(p, obj):
    P(p).write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

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

# CRITICAL FIX: Load user_state.json to preserve applied markings
user_state = JLOAD("user_state.json", {})

learn = JLOAD("learn_registry.json", {})
if not isinstance(learn, dict): learn={}
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
    try: return urllib.parse.urlparse(u or "").netloc.lower()
    except: return ""

def path_tokens(u):
    try:
        p=urllib.parse.urlparse(u or "")
        return [s for s in (p.path or "").lower().split("/") if s]
    except: return []

def title_tokens(t):
    return [x for x in re.split(r"[^a-z0-9]+",(t or "").lower()) if x]

def norm_url(u):
    try:
        p=urllib.parse.urlparse(u or "")
        base=p._replace(query="", fragment="")
        s=urllib.parse.urlunparse(base)
        return s.rstrip("/").lower()
    except: return (u or "").rstrip("/").lower()

def slugify(text):
    t=(text or "").lower()
    t=re.sub(r"[^a-z0-9]+","-",t).strip("-")
    return t[:80] if t else ""

def parse_date_any(s):
    if not s or s.strip().upper()=="N/A": return None
    s=s.strip()
    for f in ("%d/%m/%Y","%Y-%m-%d","%d-%m-%Y","%d %B %Y","%d %b %Y"):
        try: return datetime.strptime(s,f).date()
        except: pass
    return None

# NEW: Check eligibility - Filter only legitimate garbage
def check_eligibility(job):
    """
    Check if job is legitimate
    Remove: Checks 4 & 5 (missing data OK, you can add manually)
    Keep: Checks 1 & 3 (Hindi garbage, wrong domicile)
    Optional: Check 2 (very short titles are likely garbage)
    """
    title = job.get('title', '')
    domicile = (job.get('domicile', '') or 'N/A').upper()
    
    # Check 1: Title not corrupted (Hindi) - KEEP ✅
    non_ascii = sum(1 for c in title if ord(c) > 127) / max(len(title), 1)
    if non_ascii > 0.3:
        return False, "Hindi_title"
    
    # Check 2: Valid title (optional - removes obvious garbage) - OPTIONAL ⚠️
    # If a title is VERY short (<5 chars), it's likely corrupted
    if not title or len(title) < 5:
        return False, "Invalid_title"
    
    # Check 3: Domicile match - KEEP ✅
    if 'BIHAR' not in domicile and 'ALL' not in domicile:
        return False, f"Domicile_{domicile}"
    
    # REMOVED: Check 4 (qualification) - REMOVED ✅
    # REMOVED: Check 5 (deadline) - REMOVED ✅
    
    return True, "Eligible"

UPD_TOK = [
    "corrigendum","extension","extended","addendum","amendment","revised","rectified",
    "notice","last date","reopen","re-open","reopened"
]
DATE_PAT = re.compile(r"(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{2,4})")
def is_update_title(t): return any(k in (t or "").lower() for k in UPD_TOK)

def normalize_pdf_stem(u):
    try:
        p=urllib.parse.urlparse(u or "")
        fn=(p.path or "").rsplit("/",1)[-1].lower()
        fn=re.sub(r"(?i)(corrigendum|extension|extended|addendum|amendment|notice|revised|rectified|reopen|re-open|reopened)","",fn)
        return re.sub(r"[\W_]+","", fn)
    except: return ""

def url_root(u):
    try:
        p=urllib.parse.urlparse(u or "")
        root=p._replace(query="", fragment="")
        path=(root.path or "/").rsplit("/",1)[0]
        return f"{root.scheme}://{root.netloc}{path}"
    except: return u or ""

def adv_no(t):
    m=re.search(r"(advt|advertisement|notice)\s*(no\.?|number)?\s*[:\-]?\s*([A-Za-z0-9\/\-\._]+)", t or "", re.I)
    if m: return m.group(3).lower()
    return ""

POSTS_PAT = re.compile(r"(\d{1,6})\s*(posts?|vacanc(?:y|ies)|seats?)", re.I)
def parse_posts_from_text(txt):
    if not txt: return None
    m = POSTS_PAT.search(txt)
    if m:
        try: return int(m.group(1))
        except: return None
    return None

def learn_set_slug(slug, **kw):
    if not slug: return
    if not isinstance(learn.get("bySlug"), dict): learn["bySlug"]={}
    rec = learn["bySlug"].setdefault(slug, {})
    changed=False
    for k,v in kw.items():
        if v in (None,""): continue
        if rec.get(k)!=v:
            rec[k]=v; changed=True
    if changed:
        rec["updatedAt"]=datetime.utcnow().isoformat()+"Z"
        note({"slug_hint":slug, **kw})

def patterns_for_host(h):
    return (learn.get("patterns") or {}).get(h, [])

def mark_non_vacancy_pattern(h, title, url):
    if not h: return
    tt = list(dict.fromkeys(title_tokens(title)))[:8]
    pt = [x for x in path_tokens(url) if len(x)<=40][:6]
    pat = {"kind":"non_vacancy","titleTokens":tt,"pathTokens":pt,"addedAt":datetime.utcnow().isoformat()+"Z"}
    learn["patterns"]=learn.get("patterns") or {}
    arr = learn["patterns"].setdefault(h, [])
    def same(a,b): return a.get("kind")==b.get("kind") and a.get("titleTokens")==b.get("titleTokens") and a.get("pathTokens")==b.get("pathTokens")
    if not any(same(pat,p) for p in arr):
        arr.append(pat); note({"learned":"non_vacancy_pattern","host":h,"titleTokens":tt,"pathTokens":pt})

def matches_non_vacancy_pattern(h, title, url):
    arr = patterns_for_host(h)
    if not arr: return False
    tt = set(title_tokens(title)); pt = set(path_tokens(url))
    for p in arr:
        if p.get("kind")!="non_vacancy": continue
        need_tt = set(p.get("titleTokens",[])); need_pt = set(p.get("pathTokens",[]))
        if need_pt and not need_pt.issubset(pt): continue
        if not need_tt or len(tt.intersection(need_tt))>=max(1,len(need_tt)//2 or 1):
            return True
    return False

def stable_id(applyLink):
    return f"user_{abs(hash(norm_url(applyLink)))%10**9}"

# CRITICAL FIX: Deduplicate by URL first, then assign stable IDs
url_to_job = {}

for j in jobs:
    url_key = norm_url(j.get("applyLink"))
    
    if not url_key:
        continue
    
    # If we've seen this URL before, merge/keep the better one
    if url_key in url_to_job:
        existing = url_to_job[url_key]
        
        # Prefer official sources over aggregators
        if j.get("source") == "official" and existing.get("source") != "official":
            url_to_job[url_key] = j
        # Keep the one with more complete data
        elif len(j.get("title", "")) > len(existing.get("title", "")):
            url_to_job[url_key] = j
        
        # Merge flags from both
        existing_flags = existing.get("flags", {})
        new_flags = j.get("flags", {})
        url_to_job[url_key].setdefault("flags", {})
        url_to_job[url_key]["flags"].update(existing_flags)
        url_to_job[url_key]["flags"].update(new_flags)
    else:
        url_to_job[url_key] = j

# Now assign IDs to deduplicated jobs
jobs = []
for url_key, j in url_to_job.items():
    # Always use stable ID based on URL
    j["id"] = stable_id(j.get("applyLink"))
    jobs.append(j)

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
    best=None; score=0.0
    for p in parents:
        s=0.0
        if url_root(j.get("applyLink"))==url_root(p.get("applyLink")): s+=0.45
        if normalize_pdf_stem(j.get("applyLink")) and normalize_pdf_stem(j.get("applyLink"))==normalize_pdf_stem(p.get("applyLink")): s+=0.35
        if adv_no(j.get("title")) and adv_no(j.get("title"))==adv_no(p.get("title")): s+=0.25
        if s>score: score, best = s, p
    if best and score>=0.6:
        best.setdefault("updates", []).append({"title": j.get("title"), "link": j.get("applyLink"), "capturedAt": datetime.utcnow().isoformat()+"Z"})
        dates = [m.group(1) for m in DATE_PAT.finditer(j.get("title") or "")]
        parsed = [parse_date_any(x.replace("-","/")) for x in dates if x]; parsed = [d for d in parsed if d]
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
    j["type"]="UPDATE"; j.setdefault("flags",{})["no_parent_found"]=True

jobs = parents

seen_keys={norm_url(j.get("applyLink")) for j in jobs}
for s in subs:
    if s.get("type")!="missing": continue
    title=(s.get("title") or "").strip()
    url=norm_url((s.get("url") or "").strip())
    site=(s.get("officialSite") or "").strip()
    last=(s.get("lastDate") or s.get("deadline") or "").strip() or "N/A"
    posts=s.get("posts")
    if site and site not in rules["captureHints"]: rules["captureHints"].append(site)
    if not title or not url: continue
    if url in seen_keys: continue
    card={
        "id": stable_id(url),
        "title": title, "qualificationLevel": "Any graduate", "domicile": "All India",
        "deadline": last, "applyLink": url, "detailLink": url,
        "source": "official", "type": "VACANCY",
        "flags": {"added_from_missing": True, "trusted": True}
    }
    try:
        if isinstance(posts,str) and posts.strip().isdigit(): posts=int(posts.strip())
        if isinstance(posts,int) and posts>0: card["numberOfPosts"]=posts
    except: pass
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
    if r.get("type")!="report": continue
    jid=r.get("jobId") or ""
    report_map[jid]=r
    for k in ("lastDate","eligibility","evidenceUrl","posts"):
        v=r.get(k)
        if v: report_map[jid][k]=v
    if r.get("reasonCode"):
        report_map[jid]["reasonCode"]=r["reasonCode"]
    else:
        note({"warn":"report_missing_reasonCode","jobId":jid})

def keep_date(j):
    d=parse_date_any(j.get("deadline"))
    if d: return d
    ku=j.get("flags",{}).get("keep_until")
    if ku:
        try: return datetime.fromisoformat(ku).date()
        except: return None
    return None

primary=[]; other=[]; applied_ids=[]; today=date.today()
rejected_hindi=0; rejected_ineligible=0

# Build applied_ids from user_state.json
for jid, state_rec in user_state.items():
    if not state_rec or not isinstance(state_rec, dict): continue
    action = state_rec.get("action")
    
    if action == "applied":
        applied_ids.append(jid)
    elif action == "exam_done":
        ts = state_rec.get("ts")
        if ts:
            try:
                done_date = datetime.fromisoformat(ts.replace("Z","")).date()
                days_since_done = (today - done_date).days
                if days_since_done <= 7:
                    applied_ids.append(jid)
            except:
                applied_ids.append(jid)

for j in jobs:
    jid = j["id"]
    h=host(j.get("applyLink"))

    # Check eligibility BEFORE processing
    is_eligible, reason = check_eligibility(j)
    if not is_eligible:
        if "Hindi" in reason:
            rejected_hindi += 1
        else:
            rejected_ineligible += 1
        j.setdefault("flags",{})["removed_reason"] = f"auto_filtered_{reason}"
        j.setdefault("flags",{})["auto_filtered"] = reason
        archived.append(j)
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
            j["deadline"]=info["lastDate"]; learn_set_slug(slugify(j.get("title")), lastDate=j["deadline"])
        if "wrong_eligibility" in reasons and info.get("eligibility"):
            j["qualificationLevel"]=info["eligibility"]; learn_set_slug(slugify(j.get("title")), eligibility=j["qualificationLevel"])
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
            except: pass

    last=keep_date(j)
    if last is not None:
        j["daysLeft"]=(last - today).days
    if not j.get("numberOfPosts"):
        c=parse_posts_from_text(j.get("title")) or j.get("flags",{}).get("posts")
        if c:
            j["numberOfPosts"]=c

    # CRITICAL FIX 1: If job is applied, keep it visible (don't remove!)
    if jid in applied_ids:
        primary.append(j)
        continue

    # CRITICAL FIX 2: Check exam_done + 7 days (ONLY removal reason for non-applied!)
    if jid in user_state:
        state_rec = user_state.get(jid)
        if state_rec and state_rec.get("action") == "exam_done":
            ts = state_rec.get("ts")
            if ts:
                try:
                    done_date = datetime.fromisoformat(ts.replace("Z","")).date()
                    days_since_done = (today - done_date).days
                    if days_since_done > 7:
                        j.setdefault("flags",{})["removed_reason"]="auto_archived_exam_done_7d"
                        archived.append(j)
                        continue
                except:
                    pass

    # CRITICAL FIX 3: Don't remove for expired deadline! Keep all jobs!
    if last and last < today:
        other.append(j)
    else:
        primary.append(j)

def host_only(u):
    try: return urllib.parse.urlparse(u or "").netloc.lower()
    except: return ""
sources=set()
for h in (rules.get("captureHints") or []):
    try: sources.add(urllib.parse.urlparse(h).netloc.lower())
    except: pass
seen_hosts={}
for j in primary+other:
    seen_hosts.setdefault(host_only(j.get("applyLink")),0); seen_hosts[host_only(j.get("applyLink"))]+=1
sources_status=[{"host":h,"items":seen_hosts.get(h,0)} for h in sorted(sources)]

transp = raw.get("transparencyInfo") or {}
transp.update({
    "schemaVersion":"1.9",
    "runMode": RUN_MODE,
    "lastUpdated": datetime.utcnow().isoformat()+"Z",
    "mergedUpdates": merged_count,
    "totalListings": len(primary)+len(other),
    "sourcesByStatus": sources_status,
    "archivedCount": len(archived),
    "appliedCount": len(applied_ids),
    "rejectedHindi": rejected_hindi,
    "rejectedIneligible": rejected_ineligible,
    "learning": {
        "hosts": len(learn.get("byHost") or {}),
        "slugs": len(learn.get("bySlug") or {}),
        "patterns": { h: len(v) for h,v in (learn.get("patterns") or {}).items() }
    }
})

out = {
    "jobListings": primary+other,
    "archivedListings": archived,
    "sections": {
        "applied": applied_ids,
        "other": [j["id"] for j in other],
        "primary": [j["id"] for j in primary if j["id"] not in applied_ids]
    },
    "transparencyInfo": transp
}

JWRITE("data.json", out)
JWRITE("rules.json", rules)
JWRITE("learn_registry.json", learn)
JWRITE("learn.json", {"generatedAt": datetime.utcnow().isoformat()+"Z","runMode": RUN_MODE})
JWRITE("health.json", {"ok": True, **transp})
print(f"✓ QC and Learn complete: {len(primary)+len(other)} active ({len(applied_ids)} applied), {len(archived)} archived (hindi:{rejected_hindi}, ineligible:{rejected_ineligible}), mode={RUN_MODE}")
