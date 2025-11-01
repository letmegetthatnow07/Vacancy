# qc_checks.py — validate final data.json with auto-deduplication
import json, sys, pathlib
from urllib.parse import urlparse
from datetime import datetime, date

def is_http_url(u):
  if not u: return False
  try:
    p=urlparse(u); return p.scheme in ("http","https") and bool(p.netloc)
  except: return False

def parse_date_any(s):
  if not s or s.strip().upper()=="N/A": return None
  s=s.strip()
  for f in ("%Y-%m-%d","%d/%m/%Y","%d-%m-%Y","%d %B %Y","%d %b %Y"):
    try: return datetime.strptime(s,f).date()
    except: pass
  return None

def norm_url(u):
    try:
        p = urlparse(u or "")
        base = p._replace(query="", fragment="")
        return base.geturl().rstrip("/").lower()
    except:
        return (u or "").rstrip("/").lower()

def main():
  p = pathlib.Path("data.json")
  if not p.exists(): print("qc: data.json missing"); sys.exit(2)
  try:
    data=json.loads(p.read_text(encoding="utf-8"))
  except Exception as e:
    print(f"qc: invalid JSON: {e}"); sys.exit(2)

  listings=data.get("jobListings", []); archived=data.get("archivedListings", []); tinfo=data.get("transparencyInfo", {})
  problems=[]; seen={}; today=date.today()

  if not isinstance(listings,list): problems.append("jobListings must be list")
  if not isinstance(archived,list): problems.append("archivedListings must be list")

  # Deduplicate active listings
  deduped = []
  for i, rec in enumerate(listings):
    rid=rec.get("id")
    url_key = norm_url(rec.get("applyLink"))
    
    if not rid: problems.append(f"[active #{i}] missing id"); continue
    
    if rid in seen:
      # Duplicate found - keep the one with more complete data
      existing = seen[rid]
      if len(rec.get("title", "")) > len(existing.get("title", "")):
        # Replace with better version
        deduped[seen[rid + "_idx"]] = rec
        problems.append(f"[active #{i}] duplicate id resolved: {rid}")
      else:
        problems.append(f"[active #{i}] duplicate id: {rid} (skipped)")
      continue
    
    seen[rid] = rec
    seen[rid + "_idx"] = len(deduped)
    deduped.append(rec)
    
    # Basic validation
    title=(rec.get("title") or "").strip()
    if len(title)<6: problems.append(f"[{rid}] short title")
    al, pl = rec.get("applyLink"), rec.get("pdfLink")
    if not (is_http_url(al) or is_http_url(pl)): problems.append(f"[{rid}] invalid URLs")
    dl=rec.get("deadline")
    if dl and dl.strip().upper()!="N/A":
      d=parse_date_any(dl)
      if not d: problems.append(f"[{rid}] invalid deadline format: {dl}")
    src=rec.get("source")
    if src not in ("official","aggregator"): problems.append(f"[{rid}] invalid source: {src}")
    typ=rec.get("type")
    if typ not in ("VACANCY","UPDATE"): problems.append(f"[{rid}] invalid type: {typ}")

  # If deduplication happened, write back
  if len(deduped) < len(listings):
    data["jobListings"] = deduped
    data["transparencyInfo"]["totalListings"] = len(deduped)
    p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"qc: Auto-deduped {len(listings) - len(deduped)} duplicates")

  for i, rec in enumerate(archived):
    rr=rec.get("flags",{}).get("removed_reason")
    if not rr: problems.append(f"[archived #{i}] missing removed_reason")

  if isinstance(tinfo.get("totalListings"),int) and tinfo["totalListings"]!=len(deduped):
    problems.append(f"transparencyInfo.totalListings mismatch (updating to {len(deduped)})")
    data["transparencyInfo"]["totalListings"] = len(deduped)

  # Print non-critical warnings but don't fail
  critical = [p for p in problems if "duplicate id:" in p or "missing id" in p or "invalid JSON" in p]
  
  if critical:
    print("qc: FAIL (critical errors)")
    [print(" -", m) for m in critical]
    sys.exit(1)
  
  if problems:
    print(f"qc: OK with warnings (active={len(deduped)}, archived={len(archived)})")
    [print(" ⚠", m) for m in problems[:5]]  # Show first 5 warnings
  else:
    print(f"qc: OK (active={len(deduped)}, archived={len(archived)})")
  
  sys.exit(0)

if __name__=="__main__": main()
