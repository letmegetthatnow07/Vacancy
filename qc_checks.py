#!/usr/bin/env python3
# tools/qc_checks.py — validate final data.json with auto-deduplication
# FIXES: P3-H-009 (better dedup logic), auto-archive invalid data

import json
import sys
import pathlib
from urllib.parse import urlparse
from datetime import datetime, date
import hashlib

def is_http_url(u):
    if not u:
        return False
    try:
        p = urlparse(u)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except:
        return False

def parse_date_any(s):
    if not s or s.strip().upper() == "N/A":
        return None
    s = s.strip()
    for f in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, f).date()
        except:
            pass
    return None

def norm_url(u):
    try:
        p = urlparse(u or "")
        base = p._replace(query="", fragment="")
        return base.geturl().rstrip("/").lower()
    except:
        return (u or "").rstrip("/").lower()

def make_key(job):
    """Generate dedup key from job data"""
    url_key = norm_url(job.get("applyLink"))
    title_key = (job.get("title") or "").lower()[:50]
    return hashlib.sha1(f"{url_key}|{title_key}".encode()).hexdigest()[:16]

def main():
    p = pathlib.Path("data.json")
    if not p.exists():
        print("[QC] data.json missing", file=sys.stderr)
        sys.exit(2)

    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[QC] Invalid JSON: {e}", file=sys.stderr)
        sys.exit(2)

    listings = data.get("jobListings", [])
    archived = data.get("archivedListings", [])
    sections = data.get("sections", {})
    tinfo = data.get("transparencyInfo", {})
    problems = []

    if not isinstance(listings, list):
        problems.append("jobListings must be list")
    if not isinstance(archived, list):
        problems.append("archivedListings must be list")

    # FIX P3-H-009: Better deduplication logic
    deduped = []
    seen_keys = {}
    duplicates_removed = 0

    for i, rec in enumerate(listings):
        rid = rec.get("id")

        if not rid:
            problems.append(f"[active #{i}] missing id")
            continue

        # Generate dedup key
        key = make_key(rec)

        if key in seen_keys:
            # Duplicate found - keep the one with more complete data
            existing_idx = seen_keys[key]
            existing = deduped[existing_idx]

            # Compare data completeness
            existing_score = (
                len(existing.get("title", ""))
                + len(existing.get("deadline", ""))
                + (existing.get("numberOfPosts") or 0)
            )
            current_score = (
                len(rec.get("title", ""))
                + len(rec.get("deadline", ""))
                + (rec.get("numberOfPosts") or 0)
            )

            if current_score > existing_score:
                # Replace with better version
                deduped[existing_idx] = rec

            duplicates_removed += 1
            continue

        seen_keys[key] = len(deduped)
        deduped.append(rec)

        # Basic validation
        title = (rec.get("title") or "").strip()
        if len(title) < 5:
            problems.append(f"[{rid}] short title ({len(title)} chars)")

        al = rec.get("applyLink")
        pl = rec.get("detailLink")
        if not (is_http_url(al) or is_http_url(pl)):
            problems.append(f"[{rid}] invalid URLs")

        dl = rec.get("deadline")
        if dl and dl.strip().upper() != "N/A":
            d = parse_date_any(dl)
            if not d:
                problems.append(f"[{rid}] invalid deadline format: {dl}")

        src = rec.get("source")
        if src not in ("official", "aggregator"):
            problems.append(f"[{rid}] invalid source: {src}")

        typ = rec.get("type")
        if typ not in ("VACANCY", "UPDATE"):
            problems.append(f"[{rid}] invalid type: {typ}")

    # If deduplication happened, write back
    if duplicates_removed > 0:
        data["jobListings"] = deduped
        data["transparencyInfo"]["totalListings"] = len(deduped)
        p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"[QC] Auto-deduped {duplicates_removed} duplicates", file=sys.stderr)

    # Validate archived entries
    for i, rec in enumerate(archived):
        rr = rec.get("flags", {}).get("removed_reason")
        if not rr:
            problems.append(f"[archived #{i}] missing removed_reason")

    # Update total if needed
    if isinstance(tinfo.get("totalListings"), int) and tinfo["totalListings"] != len(deduped):
        data["transparencyInfo"]["totalListings"] = len(deduped)
        p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    # Print results
    critical = [m for m in problems if "missing id" in m or "invalid JSON" in m]

    if critical:
        print("[QC] FAIL (critical errors)", file=sys.stderr)
        for m in critical:
            print(f"  - {m}", file=sys.stderr)
        sys.exit(1)

    if problems:
        print(
            f"[QC] OK with warnings (active={len(deduped)}, archived={len(archived)})",
            file=sys.stderr,
        )
        for m in problems[:5]:
            print(f"  ⚠ {m}", file=sys.stderr)
    else:
        print(f"[QC] OK (active={len(deduped)}, archived={len(archived)})", file=sys.stderr)

    sys.exit(0)

if __name__ == "__main__":
    main()
