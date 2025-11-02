#!/usr/bin/env python3
# tools/pdf_parser.py — Extract vacancy details from PDFs with OCR and Hindi support
# FIXES: P2-C-006, P2-C-007 (deterministic SHA1 IDs), P2-H-001 (clean output)

import re, json, sys, pathlib, hashlib, requests, time, urllib3, argparse, os
from datetime import datetime, date
from urllib.parse import urlparse
import tempfile

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    import PyPDF2
except ImportError:
    PyPDF2 = None

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

try:
    from pdf2image import convert_from_path
    import pytesseract
    HAS_OCR = True
except ImportError:
    HAS_OCR = False
    print("⚠ OCR not available. Install: pip install pdf2image pytesseract pillow", file=sys.stderr)

# Eligibility rules
ALLOWED_EDU = ["10th", "matric", "ssc", "12th", "intermediate", "hsc", "any graduate", "graduate", "स्नातक", "मैट्रिक"]
BLOCKED_EDU = [
    "b.tech", "btech", "b.e", "m.tech", "mtech", "m.e", "mca", "bca", "mba", 
    "msc", "m.sc", "phd", "m.phil", "postgraduate", "post graduate",
    "teacher", "tgt", "pgt", "prt", "b.ed", "ctet", "tet",
    "nursing", "pharma", "iti", "polytechnic", "diploma"
]

DATE_PAT = re.compile(r"(\d{1,2}[-/\.]\d{1,2}[-/\.]\d{2,4})", re.I)
POSTS_PAT = re.compile(r"(\d{1,6})\s*(posts?|vacanc(?:y|ies)|seats?|पद|रिक्ति)", re.I)

def clean(s):
    return re.sub(r"\s+", " ", (s or "").strip())

def norm_url(u):
    try:
        p = urlparse(u or "")
        base = p._replace(query="", fragment="")
        return base.geturl().rstrip("/").lower()
    except:
        return (u or "").rstrip("/").lower()

def stable_id(url):
    """
    FIX P2-C-006 & P2-C-007: Use DETERMINISTIC SHA1 ID (Phase 1 compatible)
    Matches collector.py, schema_merge.py, qc_and_learn.py
    """
    try:
        norm = norm_url(url)
        return f"job_{hashlib.sha1(norm.encode()).hexdigest()[:12]}"
    except:
        return f"job_{hashlib.md5((url or '').lower().encode()).hexdigest()[:12]}"

def download_pdf(url, cache_dir=".cache"):
    pathlib.Path(cache_dir).mkdir(exist_ok=True)
    file_hash = hashlib.sha1(url.encode()).hexdigest()[:16]
    cache_path = pathlib.Path(cache_dir) / f"{file_hash}.pdf"
    
    if cache_path.exists():
        return cache_path
    
    try:
        r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"}, verify=True)
        r.raise_for_status()
        cache_path.write_bytes(r.content)
        print(f"✓ Downloaded: {url[:60]}...", file=sys.stderr)
        return cache_path
    except requests.exceptions.SSLError:
        try:
            print(f"⚠ SSL error, retrying without verification: {url[:60]}...", file=sys.stderr)
            r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"}, verify=False)
            r.raise_for_status()
            cache_path.write_bytes(r.content)
            print(f"✓ Downloaded (no SSL): {url[:60]}...", file=sys.stderr)
            return cache_path
        except Exception as e:
            print(f"✗ Download failed: {url} - {e}", file=sys.stderr)
            return None
    except Exception as e:
        print(f"✗ Download failed: {url} - {e}", file=sys.stderr)
        return None

def extract_text_ocr(pdf_path):
    """Extract text using OCR with Hindi/English support (FIX P2-H-004: more pages)"""
    if not HAS_OCR:
        return ""
    
    try:
        print(f"⚙ Running OCR (Hindi+English) on: {pdf_path.name}...", file=sys.stderr)
        
        # Convert PDF to images (FIX P2-H-004: check up to 5 pages, not just 3)
        images = convert_from_path(str(pdf_path), dpi=300, first_page=1, last_page=5)
        
        text = ""
        for i, image in enumerate(images[:5]):
            # OCR with Hindi + English
            page_text = pytesseract.image_to_string(image, lang='hin+eng')
            text += page_text + "\n"
            print(f"  ✓ OCR page {i+1}: {len(page_text)} chars", file=sys.stderr)
        
        return clean(text)
    except Exception as e:
        print(f"⚠ OCR failed: {e}", file=sys.stderr)
        return ""

def extract_text(pdf_path):
    text = ""
    
    # Try pdfplumber first (fast, good for text PDFs)
    if pdfplumber:
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages[:5]:  # First 5 pages
                    text += page.extract_text() or ""
        except:
            pass
    
    # Fallback to PyPDF2
    if not text and PyPDF2:
        try:
            with open(pdf_path, "rb") as f:
                reader = PyPDF2.PdfReader(f)
                for page in reader.pages[:5]:
                    text += page.extract_text() or ""
        except:
            pass
    
    text = clean(text)
    
    # If no text or very little text, use OCR
    if len(text) < 200:
        print(f"⚠ Low text extraction ({len(text)} chars), trying OCR...", file=sys.stderr)
        ocr_text = extract_text_ocr(pdf_path)
        if ocr_text and len(ocr_text) > len(text):
            text = ocr_text
    
    return text

def parse_date(text):
    dates = []
    for m in DATE_PAT.finditer(text):
        raw = m.group(1).replace(".", "/").replace("-", "/")
        parts = raw.split("/")
        if len(parts) == 3:
            try:
                d, mo, y = int(parts[0]), int(parts[1]), int(parts[2])
                if y < 100:
                    y += 2000
                if 1 <= mo <= 12 and 1 <= d <= 31:
                    dates.append(date(y, mo, d))
            except:
                pass
    return max(dates) if dates else None

def parse_posts(text):
    m = POSTS_PAT.search(text)
    if m:
        try:
            return int(m.group(1))
        except:
            pass
    return None

def check_eligibility(text):
    t = text.lower()
    if any(blocked in t for blocked in BLOCKED_EDU):
        return False
    return True

def parse_pdf(url, pdf_path, source="unknown"):
    text = extract_text(pdf_path)
    
    if not text or len(text) < 100:
        print(f"✗ No text extracted from: {url}", file=sys.stderr)
        return None
    
    # Check eligibility
    if not check_eligibility(text[:2000]):
        print(f"✗ Filtered (eligibility): {url[:60]}...", file=sys.stderr)
        return None
    
    # FIX P2-H-008: Better title extraction (not just filename)
    title = None
    
    # Try to find title from PDF text (recruitment/notification lines)
    lines = [l.strip() for l in text.split("\n") if l.strip() and len(l) > 10]
    for line in lines[:15]:  # Check first 15 lines
        if any(kw in line.lower() for kw in ["recruitment", "notification", "advertisement", "advt", "भर्ती", "विज्ञापन"]):
            title = clean(line)[:200]
            if len(title) > 15:  # Only use if meaningful length
                break
    
    # Fallback: use filename as last resort
    if not title or len(title) < 10:
        filename = urlparse(url).path.split("/")[-1]
        title = filename.replace(".pdf", "").replace("_", " ").replace("-", " ").title()
    
    last_date = parse_date(text)
    posts = parse_posts(text)
    
    domicile = "Bihar" if any(kw in text.lower() for kw in ["bihar", "बिहार"]) else "All India"
    
    job = {
        "id": stable_id(url),  # FIX P2-C-006: Deterministic SHA1
        "title": title,
        "qualificationLevel": "Any graduate",
        "domicile": domicile,
        "deadline": last_date.strftime("%d/%m/%Y") if last_date else "N/A",
        "applyLink": url,
        "detailLink": url,
        "source": "official",
        "type": "VACANCY",
        "extractedAt": datetime.utcnow().isoformat() + "Z",
        "meta": {"sourceUrl": source, "sourceSite": "PDF"},
        "flags": {"parsed_from_pdf": True, "ocr_used": len(text) > 200 and not text.isascii()}
    }
    
    if posts:
        job["numberOfPosts"] = posts
    
    print(f"✓ Extracted: {title[:60]}... | Posts: {posts or 'N/A'} | Date: {job['deadline']}", file=sys.stderr)
    
    # FIX P2-H-001: Output ONLY JSONL to stdout (all debug to stderr)
    print(json.dumps(job, ensure_ascii=False))
    
    return job

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pdf_files", nargs="*", help="PDF file paths or URLs")
    ap.add_argument("--source", default="unknown", help="Source identifier")
    ap.add_argument("--output", help="Output JSONL file (optional, stdout by default)")
    args = ap.parse_args()
    
    # Accept URLs/paths from args or stdin
    inputs = args.pdf_files if args.pdf_files else [line.strip() for line in sys.stdin if line.strip()]
    
    results = []
    for item in inputs:
        # Determine if URL or local file
        if item.startswith("http://") or item.startswith("https://"):
            pdf_path = download_pdf(item)
            url = item
        elif pathlib.Path(item).exists():
            pdf_path = pathlib.Path(item)
            url = f"file://{pdf_path.name}"
        else:
            print(f"✗ Invalid input: {item}", file=sys.stderr)
            continue
        
        if pdf_path:
            job = parse_pdf(url, pdf_path, args.source)
            if job:
                results.append(job)
        
        time.sleep(0.3)
    
    # If output file specified, write there
    if args.output:
        pathlib.Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            for job in results:
                f.write(json.dumps(job, ensure_ascii=False) + "\n")
    
    print(f"\n✓ Processed {len(inputs)} PDFs, extracted {len(results)} eligible jobs", file=sys.stderr)
    return 0 if results else 1

if __name__ == "__main__":
    sys.exit(main())
