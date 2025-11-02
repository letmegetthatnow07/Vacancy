#!/usr/bin/env python3
# tools/eligibility.py v2025-11-02-MERGED
# FIX C-003: Combined Hindi detection + comprehensive eligibility filtering

import re

# Teacher positions
TEACHER = {
    "teacher", "tgt", "pgt", "prt", "faculty", "lecturer", 
    "assistant professor", "professor", "b.ed", "ctet", "tet"
}

# Tech/Engineering positions
TECH = {
    "b.tech", "btech", "b.e", "m.tech", "m.e", "mca", "bca", 
    "engineer", "developer", "scientist", "architect", "analyst", 
    "devops", "cloud", "ml", "ai", "research", "nursing", 
    "pharmac", "iti", "polytechnic", "diploma"
}

# Postgraduate degrees
PG = {
    "mba", "pg", "post graduate", "postgraduate", "phd", "m.phil", 
    "mcom", "m.com", "ma", "m.a", "msc", "m.sc", "mca", "mba"
}

# Open to all India / no domicile restriction
OPEN = {
    "all india", "any state", "open to all", "pan india", 
    "indian citizens", "across india", "from any state", 
    "other state candidates", "outside state"
}

# Closed / domicile restricted
CLOSE = {
    "domicile", "resident", "locals only", "local candidates", 
    "state quota", "only for domicile"
}

def clean(s):
    """Normalize whitespace"""
    return re.sub(r"\s+", " ", (s or "").strip())

def is_hindi_title(title):
    """
    FIX C-003: Detect Hindi/Devanagari text (>30%)
    Devanagari Unicode range: 0x0900-0x097F
    """
    if not title:
        return False
    
    devanagari_count = sum(1 for c in title if 0x0900 <= ord(c) <= 0x097F)
    total_chars = len(title)
    
    if total_chars == 0:
        return False
    
    devanagari_ratio = devanagari_count / total_chars
    return devanagari_ratio > 0.3

def education_band(text):
    """Extract education level from text"""
    t = (text or "").lower()
    
    if any(k in t for k in ["10th", "matric", "ssc"]):
        return "10th pass"
    
    if any(k in t for k in ["12th", "intermediate", "hsc"]):
        return "12th pass"
    
    if "any graduate" in t or "any degree" in t or re.search(r"\bgraduate\b", t):
        return "Any graduate"
    
    return "N/A"

def allow_skills(text):
    """Check if job requires specialty skills we don't have"""
    t = (text or "").lower()
    
    blocked_skills = {
        "steno", "shorthand", "trade test", "cad", "sap", "oracle", 
        "aws", "azure", "docker", "kubernetes", "tally erp"
    }
    
    return not any(skill in t for skill in blocked_skills)

def allow_domicile(title):
    """
    Smart domicile check:
    - Allow if Bihar and not restricted
    - Allow if explicitly open to all India
    - Reject if restricted to locals and not Bihar
    """
    t = (title or "").lower()
    
    # Bihar jobs without restriction = ✅
    if "bihar" in t and not any(k in t for k in CLOSE):
        return True
    
    # Explicitly open to all India = ✅
    if any(k in t for k in OPEN):
        return True
    
    # Restricted to locals but not Bihar = ❌
    if "only for domicile" in t and "bihar" not in t:
        return False
    
    # Default: allow (might be generic)
    return True

def exclude_streams(text):
    """Check if job is in blocked stream"""
    t = (text or "").lower()
    
    if any(k in t for k in TEACHER):
        return True
    
    if any(k in t for k in TECH):
        return True
    
    if any(k in t for k in PG):
        return True
    
    return False

def is_eligible(title):
    """
    Complete eligibility check combining all filters
    Returns: (is_eligible: bool, reason: str)
    """
    if not title:
        return False, "Empty_title"
    
    t = clean(title).lower()
    
    # FIX C-003: Check for Hindi first (>30% Devanagari)
    if is_hindi_title(title):
        return False, "Hindi_title"
    
    # Check for invalid/corrupted title
    if len(title.strip()) < 3:
        return False, "Invalid_title"
    
    # Exclude teacher positions
    if any(k in t for k in TEACHER):
        return False, "Teacher_position"
    
    # Exclude tech/engineering positions
    if any(k in t for k in TECH):
        return False, "Tech_position"
    
    # Exclude postgraduate positions
    if any(k in t for k in PG):
        return False, "Postgraduate_position"
    
    # Check for specialty skills
    if not allow_skills(t):
        return False, "Specialty_skills_required"
    
    # Check domicile
    if not allow_domicile(title):
        return False, "Wrong_domicile"
    
    # Check education band
    band = education_band(t)
    if band not in {"10th pass", "12th pass", "Any graduate"}:
        return False, f"Wrong_education_{band}"
    
    return True, "Eligible"
