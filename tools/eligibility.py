#!/usr/bin/env python3
# tools/eligibility.py v2025-11-03-FINAL
# FIX C-003: Better Hindi detection + comprehensive eligibility

import re

# Teacher positions
TEACHER = {
    "teacher", "tgt", "pgt", "prt", "faculty", "lecturer", 
    "assistant professor", "professor", "b.ed", "b ed", "ctet", "tet",
    "teaching", "instructional"
}

# Tech/Engineering positions  
TECH = {
    "b.tech", "btech", "b tech", "b.e", "b e", "m.tech", "m tech", "m.e", "m e",
    "mca", "bca", "engineer", "developer", "scientist", "architect", "analyst", 
    "devops", "cloud", "ml", "ai", "ai/ml", "research", "nursing", "iti", 
    "polytechnic", "diploma", "pharma", "pharmacy", "pharmacist"
}

# Postgraduate degrees
PG = {
    "mba", "m.b.a", "pg", "post graduate", "postgraduate", "post-graduate",
    "phd", "m.phil", "m phil", "mcom", "m.com", "m com", "ma", "m.a", "m a",
    "msc", "m.sc", "m sc", "master"
}

def clean(s):
    """Normalize whitespace"""
    return re.sub(r"\s+", " ", (s or "").strip())

def is_hindi_title(title):
    """
    Detect if title is PREDOMINANTLY Hindi/Devanagari (>70%)
    Devanagari Unicode range: 0x0900-0x097F
    """
    if not title or len(title) < 3:
        return False
    
    devanagari_count = sum(1 for c in title if 0x0900 <= ord(c) <= 0x097F)
    total_chars = len(title)
    
    devanagari_ratio = devanagari_count / total_chars if total_chars > 0 else 0
    
    # Only reject if >70% is Devanagari (majority Hindi)
    return devanagari_ratio > 0.7

def allow_skills(text):
    """Check if job requires specialty skills we filter out"""
    t = (text or "").lower()
    
    blocked_skills = {
        "steno", "stenographer", "shorthand",
        "trade test", "welding", "plumbing", "carpentry",
        "cad", "catia", "solidworks", "autocad",
        "sap", "oracle", "ems", "tally", "tally erp",
        "aws", "azure", "docker", "kubernetes", "jenkins"
    }
    
    return not any(skill in t for skill in blocked_skills)

def allow_domicile(title):
    """
    Smart domicile check for Bihar/India jobs
    """
    t = (title or "").lower()
    
    # Allow Bihar jobs without restriction
    if "bihar" in t:
        if "domicile" not in t and "locals only" not in t and "local candidates" not in t:
            return True
    
    # Explicitly open to all India
    if any(x in t for x in [
        "all india", "any state", "open to all", "pan india",
        "indian citizens", "across india", "from any state",
        "other state candidates", "outside state"
    ]):
        return True
    
    # Restricted to locals and NOT Bihar = reject
    if any(x in t for x in ["domicile", "locals only", "local candidates", "state quota"]):
        if "bihar" not in t:
            return False
    
    # Default: allow (generic)
    return True

def is_eligible(title):
    """
    Complete eligibility check combining all filters
    Returns: (is_eligible: bool, reason: str)
    """
    if not title or len(title.strip()) < 3:
        return False, "Invalid_title"
    
    t = clean(title).lower()
    
    # Check 1: Predominantly Hindi (>70% Devanagari)
    if is_hindi_title(title):
        return False, "Hindi_title"
    
    # Check 2: Exclude Teacher positions
    if any(k in t for k in TEACHER):
        return False, "Teacher_position"
    
    # Check 3: Exclude Tech/Engineering
    if any(k in t for k in TECH):
        return False, "Tech_position"
    
    # Check 4: Exclude Postgraduate-only
    if any(k in t for k in PG):
        return False, "Postgraduate_position"
    
    # Check 5: Specialty skills
    if not allow_skills(t):
        return False, "Specialty_skills_required"
    
    # Check 6: Domicile check
    if not allow_domicile(title):
        return False, "Wrong_domicile"
    
    return True, "Eligible"
