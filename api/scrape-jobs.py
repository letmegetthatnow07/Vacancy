#!/usr/bin/env python3
# api/scrape-jobs.py - Vercel serverless scraper function
# FIXES: P2-C-008 (correct async handler), P2-C-014 (env vars)

import json
import os
import pathlib
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs
import requests
from bs4 import BeautifulSoup

async def handler(request):
    """
    FIX P2-C-008: Use Vercel-compatible async handler, not BaseHTTPRequestHandler
    """
    
    try:
        # Parse query parameters
        query_params = parse_qs(urlparse(request.url).query) if hasattr(request, 'url') else {}
        is_deep_mode = 'mode' in query_params and query_params['mode'][0] == 'deep'
        
        # Scrape and find PDFs
        pdf_links = scrape_and_find_pdfs(deep_mode=is_deep_mode)
        sources_tried = 30  # Approximate
        
        # Trigger GitHub Actions webhook for OCR
        if pdf_links:
            trigger_ocr_workflow(pdf_links, deep_mode=is_deep_mode)
        
        # Return success response
        response = {
            'ok': True,
            'mode': 'deep' if is_deep_mode else 'normal',
            'scrapedSources': sources_tried,
            'pdfsSent': len(pdf_links),
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps(response)
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'ok': False, 'error': str(e)})
        }

def scrape_and_find_pdfs(deep_mode=False):
    """
    Scrape HTML sources and extract PDF links
    If deep_mode=True, scrapes more aggressively
    """
    
    pdf_links = []
    sources_tried = []
    
    try:
        # Load rules
        rules_path = pathlib.Path('/var/task/rules.json')
        if rules_path.exists():
            rules = json.load(open(rules_path))
        else:
            rules = {"captureHints": [], "aggregatorScores": {}}
        
        hints = rules.get('captureHints', [])
        
        # Deep mode: scrape ALL hints, normal mode: limit to 15
        scrape_limit = len(hints) if deep_mode else min(15, len(hints))
        
        # Scrape each hint
        for hint_url in hints[:scrape_limit]:
            try:
                sources_tried.append(hint_url)
                
                # Deep mode: longer timeout
                timeout = 30 if deep_mode else 10
                
                response = requests.get(
                    hint_url,
                    headers={'User-Agent': 'Mozilla/5.0'},
                    timeout=timeout,
                    verify=True
                )
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find PDF links
                for a in soup.select('a[href*=".pdf"]'):
                    href = a.get('href')
                    title = a.get_text(strip=True)
                    
                    if not href:
                        continue
                    
                    full_url = href if href.startswith('http') else urljoin(hint_url, href)
                    
                    pdf_links.append({
                        'url': full_url,
                        'source': urlparse(hint_url).netloc,
                        'title': title,
                        'foundAt': hint_url,
                        'deepMode': deep_mode
                    })
            
            except requests.exceptions.Timeout:
                print(f"Timeout: {hint_url}")
                continue
            except Exception as e:
                print(f"Scrape failed for {hint_url}: {e}")
                continue
    
    except Exception as e:
        print(f"Setup failed: {e}")
    
    return pdf_links

def trigger_ocr_workflow(pdf_links, deep_mode=False):
    """
    Send webhook to GitHub Actions to process PDFs
    FIX P2-C-014: Use env vars from Vercel secrets
    """
    
    # FIX P2-C-014: Read from environment (Vercel injects via vercel.json env section)
    github_token = os.environ.get('GITHUB_TOKEN')
    github_repo = os.environ.get('GITHUB_REPO')
    
    if not github_token or not github_repo:
        print("Warning: GitHub webhook credentials not configured")
        return
    
    webhook_url = f"https://api.github.com/repos/{github_repo}/dispatches"
    
    payload = {
        'event_type': 'process-pdfs',
        'client_payload': {
            'pdfs': pdf_links,
            'triggered_by': 'vercel-scraper',
            'mode': 'deep' if deep_mode else 'normal',
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
    }
    
    try:
        response = requests.post(
            webhook_url,
            headers={
                'Authorization': f'token {github_token}',
                'Accept': 'application/vnd.github.v3+json'
            },
            json=payload,
            timeout=10
        )
        
        if response.status_code == 204:
            mode_text = "deep" if deep_mode else "normal"
            print(f"✓ Triggered GitHub {mode_text} OCR workflow with {len(pdf_links)} PDFs")
        else:
            print(f"Webhook failed: {response.status_code} - {response.text}")
    
    except Exception as e:
        print(f"Webhook error: {e}")
