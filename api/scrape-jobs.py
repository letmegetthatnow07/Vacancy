#!/usr/bin/env python3
# api/scrape-jobs.py - Vercel serverless function

import json
import os
from http.server import BaseHTTPRequestHandler
from datetime import datetime
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Main scraping endpoint called by Vercel cron"""
        
        # Your existing scraping logic (simplified)
        pdf_links = self.scrape_and_find_pdfs()
        
        # Trigger GitHub Actions webhook for OCR
        if pdf_links:
            self.trigger_ocr_workflow(pdf_links)
        
        # Return success
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        
        response = {
            'ok': True,
            'scrapedSources': len(self.sources_tried),
            'pdfsSent': len(pdf_links),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        self.wfile.write(json.dumps(response).encode())
    
    def scrape_and_find_pdfs(self):
        """
        Scrape HTML sources and extract PDF links
        Returns list of {url, source, title} dicts
        """
        
        pdf_links = []
        self.sources_tried = []
        
        # Load rules
        try:
            # In Vercel, read from project root
            with open('rules.json') as f:
                rules = json.load(f)
            hints = rules.get('captureHints', [])
        except:
            hints = []
        
        # Scrape each hint (official sources only)
        for hint_url in hints[:20]:  # Limit to avoid timeout
            try:
                self.sources_tried.append(hint_url)
                
                response = requests.get(
                    hint_url,
                    headers={'User-Agent': 'Mozilla/5.0'},
                    timeout=10
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
                        'foundAt': hint_url
                    })
            
            except Exception as e:
                print(f"Scrape failed for {hint_url}: {e}")
                continue
        
        return pdf_links
    
    def trigger_ocr_workflow(self, pdf_links):
        """
        Send webhook to GitHub Actions to process PDFs
        """
        
        github_token = os.environ.get('GITHUB_TOKEN')
        github_repo = os.environ.get('GITHUB_REPO')  # format: "username/repo"
        
        if not github_token or not github_repo:
            print("Warning: GitHub webhook credentials not configured")
            return
        
        webhook_url = f"https://api.github.com/repos/{github_repo}/dispatches"
        
        payload = {
            'event_type': 'process-pdfs',
            'client_payload': {
                'pdfs': pdf_links,
                'triggered_by': 'vercel-scraper',
                'timestamp': datetime.utcnow().isoformat()
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
                print(f"✓ Triggered GitHub OCR workflow with {len(pdf_links)} PDFs")
            else:
                print(f"Webhook failed: {response.status_code} - {response.text}")
        
        except Exception as e:
            print(f"Webhook error: {e}")
