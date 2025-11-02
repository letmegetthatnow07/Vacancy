#!/usr/bin/env python3
# api/scrape-jobs.py - Vercel serverless scraper
# Runs Phase 1 (collector → merge → QC) on schedule
# Stores results in Cloudflare KV (NOT GitHub)

import json
import os
import subprocess
import sys
from http.server import BaseHTTPRequestHandler

# Phase 1 imports
sys.path.insert(0, '/var/task')

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Triggered by Vercel cron - runs full Phase 1 pipeline"""
        
        try:
            # Step 1: Run Collector
            print("Running collector...", file=sys.stderr)
            collector_result = subprocess.run(
                [sys.executable, 'tools/collector.py'],
                capture_output=True,
                text=True,
                timeout=120
            )
            
            if collector_result.returncode != 0:
                return self.json_response({
                    'ok': False,
                    'error': 'collector_failed',
                    'detail': collector_result.stderr[:500]
                }, 500)
            
            candidates = [json.loads(line) for line in collector_result.stdout.strip().split('\n') if line.strip()]
            print(f"Collected {len(candidates)} candidates", file=sys.stderr)
            
            # Step 2: Run Schema Merge
            print("Running schema merge...", file=sys.stderr)
            merge_result = subprocess.run(
                [sys.executable, 'tools/schema_merge.py', 'data.json', '/tmp/candidates.jsonl', 'data.json'],
                capture_output=True,
                text=True,
                timeout=120
            )
            
            if merge_result.returncode != 0:
                return self.json_response({
                    'ok': False,
                    'error': 'merge_failed',
                    'detail': merge_result.stderr[:500]
                }, 500)
            
            # Step 3: Run QC + Learn
            print("Running QC + Learn...", file=sys.stderr)
            qc_result = subprocess.run(
                [sys.executable, 'qc_and_learn.py', '--mode', 'nightly'],
                capture_output=True,
                text=True,
                timeout=120
            )
            
            if qc_result.returncode != 0:
                return self.json_response({
                    'ok': False,
                    'error': 'qc_failed',
                    'detail': qc_result.stderr[:500]
                }, 500)
            
            # Step 4: Save to Cloudflare KV (NOT GitHub!)
            import requests
            
            with open('data.json', 'r') as f:
                data_content = f.read()
            
            kv_api = f"https://api.cloudflare.com/client/v4/accounts/{os.environ.get('CLOUDFLARE_KV_ACCOUNT_ID')}/storage/kv/namespaces/{os.environ.get('CLOUDFLARE_KV_NAMESPACE_ID')}/values/data.json"
            
            kv_response = requests.put(
                kv_api,
                data=data_content,
                headers={
                    'Authorization': f"Bearer {os.environ.get('CLOUDFLARE_KV_API_TOKEN')}",
                    'Content-Type': 'application/json'
                }
            )
            
            if not kv_response.ok:
                return self.json_response({
                    'ok': False,
                    'error': 'kv_save_failed',
                    'detail': kv_response.text[:500]
                }, 500)
            
            # Step 5: Optionally queue PDFs for OCR (via GitHub dispatch)
            dispatch_payload = {
                "event_type": "process-pdfs",
                "client_payload": {
                    "pdfs": [
                        {"url": "...", "jobId": "...", "title": "..."}
                        # Would populate from candidates with needs_pdf_review flag
                    ],
                    "mode": "auto"
                }
            }
            
            dispatch_response = requests.post(
                f"https://api.github.com/repos/{os.environ.get('GITHUB_REPO')}/dispatches",
                json=dispatch_payload,
                headers={
                    'Authorization': f"token {os.environ.get('GITHUB_TOKEN')}",
                    'Accept': 'application/vnd.github.v3+json'
                }
            )
            
            return self.json_response({
                'ok': True,
                'collected': len(candidates),
                'merged': True,
                'qc_passed': True,
                'stored_in_kv': True,
                'pdf_dispatch': dispatch_response.status_code == 204,
                'timestamp': __import__('datetime').datetime.utcnow().isoformat()
            })
        
        except Exception as e:
            return self.json_response({
                'ok': False,
                'error': 'exception',
                'detail': str(e)[:500]
            }, 500)
    
    def json_response(self, data, status=200):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())
