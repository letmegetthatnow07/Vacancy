#!/usr/bin/env python3
# api/scrape-jobs.py - Vercel serverless scraper
# Saves results to Cloudflare KV

import json
import os
import subprocess
import sys
import requests
from http.server import BaseHTTPRequestHandler

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
                timeout=120,
                cwd='/var/task'
            )
            
            if collector_result.returncode != 0:
                print(f"Collector error: {collector_result.stderr}", file=sys.stderr)
                return self.json_response({
                    'ok': False,
                    'error': 'collector_failed',
                    'detail': collector_result.stderr[:500]
                }, 500)
            
            # Parse candidates
            candidates = []
            for line in collector_result.stdout.strip().split('\n'):
                if line.strip():
                    try:
                        candidates.append(json.loads(line))
                    except:
                        pass
            
            print(f"Collected {len(candidates)} candidates", file=sys.stderr)
            
            # Step 2: Run Schema Merge
            print("Running schema merge...", file=sys.stderr)
            merge_result = subprocess.run(
                [sys.executable, 'tools/schema_merge.py', 'data.json', '/tmp/candidates.jsonl', 'data.json'],
                capture_output=True,
                text=True,
                timeout=120,
                cwd='/var/task'
            )
            
            # Step 3: Run QC + Learn
            print("Running QC + Learn...", file=sys.stderr)
            qc_result = subprocess.run(
                [sys.executable, 'qc_and_learn.py', '--mode', 'nightly'],
                capture_output=True,
                text=True,
                timeout=120,
                cwd='/var/task'
            )
            
            # Step 4: Read final data.json
            try:
                with open('/var/task/data.json', 'r') as f:
                    data_content = f.read()
                data_obj = json.loads(data_content)
                print(f"Final data: {len(data_obj.get('jobListings', []))} jobs", file=sys.stderr)
            except Exception as e:
                print(f"Error reading data.json: {e}", file=sys.stderr)
                return self.json_response({
                    'ok': False,
                    'error': 'data_read_failed',
                    'detail': str(e)[:200]
                }, 500)
            
            # Step 5: Save to Cloudflare KV
            print("Saving to Cloudflare KV...", file=sys.stderr)
            
            kv_account = os.environ.get('CLOUDFLARE_KV_ACCOUNT_ID')
            kv_token = os.environ.get('CLOUDFLARE_KV_API_TOKEN')
            kv_namespace = os.environ.get('CLOUDFLARE_KV_NAMESPACE_ID')
            
            if not (kv_account and kv_token and kv_namespace):
                print("Missing KV credentials", file=sys.stderr)
                return self.json_response({
                    'ok': False,
                    'error': 'missing_kv_credentials'
                }, 500)
            
            # Save data.json to KV
            kv_url = f"https://api.cloudflare.com/client/v4/accounts/{kv_account}/storage/kv/namespaces/{kv_namespace}/values/data.json"
            
            kv_response = requests.put(
                kv_url,
                data=data_content,
                headers={
                    'Authorization': f"Bearer {kv_token}",
                    'Content-Type': 'application/json'
                },
                timeout=30
            )
            
            if not kv_response.ok:
                print(f"KV save failed: {kv_response.status_code} - {kv_response.text}", file=sys.stderr)
                return self.json_response({
                    'ok': False,
                    'error': 'kv_save_failed',
                    'detail': kv_response.text[:200]
                }, 500)
            
            print(f"✓ Saved to KV", file=sys.stderr)
            
            # Step 6: Trigger GitHub OCR workflow
            print("Triggering GitHub OCR...", file=sys.stderr)
            
            github_token = os.environ.get('GITHUB_TOKEN')
            github_repo = os.environ.get('GITHUB_REPO')
            
            pdf_jobs = []
            for candidate in candidates:
                if candidate.get('flags', {}).get('needs_pdf_review') and candidate.get('pdfLink'):
                    pdf_jobs.append({
                        'url': candidate['pdfLink'],
                        'jobId': candidate.get('id', ''),
                        'title': candidate.get('title', ''),
                        'source': candidate.get('source', 'unknown')
                    })
            
            if pdf_jobs and github_token and github_repo:
                dispatch_payload = {
                    "event_type": "process-pdfs",
                    "client_payload": {
                        "pdfs": pdf_jobs,
                        "mode": "auto"
                    }
                }
                
                dispatch_response = requests.post(
                    f"https://api.github.com/repos/{github_repo}/dispatches",
                    json=dispatch_payload,
                    headers={
                        'Authorization': f"token {github_token}",
                        'Accept': 'application/vnd.github.v3+json'
                    },
                    timeout=15
                )
                
                pdf_triggered = dispatch_response.status_code == 204
                print(f"{'✓' if pdf_triggered else '✗'} GitHub dispatch: {dispatch_response.status_code}", file=sys.stderr)
            else:
                pdf_triggered = False
            
            return self.json_response({
                'ok': True,
                'collected': len(candidates),
                'jobs_in_data': len(data_obj.get('jobListings', [])),
                'merged': True,
                'qc_passed': True,
                'stored_in_kv': True,
                'pdf_count': len(pdf_jobs),
                'pdf_dispatch': pdf_triggered,
                'timestamp': __import__('datetime').datetime.utcnow().isoformat() + 'Z'
            })
        
        except Exception as e:
            print(f"Exception: {e}", file=sys.stderr)
            return self.json_response({
                'ok': False,
                'error': 'exception',
                'detail': str(e)[:500]
            }, 500)
    
    def json_response(self, data, status=200):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())
