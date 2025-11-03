#!/usr/bin/env python3
# api/scrape-jobs.py - Vercel serverless scraper (FIXED FOR VERCEL)
# FIX: Changed from BaseHTTPRequestHandler class to handler(request) function

import json
import os
import subprocess
import sys
import requests
from datetime import datetime

def handler(request):
    """Vercel serverless function - CORRECT FORMAT for cron triggers"""
    
    try:
        project_root = os.getcwd()
        print(f"[INFO] Project root: {project_root}", file=sys.stderr)
        
        # Verify critical files exist
        required_files = ['tools/collector.py', 'tools/schema_merge.py', 'qc_and_learn.py']
        for req_file in required_files:
            full_path = os.path.join(project_root, req_file)
            if not os.path.exists(full_path):
                print(f"[ERROR] Missing required file: {req_file}", file=sys.stderr)
                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        'ok': False,
                        'error': 'missing_file',
                        'file': req_file,
                        'path': full_path
                    })
                }
        
        # ===== STEP 0: Download user_state from Cloudflare KV =====
        print("[STEP 0] Fetching user_state from Cloudflare KV...", file=sys.stderr)
        
        kv_account = os.environ.get('CLOUDFLARE_KV_ACCOUNT_ID')
        kv_token = os.environ.get('CLOUDFLARE_KV_API_TOKEN')
        kv_namespace = os.environ.get('CLOUDFLARE_KV_NAMESPACE_ID')
        
        user_state_data = {}
        
        if kv_account and kv_token and kv_namespace:
            try:
                kv_url = f"https://api.cloudflare.com/client/v4/accounts/{kv_account}/storage/kv/namespaces/{kv_namespace}/values/user_state_personal.json"
                
                kv_response = requests.get(
                    kv_url,
                    headers={'Authorization': f"Bearer {kv_token}"},
                    timeout=15
                )
                
                if kv_response.ok:
                    user_state_data = kv_response.json()
                    print(f"[OK] Downloaded user_state from KV: {len(user_state_data)} entries", file=sys.stderr)
                else:
                    print(f"[WARN] KV fetch returned {kv_response.status_code} - using empty state", file=sys.stderr)
            
            except Exception as e:
                print(f"[WARN] KV fetch failed: {e} - using empty state", file=sys.stderr)
        else:
            print("[WARN] Missing KV credentials - using empty user_state", file=sys.stderr)
        
        # Write user_state to local file for GitHub Actions
        user_state_path = os.path.join(project_root, 'user_state.json')
        try:
            with open(user_state_path, 'w', encoding='utf-8') as f:
                json.dump(user_state_data, f, indent=2, ensure_ascii=False)
            print(f"[OK] Wrote user_state to {user_state_path}", file=sys.stderr)
        except Exception as e:
            print(f"[WARN] Writing user_state failed: {e}", file=sys.stderr)
        
        # ===== STEP 1: Run Collector =====
        print("[STEP 1] Running collector...", file=sys.stderr)
        
        collector_path = os.path.join(project_root, 'tools/collector.py')
        collector_result = subprocess.run(
            [sys.executable, collector_path],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=project_root
        )
        
        if collector_result.returncode != 0:
            print(f"[ERROR] Collector failed: {collector_result.stderr}", file=sys.stderr)
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'ok': False,
                    'error': 'collector_failed',
                    'detail': collector_result.stderr[:500],
                    'stdout': collector_result.stdout[:500]
                })
            }
        
        # Parse candidates from collector output (JSONL format)
        candidates = []
        for line in collector_result.stdout.strip().split('\n'):
            if line.strip():
                try:
                    candidates.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        
        # Write to tmp/candidates.jsonl for schema_merge.py
        candidates_jsonl_path = os.path.join(project_root, 'tmp/candidates.jsonl')
        os.makedirs(os.path.dirname(candidates_jsonl_path), exist_ok=True)
        
        with open(candidates_jsonl_path, 'w', encoding='utf-8') as f:
            for cand in candidates:
                f.write(json.dumps(cand, ensure_ascii=False) + '\n')
        
        print(f"[OK] Collector: {len(candidates)} candidates → {candidates_jsonl_path}", file=sys.stderr)
        
        # ===== STEP 2: Run Schema Merge =====
        print("[STEP 2] Running schema merge...", file=sys.stderr)
        
        data_json_path = os.path.join(project_root, 'data.json')
        schema_merge_path = os.path.join(project_root, 'tools/schema_merge.py')
        
        merge_result = subprocess.run(
            [sys.executable, schema_merge_path, data_json_path, candidates_jsonl_path, data_json_path],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=project_root
        )
        
        if merge_result.returncode != 0:
            print(f"[WARN] Schema merge returned non-zero: {merge_result.stderr}", file=sys.stderr)
        
        print(f"[OK] Schema merge completed", file=sys.stderr)
        
        # ===== STEP 3: Run QC & Learn =====
        print("[STEP 3] Running QC and Learn...", file=sys.stderr)
        
        qc_path = os.path.join(project_root, 'qc_and_learn.py')
        qc_result = subprocess.run(
            [sys.executable, qc_path, '--mode', 'nightly'],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=project_root
        )
        
        if qc_result.returncode != 0:
            print(f"[WARN] QC returned non-zero: {qc_result.stderr}", file=sys.stderr)
        
        print(f"[OK] QC completed", file=sys.stderr)
        
        # ===== STEP 4: Read Final data.json =====
        print("[STEP 4] Reading final data.json...", file=sys.stderr)
        
        try:
            with open(data_json_path, 'r', encoding='utf-8') as f:
                data_content = f.read()
            data_obj = json.loads(data_content)
            job_count = len(data_obj.get('jobListings', []))
            print(f"[OK] Final data.json: {job_count} jobs", file=sys.stderr)
        except Exception as e:
            print(f"[ERROR] Reading data.json: {e}", file=sys.stderr)
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'ok': False,
                    'error': 'data_read_failed',
                    'detail': str(e)[:200]
                })
            }
        
        # ===== STEP 5: Save to Cloudflare KV =====
        print("[STEP 5] Saving to Cloudflare KV...", file=sys.stderr)
        
        kv_saved = False
        if kv_account and kv_token and kv_namespace:
            try:
                kv_url = f"https://api.cloudflare.com/client/v4/accounts/{kv_account}/storage/kv/namespaces/{kv_namespace}/values/data.json"
                
                kv_response = requests.put(
                    kv_url,
                    data=data_content.encode('utf-8'),
                    headers={'Authorization': f"Bearer {kv_token}"},
                    timeout=30
                )
                
                if kv_response.ok:
                    print(f"[OK] KV saved: {kv_response.status_code}", file=sys.stderr)
                    kv_saved = True
                else:
                    print(f"[ERROR] KV save failed: {kv_response.status_code} - {kv_response.text[:200]}", file=sys.stderr)
            
            except Exception as e:
                print(f"[ERROR] KV save exception: {e}", file=sys.stderr)
        else:
            print("[WARN] Missing KV credentials - skipping KV save", file=sys.stderr)
        
        # Cleanup temp files
        try:
            os.remove(candidates_jsonl_path)
        except:
            pass
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'ok': True,
                'collected': len(candidates),
                'jobs_in_data': job_count,
                'merged': True,
                'qc_passed': True,
                'stored_in_kv': kv_saved,
                'user_state_synced': bool(user_state_data),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            })
        }
    
    except subprocess.TimeoutExpired as e:
        print(f"[ERROR] Process timeout: {e}", file=sys.stderr)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'ok': False,
                'error': 'timeout',
                'detail': str(e)[:200]
            })
        }
    
    except Exception as e:
        print(f"[ERROR] Exception: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'ok': False,
                'error': 'exception',
                'detail': str(e)[:500]
            })
        }
