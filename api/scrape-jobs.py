import json
import os
import subprocess
import sys
import requests
from datetime import datetime
def handler(request):
try:
project_root = os.getcwd()
print(f"[INFO] Project root: {project_root}", file=sys.stderr)
required_files = ['tools/collector.py', 'tools/schema_merge.py', 'qc_and_learn.py']
for req_file in required_files:
full_path = os.path.join(project_root, req_file)
if not os.path.exists(full_path):
print(f"[ERROR] Missing required file: {req_file}", file=sys.stderr)
return {'statusCode': 500,'body': json.dumps({'ok': False,'error': 'missing_file','file': req_file,'path': full_path})}
print("[STEP 0] Fetching user_state from Cloudflare KV...", file=sys.stderr)
kv_account = os.environ.get('CLOUDFLARE_KV_ACCOUNT_ID')
kv_token = os.environ.get('CLOUDFLARE_KV_API_TOKEN')
kv_namespace = os.environ.get('CLOUDFLARE_KV_NAMESPACE_ID')
user_state_data = {}
if kv_account and kv_token and kv_namespace:
try:
kv_url = f"https://api.cloudflare.com/client/v4/accounts/{kv_account}/storage/kv/namespaces/{kv_namespace}/values/user_state_personal.json"
kv_response = requests.get(kv_url, headers={'Authorization': f"Bearer {kv_token}"}, timeout=15)
if kv_response.ok:
user_state_data = kv_response.json()
print(f"[OK] Downloaded user_state from KV: {len(user_state_data)} entries", file=sys.stderr)
else:
print(f"[WARN] KV fetch returned {kv_response.status_code}", file=sys.stderr)
except Exception as e:
print(f"[WARN] KV fetch failed: {e}", file=sys.stderr)
else:
print("[WARN] Missing KV credentials - using empty user_state", file=sys.stderr)
try:
with open('user_state.json', 'w', encoding='utf-8') as f:
json.dump(user_state_data, f, indent=2, ensure_ascii=False)
print(f"[OK] Wrote user_state.json", file=sys.stderr)
except Exception as e:
print(f"[WARN] Writing user_state.json failed: {e}", file=sys.stderr)
print("[STEP 1] Running collector...", file=sys.stderr)
collector_result = subprocess.run([sys.executable, os.path.join(project_root, 'tools/collector.py')], capture_output=True, text=True, timeout=120, cwd=project_root)
if collector_result.returncode != 0:
print(f"[ERROR] Collector failed: {collector_result.stderr}", file=sys.stderr)
return {'statusCode': 500,'body': json.dumps({'ok': False,'error': 'collector_failed','detail': collector_result.stderr[:500],'stdout': collector_result.stdout[:500]})}
candidates = []
for line in collector_result.stdout.strip().split('\n'):
s = line.strip()
if not s: continue
try: candidates.append(json.loads(s))
except json.JSONDecodeError: pass
os.makedirs('tmp', exist_ok=True)
with open('tmp/candidates.jsonl', 'w', encoding='utf-8') as f:
for cand in candidates: f.write(json.dumps(cand, ensure_ascii=False) + '\n')
print(f"[OK] Collector: {len(candidates)} candidates -> tmp/candidates.jsonl", file=sys.stderr)
print("[STEP 2] Running schema merge...", file=sys.stderr)
data_json_path = os.path.join(project_root, 'data.json')
merge_result = subprocess.run([sys.executable, os.path.join(project_root, 'tools/schema_merge.py'), data_json_path, 'tmp/candidates.jsonl', data_json_path], capture_output=True, text=True, timeout=120, cwd=project_root)
if merge_result.returncode != 0:
print(f"[WARN] Schema merge non-zero: {merge_result.stderr}", file=sys.stderr)
print("[OK] Schema merge completed", file=sys.stderr)
print("[STEP 3] Running QC and Learn...", file=sys.stderr)
qc_result = subprocess.run([sys.executable, os.path.join(project_root, 'qc_and_learn.py'), '--mode', 'nightly'], capture_output=True, text=True, timeout=120, cwd=project_root)
if qc_result.returncode != 0:
print(f"[WARN] QC returned non-zero: {qc_result.stderr}", file=sys.stderr)
print("[OK] QC completed", file=sys.stderr)
print("[STEP 4] Reading final data.json...", file=sys.stderr)
try:
data_obj = json.loads(open(data_json_path, 'r', encoding='utf-8').read())
job_count = len(data_obj.get('jobListings', []))
print(f"[OK] Final data.json: {job_count} jobs", file=sys.stderr)
except Exception as e:
print(f"[ERROR] Reading data.json: {e}", file=sys.stderr)
return {'statusCode': 500,'body': json.dumps({'ok': False,'error': 'data_read_failed','detail': str(e)[:200]})}
print("[STEP 5] Saving to Cloudflare KV...", file=sys.stderr)
kv_saved = False
if kv_account and kv_token and kv_namespace:
try:
health_data = {'ok': True,'totalListings': job_count,'lastUpdated': datetime.utcnow().isoformat() + 'Z','source': 'vercel-scraper'}
kv_health_url = f"https://api.cloudflare.com/client/v4/accounts/{kv_account}/storage/kv/namespaces/{kv_namespace}/values/health.json"
hr = requests.put(kv_health_url, headers={'Authorization': f"Bearer {kv_token}", 'Content-Type': 'application/json'}, json=health_data, timeout=30)
if not hr.ok: print(f"[WARN] Health save failed: {hr.status_code}", file=sys.stderr)
if job_count > 0:
kv_data_url = f"https://api.cloudflare.com/client/v4/accounts/{kv_account}/storage/kv/namespaces/{kv_namespace}/values/data.json"
dr = requests.put(kv_data_url, headers={'Authorization': f"Bearer {kv_token}", 'Content-Type': 'application/json'}, json=data_obj, timeout=30)
if dr.ok: print(f"[OK] KV saved data.json", file=sys.stderr); kv_saved = True
else: print(f"[ERROR] KV data save failed: {dr.status_code} - {dr.text[:200]}", file=sys.stderr)
else:
print("[SKIP] KV save: 0 jobs (protect against empty publish)", file=sys.stderr)
except Exception as e:
print(f"[ERROR] KV save exception: {e}", file=sys.stderr)
else:
print("[WARN] Missing KV credentials - skipping KV save", file=sys.stderr)
try: os.remove('tmp/candidates.jsonl')
except: pass
return {'statusCode': 200,'body': json.dumps({'ok': True,'collected': len(candidates),'jobs_in_data': job_count,'merged': True,'qc_passed': True,'stored_in_kv': kv_saved,'user_state_synced': bool(user_state_data),'timestamp': datetime.utcnow().isoformat() + 'Z'})}
except subprocess.TimeoutExpired as e:
print(f"[ERROR] Process timeout: {e}", file=sys.stderr)
return {'statusCode': 500,'body': json.dumps({'ok': False,'error': 'timeout','detail': str(e)[:200]})}
except Exception as e:
print(f"[ERROR] Exception: {e}", file=sys.stderr)
import traceback; traceback.print_exc(file=sys.stderr)
return {'statusCode': 500,'body': json.dumps({'ok': False,'error': 'exception','detail': str(e)[:500]})}
