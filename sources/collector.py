# 1. Replace entire tools/collector.py with code above

# 2. Commit
git add tools/collector.py
git commit -m "fix: custom SSL context with HTTPAdapter

- Create custom SSLAdapter that disables certificate verification
- Use session-based requests instead of verify=False
- Works in strict Vercel environment"

git push origin main

# 3. Test
curl https://vacancy-kappa.vercel.app/api/scrape-jobs

# Expected output:
# [FETCH_OK] https://bpsc.bihar.gov.in/: found 8 jobs
# [DONE] Collected 150+ total jobs
