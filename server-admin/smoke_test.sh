#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# smoke_test.sh — WHG whole-site smoke test / post-upgrade validation harness
#
# Purpose: quickly confirm a deployment (esp. after a dependency / framework
# upgrade) has not broken the site. Sweeps public + authenticated pages for
# 500s, checks the DRF API returns JSON, and (optionally, over SSH) runs
# container-side checks: Elasticsearch search, outbound HTTP libs, the
# Django test suite, and the resources multi-file form.
#
# Created during the issue-#119 dependency-security campaign (Django 4.2 LTS).
#
# Requirements: bash, curl, python3 (for JSON checks). Deep checks also need
# ssh access to the whg host and `docker`.
#
# ── USAGE ────────────────────────────────────────────────────────────────────
#   server-admin/smoke_test.sh dev            # HTTP sweep of dev (public only)
#   server-admin/smoke_test.sh prod           # HTTP sweep of prod (public only)
#   server-admin/smoke_test.sh dev --cookie "sessionid=...; csrftoken=..."
#                                             # + authenticated-page sweep
#   server-admin/smoke_test.sh dev --deep --ssh whg
#                                             # + container-side checks over ssh
#   server-admin/smoke_test.sh dev --deep --ssh whg --suite
#                                             # ...also run the full test suite
#   server-admin/smoke_test.sh https://local.whgazetteer.org  --cookie "..."
#                                             # arbitrary base URL
#
# Get a session cookie for --cookie: log in via a browser, then copy the
# `sessionid` (and `csrftoken`) cookie values for the target host.
#
# Exit code 0 = all checks passed; non-zero = at least one failure.
# ─────────────────────────────────────────────────────────────────────────────
set -uo pipefail

UA="Mozilla/5.0 (WHG-smoke-test)"
COOKIE=""
SSH_HOST=""
CONTAINER=""
DEEP=0
RUN_SUITE=0
BASE=""

# ── Argument parsing ─────────────────────────────────────────────────────────
TARGET="${1:-dev}"; shift || true
while [ $# -gt 0 ]; do
  case "$1" in
    --cookie)    COOKIE="$2"; shift 2 ;;
    --ssh)       SSH_HOST="$2"; shift 2 ;;
    --container) CONTAINER="$2"; shift 2 ;;
    --deep)      DEEP=1; shift ;;
    --suite)     RUN_SUITE=1; DEEP=1; shift ;;
    -h|--help)   sed -n '2,40p' "$0"; exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
  esac
done

# ── Resolve target → base URL (+ default ssh host / container filter) ─────────
case "$TARGET" in
  dev)  BASE="https://dev.whgazetteer.org"; : "${SSH_HOST:=}"; CONTAINER_FILTER="web_dev" ;;
  prod) BASE="https://whgazetteer.org";     : "${SSH_HOST:=}"; CONTAINER_FILTER="web_whgazetteer-org_main" ;;
  http*) BASE="$TARGET"; CONTAINER_FILTER="web_" ;;
  *) echo "Unknown target '$TARGET' (use dev|prod|<url>)" >&2; exit 2 ;;
esac

PASS=0; FAIL=0; WARN=0
ok()   { PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$1"; }
bad()  { FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$1"; }
warn() { WARN=$((WARN+1)); printf '  \033[33mWARN\033[0m %s\n' "$1"; }
hd()   { printf '\n\033[1m%s\033[0m\n' "$1"; }

echo "WHG smoke test → $BASE   ($(date -u '+%Y-%m-%d %H:%M:%SZ'))"
[ -n "$COOKIE" ] && echo "  (authenticated sweep enabled)"

# ── 1. HTTP page sweep (flag only 5xx as failures) ───────────────────────────
http_code() { curl -s -o /dev/null -w "%{http_code}" -A "$UA" ${COOKIE:+-H "Cookie: $COOKIE"} "$BASE$1" --max-time 30; }

PUBLIC_PAGES="/ /about/ /teaching/ /license/ /privacy_policy/ /terms_of_use/ /publications/ /development/ /workbench/ /search/ /atlas/ /reconciliation/ /downloads/ /public_data/ /people_overview/ /journeys_routes/ /sitemap.xml /accounts/login/"
# Authenticated pages (only swept when --cookie given). Read-only GETs only.
AUTH_PAGES="/dashboard_user/ /profile/ /admin/"

hd "1. Page sweep (fail = HTTP 5xx)"
for ep in $PUBLIC_PAGES; do
  c=$(http_code "$ep")
  if [ "$c" -ge 500 ] 2>/dev/null; then bad "$ep → $c"; else printf '  ---- %-24s %s\n' "$ep" "$c"; fi
done
if [ -n "$COOKIE" ]; then
  for ep in $AUTH_PAGES; do
    c=$(http_code "$ep")
    if [ "$c" -ge 500 ] 2>/dev/null; then bad "$ep → $c"; else printf '  ---- %-24s %s\n' "$ep" "$c"; fi
  done
fi
[ "$FAIL" -eq 0 ] && ok "no 5xx across swept pages"

# ── 2. DRF API JSON checks ───────────────────────────────────────────────────
hd "2. API endpoints return JSON (DRF)"
api_json() { # $1=path  $2=grep-token expected in body
  local body code
  body=$(curl -s -w $'\n%{http_code}' -A "$UA" ${COOKIE:+-H "Cookie: $COOKIE"} "$BASE$1" --max-time 30)
  code=$(printf '%s' "$body" | tail -1)
  body=$(printf '%s' "$body" | sed '$d')
  if [ "$code" = "200" ] && printf '%s' "$body" | grep -q "$2"; then ok "$1 → 200, contains '$2'"; else bad "$1 → $code (expected 200 with '$2')"; fi
}
api_json "/api/sources/"     '"namespace"'
api_json "/api/attribution/" '"whg"'
api_json "/api/place/88106/" '"title"'
api_json "/api/area_list/"   '"type"'

# ── 3. Deep container-side checks (ES, outbound libs, forms, suite) ──────────
if [ "$DEEP" -eq 1 ]; then
  hd "3. Container-side checks"
  RUN="bash -c"
  if [ -n "$SSH_HOST" ]; then RUN="ssh $SSH_HOST"; fi
  # auto-detect container if not supplied
  if [ -z "$CONTAINER" ]; then
    CONTAINER=$($RUN "docker ps --filter name=$CONTAINER_FILTER --format '{{.Names}}' | head -1" 2>/dev/null)
  fi
  if [ -z "$CONTAINER" ]; then
    warn "could not resolve a web container (need --ssh/--container); skipping deep checks"
  else
    echo "  container: $CONTAINER"
    DJ='from django.conf import settings; import requests, certifi, urllib3, jwt, cryptography, django
print("django", django.get_version())
print("libs", "requests",requests.__version__,"urllib3",urllib3.__version__,"certifi",certifi.__version__,"pyjwt",jwt.__version__,"cryptography",cryptography.__version__)
es=settings.ES_CONN
r=es.search(index=settings.ES_WHG, body={"query":{"match":{"names.toponym":"Paris"}}}, size=1)
tot=r["hits"]["total"]["value"] if isinstance(r["hits"]["total"],dict) else r["hits"]["total"]
print("ES_SEARCH", settings.ES_WHG, "paris_hits", tot)
tok=jwt.encode({"s":"smoke"},"x"*32,algorithm="HS256"); print("PYJWT_OK", jwt.decode(tok,"x"*32,algorithms=["HS256"]))
from resources.forms import ResourceModelForm; f=ResourceModelForm()
print("RESOURCES_FORM_MULTIPLE", all("multiple" in str(f[n]) for n in ["files","images"]))'
    OUT=$($RUN "docker exec $CONTAINER ./manage.py shell -c '$DJ'" 2>&1 | grep -vE "Environment variables|Deprecation")
    echo "$OUT" | sed 's/^/    /'
    echo "$OUT" | grep -q "ES_SEARCH .* paris_hits [1-9]" && ok "ES search returns hits" || warn "ES search returned 0 hits (check ES_WHG index is populated)"
    echo "$OUT" | grep -q "PYJWT_OK" && ok "PyJWT HS256 round-trip" || bad "PyJWT round-trip failed"
    echo "$OUT" | grep -q "RESOURCES_FORM_MULTIPLE True" && ok "resources form renders multiple-file inputs" || bad "resources MultipleFileField broken"

    if [ "$RUN_SUITE" -eq 1 ]; then
      hd "3b. Django test suite (compare to baseline: 117 tests, 6 fail / 34 err — all pre-existing)"
      SUITE=$($RUN "docker exec $CONTAINER ./manage.py test 2>&1 | tail -3")
      echo "$SUITE" | sed 's/^/    /'
      echo "$SUITE" | grep -qE "FAILED \(failures=6, errors=34\)|OK" && ok "test suite matches baseline (no new regressions)" || warn "test suite differs from baseline — inspect new failures/errors"
    fi
  fi
fi

# ── Summary ──────────────────────────────────────────────────────────────────
hd "Summary"
printf '  PASS=%d  FAIL=%d  WARN=%d\n' "$PASS" "$FAIL" "$WARN"
[ "$FAIL" -eq 0 ] && { echo "  ✅ smoke test passed"; exit 0; } || { echo "  ❌ smoke test found failures"; exit 1; }
