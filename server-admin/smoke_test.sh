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
#                                             # (dev only — REFUSED against prod:
#                                             #  it writes to the live ES index and
#                                             #  creates a DB on prod's Postgres),
#                                             #    comparing FAILING TEST IDS to
#                                             #    server-admin/test-baseline.txt
#   server-admin/smoke_test.sh dev --deep --ssh whg --write-baseline
#                                             # seed/refresh that baseline; READ
#                                             #    the diff before committing it
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
WRITE_BASELINE=0
# Committed list of test ids that are ALREADY failing, so the check can report new
# ones by name instead of comparing totals. Seed/refresh it with --write-baseline
# and read the diff before committing it.
SUITE_BASELINE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/test-baseline.txt"
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
    --write-baseline) RUN_SUITE=1; DEEP=1; WRITE_BASELINE=1; shift ;;
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

# ── The suite must never run against production ──────────────────────────────
#
# `manage.py test` inside the prod container is not a read-only observation of
# production, it is three writes to it:
#
#   * ES_READ_ONLY is `ENV_CONTEXT != "whgazetteer-org"`, so on prod the ES
#     connection is NOT wrapped — verified live 2026-09-07: ES_READ_ONLY False,
#     index `whg`. Combined with CELERY_ALWAYS_EAGER, which settings.py turns on
#     whenever "test" is in sys.argv, any test that fires an indexing or publish
#     task executes it inline against the LIVE index. On dev that is structurally
#     impossible; on prod nothing stops it.
#   * it CREATES and drops a database on the production Postgres and runs the
#     whole migration history into it, on a host that has OOM-killed celery.
#   * a baseline measured there would bake prod's network reachability into a
#     file that is then compared against runs made elsewhere.
#
# There is deliberately no override flag. Every reason above is structural rather
# than circumstantial, so a flag would only ever be used by someone who had not
# read this. Prod's routine check is --deep, which needs no database and no
# baseline. Blocked here, before the sweep, so nothing at all is touched.
if [ "$RUN_SUITE" -eq 1 ]; then
  case "$BASE" in
    *//whgazetteer.org*|*//www.whgazetteer.org*)
      echo "REFUSING: --suite / --write-baseline against PRODUCTION ($BASE)." >&2
      echo "" >&2
      echo "  manage.py test would run with a WRITABLE connection to the live 'whg'" >&2
      echo "  index (ES_READ_ONLY is false on prod) and CELERY_ALWAYS_EAGER on, and" >&2
      echo "  would create a test database on the production Postgres." >&2
      echo "" >&2
      echo "  Run the suite on dev instead:" >&2
      echo "    $(basename "$0") dev --deep --ssh whg --suite" >&2
      echo "" >&2
      echo "  For prod, --deep is the check: it needs no database and no baseline." >&2
      exit 2 ;;
  esac
fi

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
# The place payload the site's own pages use. Anonymous callers of this internal route need a CSRF
# cookie and header (the same as the site's JS); /api/place/ is deprecated and goes away at the v4
# switch, so it is not tested here.
CSRF_JAR=$(mktemp)
curl -s -o /dev/null -c "$CSRF_JAR" -A "$UA" "$BASE/" --max-time 30
CSRF_TOKEN=$(awk '$6=="csrftoken"{print $7}' "$CSRF_JAR")
body=$(curl -s -w $'\n%{http_code}' -A "$UA" -b "$CSRF_JAR" -H "X-CSRFToken: $CSRF_TOKEN" "$BASE/places/88106/panel/" --max-time 30)
code=$(printf '%s' "$body" | tail -1); body=$(printf '%s' "$body" | sed '$d')
rm -f "$CSRF_JAR"
if [ "$code" = "200" ] && printf '%s' "$body" | grep -q '"title"'; then ok "/places/88106/panel/ → 200, contains 'title'"; else bad "/places/88106/panel/ → $code (expected 200 with 'title')"; fi
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
      # Compare the SET of failing test ids, not the counts.
      #
      # This used to grep for `FAILED (failures=6, errors=34)`, which broke the
      # moment the runner appended `, skipped=2` — identical failures, reported as
      # a regression. Worse, its fallback alternative was a bare `OK`, so an "OK"
      # on any line satisfied it; and `tail -3` discarded every failure NAME, so a
      # mismatch told you a number had moved and nothing about which test. A
      # count-only baseline also goes stale every time anyone adds a test, and
      # then reads as noise until people stop looking at it.
      #
      # Ids are exact, survive additions and skips, and distinguish a NEW failure
      # from one that was fixed.
      hd "3b. Django test suite (comparing failing-test ids to $SUITE_BASELINE)"
      # ~2 minutes, and a deploy during it kills the `docker exec` (137) — the run
      # then looks like a suite with no failures rather than a suite that stopped.
      echo "    Running the suite (~2 min); do not deploy $TARGET until it finishes."
      # --noinput: over ssh there is no stdin, so a leftover test database makes
      # the runner prompt "Type 'yes' to delete…", EOF, and abort ~4s in having
      # run nothing. A suite invoked non-interactively must never be able to block.
      SUITE_RAW=$($RUN "docker exec $CONTAINER ./manage.py test --noinput 2>&1") || SUITE_RC=$?
      SUITE_RC=${SUITE_RC:-0}
      echo "$SUITE_RAW" | tail -3 | sed 's/^/    /'
      CURRENT=$(echo "$SUITE_RAW" | grep -E '^(FAIL|ERROR): ' | sed -E 's/^(FAIL|ERROR): //' | sort -u)

      # ⚠ Zero failing tests has TWO causes and they must not look alike: every
      # test passed, or nothing ran. Without this guard an aborted run produces
      # an empty set, --write-baseline records it as PASS, and the next real run
      # reports every known failure as a new regression. Assert the presence of
      # a result line, not merely the absence of failures.
      RANLINE=$(echo "$SUITE_RAW" | grep -Eo '^Ran [0-9]+ tests?' | tail -1)
      # Which branch the container is ACTUALLY running. The repo is bind-mounted
      # at /app, and deploy.sh resets in place, so HEAD stays on the branch name.
      DEPLOYED_BRANCH=$($RUN "docker exec $CONTAINER git -C /app rev-parse --abbrev-ref HEAD" 2>/dev/null | tr -d '\r' | tail -1)
      DEPLOYED_COMMIT=$($RUN "docker exec $CONTAINER git -C /app rev-parse --short HEAD" 2>/dev/null | tr -d '\r' | tail -1)
      if [ -z "$RANLINE" ]; then
        # Name the cause where we can. Two have bitten so far and they look
        # identical from the id set alone: 137 is the container going away under
        # a concurrent deploy (`docker exec` reports its container's death as its
        # own), and an EOFError is the runner prompting for a stale test database
        # with no stdin to answer it. Both produce zero ids and neither is a
        # green suite.
        case "$SUITE_RC" in
          137) WHY=" — killed (137): the container was recreated under it, most likely a concurrent deploy" ;;
          0)   WHY=" — exited 0 without running: check for a prompt it could not answer" ;;
          *)   WHY=" — exit $SUITE_RC" ;;
        esac
        bad "test suite did not run (no 'Ran N tests' line)$WHY — nothing was compared"
        echo "$SUITE_RAW" | tail -5 | sed 's/^/      /'
      elif [ "$WRITE_BASELINE" -eq 1 ]; then
        COUNT=$(printf '%s\n' "$CURRENT" | grep -c .)
        # An empty baseline is legitimate only if the run also reported OK. "Ran
        # 405 tests" plus an abort partway is not a green suite.
        if [ "$COUNT" -eq 0 ] && ! echo "$SUITE_RAW" | grep -qE '^OK( |$)'; then
          bad "refusing to write an empty baseline: $RANLINE but no OK — the run did not finish cleanly"
        else
          # A baseline with no provenance is a claim you cannot check: 40 bare
          # test ids say nothing about which code produced them, so the file is
          # equally plausible-looking on any branch and equally wrong on all but
          # one. The header is what lets the comparison below refuse.
          {
            echo "# WHG failing-test baseline — DO NOT hand-edit; regenerate with --write-baseline."
            echo "#"
            echo "# These ids are the tests already failing on ONE branch at ONE commit. They are"
            echo "# meaningless against any other, so smoke_test.sh refuses to compare across"
            echo "# branches rather than reporting a different failing set as regressions."
            echo "#"
            echo "# baseline-branch: ${DEPLOYED_BRANCH:-unknown}"
            echo "# baseline-commit: ${DEPLOYED_COMMIT:-unknown}"
            echo "# baseline-target: $TARGET"
            echo "# baseline-date:   $(date -u +%Y-%m-%dT%H:%MZ)"
            echo "# baseline-$RANLINE"
            echo "#"
            printf '%s\n' "$CURRENT"
          } > "$SUITE_BASELINE"
          ok "wrote $COUNT failing-test id(s) to $SUITE_BASELINE"
          echo "    measured on ${DEPLOYED_BRANCH:-unknown}@${DEPLOYED_COMMIT:-unknown} ($RANLINE)"
        fi
      elif [ ! -f "$SUITE_BASELINE" ]; then
        # Never pass by default. A missing baseline means this check compared
        # nothing, and saying so is the whole point.
        bad "no baseline at $SUITE_BASELINE — run again with --write-baseline to seed it (review the list before committing it)"
      else
        BASELINE_BRANCH=$(sed -n 's/^# baseline-branch:[[:space:]]*//p' "$SUITE_BASELINE" | head -1)
        BASELINE_COMMIT=$(sed -n 's/^# baseline-commit:[[:space:]]*//p' "$SUITE_BASELINE" | head -1)
        # An unheaded file predates this guard, or was hand-made. Either way
        # nothing says what it measured, so it cannot be trusted to say anything.
        if [ -z "$BASELINE_BRANCH" ]; then
          bad "$SUITE_BASELINE has no provenance header, so there is no way to tell which"
          echo "      branch it was measured on. Regenerate it with --write-baseline." >&2
        elif [ -n "$DEPLOYED_BRANCH" ] && [ "$BASELINE_BRANCH" != "$DEPLOYED_BRANCH" ]; then
          bad "baseline was measured on '$BASELINE_BRANCH' but '$DEPLOYED_BRANCH' is deployed"
          echo "      A different branch has a different failing set, so every difference would" >&2
          echo "      be reported as a regression and none of them would be one. Regenerate" >&2
          echo "      with --write-baseline, or deploy $BASELINE_BRANCH before comparing." >&2
        else
        echo "    baseline: ${BASELINE_BRANCH}@${BASELINE_COMMIT:-?} vs deployed ${DEPLOYED_BRANCH:-?}@${DEPLOYED_COMMIT:-?}"
        BASE_IDS=$(grep -vE '^\s*(#|$)' "$SUITE_BASELINE" | sort -u)
        NEW=$(comm -13 <(printf '%s\n' "$BASE_IDS") <(printf '%s\n' "$CURRENT"))
        GONE=$(comm -23 <(printf '%s\n' "$BASE_IDS") <(printf '%s\n' "$CURRENT"))
        if [ -n "$NEW" ]; then
          bad "$(printf '%s\n' "$NEW" | grep -c .) NEW failing test(s) since baseline:"
          printf '%s\n' "$NEW" | sed 's/^/      + /'
        else
          ok "no new failing tests (baseline: $(printf '%s\n' "$BASE_IDS" | grep -c .) known)"
        fi
        if [ -n "$GONE" ]; then
          # Not a failure, but it must be visible: a baseline that silently keeps
          # entries for tests that now pass drifts back into meaninglessness.
          warn "$(printf '%s\n' "$GONE" | grep -c .) baseline failure(s) no longer failing — refresh with --write-baseline:"
          printf '%s\n' "$GONE" | sed 's/^/      - /'
        fi
        fi
      fi
    fi
  fi
fi

# ── Summary ──────────────────────────────────────────────────────────────────
hd "Summary"
printf '  PASS=%d  FAIL=%d  WARN=%d\n' "$PASS" "$FAIL" "$WARN"
[ "$FAIL" -eq 0 ] && { echo "  ✅ smoke test passed"; exit 0; } || { echo "  ❌ smoke test found failures"; exit 1; }
