#!/usr/bin/env bash
#
# snag-diagnostics.sh — gather open `beta-snag` GitHub issues together with the GlitchTip error records
# for each report's beta session, so snags can be triaged with their diagnostic data in one view.
#
# Beta testers file snags via the on-site form (place#115); each report embeds a per-browser-session id
# (e.g. `wb-cf1f6eb8b972`). The beta-diag pipeline tags GlitchTip client+server errors with the same
# `beta_session`, so this script joins the two: for every open snag it prints the report metadata and the
# correlated GlitchTip events. A snag with NO events is almost always a UX/behavioural issue (nothing
# threw) — diagnose those from the description + code, not GlitchTip.
#
# Usage:  server-admin/snag-diagnostics.sh [--state open|closed|all] [--days N] [--repo OWNER/REPO]
# Needs:  gh (authenticated) + ssh access to the `whg` host (self-hosted GlitchTip postgres).
# Read-only: only SELECTs against GlitchTip; never writes.

set -euo pipefail

REPO="WorldHistoricalGazetteer/place"
STATE="open"
DAYS=60
SSH_HOST="whg"

while [ $# -gt 0 ]; do
  case "$1" in
    --repo)  REPO="$2"; shift 2 ;;
    --state) STATE="$2"; shift 2 ;;
    --days)  DAYS="$2"; shift 2 ;;
    --host)  SSH_HOST="$2"; shift 2 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

command -v gh >/dev/null || { echo "gh CLI not found / not on PATH" >&2; exit 1; }

echo "── beta-snag triage · repo=$REPO · state=$STATE · GlitchTip window=${DAYS}d ──"
echo

# 1. Pull the snags as TSV: number, severity, session id, title — worst severity first. Every field is
#    non-empty ('-' placeholder for a missing severity/session) so tab-splitting can't shift columns.
#    Severity from a `sev:*` label; session from the "**session:** `wb-…`" line in the body.
issues_tsv="$(gh issue list --repo "$REPO" --label beta-snag --state "$STATE" --limit 200 \
  --json number,title,body,labels \
  --jq '
    def sev:  ([.labels[].name | select(startswith("sev:")) | ltrimstr("sev:")] | first) // "-";
    def rank(x): {"major":0,"minor":1,"cosmetic":2}[x] // 9;
    def sess: (((.body // "") | [scan("wb-[0-9a-f]{6,}")] | first) // "-");
    [ .[] | {n:.number, s:sev, sess:sess, t:(.title|gsub("[\t\n]";" ")) } ]
    | sort_by(rank(.s))[]
    | [(.n|tostring), .s, .sess, .t] | @tsv
  ')"

if [ -z "$issues_tsv" ]; then echo "No $STATE beta-snag issues found."; exit 0; fi

# 2. Collect the distinct session ids and fetch all their GlitchTip events in ONE query.
sessions="$(printf '%s\n' "$issues_tsv" | cut -f3 | grep -E '^wb-' | sort -u || true)"

events_dump=""
if [ -n "$sessions" ]; then
  arr="$(printf "'%s'," $sessions | sed 's/,$//')"   # -> 'wb-a','wb-b'
  events_dump="$(ssh -o ConnectTimeout=20 "$SSH_HOST" \
    "docker exec -i glitchtip_postgres psql -U glitchtip -d glitchtip -F '|' -tA" <<SQL || true
SELECT tags->>'beta_session',
       to_char(received,'YYYY-MM-DD HH24:MI'),
       CASE level WHEN 50 THEN 'fatal' WHEN 40 THEN 'error' WHEN 30 THEN 'warning' ELSE level::text END,
       COALESCE(data->'sdk'->>'name',''),
       COALESCE(NULLIF(transaction,''),'—'),
       left(regexp_replace(title, E'[\\n\\r|]+', ' ', 'g'), 90)
FROM issue_events_issueevent
WHERE received > now() - interval '${DAYS} days'
  AND tags->>'beta_session' = ANY(ARRAY[${arr}])
ORDER BY tags->>'beta_session', received DESC;
SQL
)"
fi

# 3. Per-snag report, worst severity first.
printf '%s\n' "$issues_tsv" | while IFS=$'\t' read -r num sev session title; do
  echo "▌ #$num  [sev:$sev]  $title"
  if [ "$session" = "-" ]; then
    echo "    session: (none in report) · no GlitchTip lookup possible"
  else
    echo "    session: $session"
    matched="$(printf '%s\n' "$events_dump" | awk -F'|' -v s="$session" '$1==s')"
    if [ -z "$matched" ]; then
      echo "    GlitchTip: no correlated events in ${DAYS}d — likely UX/behavioural (nothing threw); diagnose from the report + code."
    else
      n=$(printf '%s\n' "$matched" | grep -c . || true)
      echo "    GlitchTip: $n correlated event(s) —"
      printf '%s\n' "$matched" | while IFS='|' read -r _s ts lvl sdk txn etitle; do
        printf '      %s  %-7s %-20s %-22s %s\n' "$ts" "$lvl" "${sdk#sentry.}" "$txn" "$etitle"
      done
    fi
  fi
  echo "    https://github.com/${REPO}/issues/${num}"
  echo
done

echo "── done. Reproduce a session in the GlitchTip UI with:  beta_session:<wb-…>  (errors.whgazetteer.org) ──"
