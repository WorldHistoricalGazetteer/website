# Dataset / Print-Gazetteer Leads Tracker — overview for discussion

> **Status:** working prototype, live on the **dev server**. Nothing here is final —
> this note is for colleagues to read **before the meeting** so we can spend the
> time on decisions rather than background.
> **Related:** [`dataset-leads-plan.md`](./dataset-leads-plan.md) (the fuller technical plan).

## 1. The problem

We need a successor to the old Trello board for tracking *leads* on candidate
datasets and print gazetteers — what's been suggested, by whom, its status,
priority, and the rights/cleaning effort involved. The email thread floated
Airtable, OJS, and Zotero.

## 2. Proposed approach — two layers, not one tool

1. **Bibliography layer → a shared Zotero group library.** These leads are
   literally bibliographic records (author, year, repository, access URL).
   Zotero's web connector grabs Internet Archive / HathiTrust catalogue records
   in one click, and Palak already uses Zotero.
   - *Storage convention: **link** to the source (IA/HathiTrust URL), don't
     **attach** the scanned PDF. Metadata is free; only file attachments count
     against Zotero's 300 MB free tier. So we stay free, no subscription.*
2. **Triage / workflow layer → the WHG site (this prototype).** Status, assignee,
   priority, provenance, and public intake. It lives on our own infrastructure at
   Pitt (no new subscription) and does two things a SaaS tool can't:
   - **Pull from the Zotero group library** so the bibliography and the tracker
     stay in sync.
   - **Auto-compute a "WHG gap value"** by querying our live `places` index, so
     priority isn't purely eyeballed.

**Suggested division of truth:** Zotero is the bibliographic source; the WHG app
owns workflow (status/assignee/priority). Writing *back* to Zotero is deferred.

## 3. What's already built and live on the dev server

- **Triage board = Django Admin** → *Leads › Dataset leads*. Filter by status /
  provenance / scan status / assignee; edit status, priority, and assignee inline
  from the list; bulk actions (Approve, Reject, recompute gap value, sync from
  Zotero).
- **72 candidate rows already seeded** from the candidate-bibliography spreadsheet,
  so the board starts with real data (all currently *In triage*).
- **The two fields Ruth asked for:**
  - **Lead provenance** — own research vs community recommendation vs public form,
    plus *who* recommended it.
  - **Priority score** — composed from **gap-filling value × difficulty**
    (cleaning, rights, etc.).
- **Public suggestion form** at `/leads/suggest/`, reachable from the new
  **Contribute** menu in the site header. No login required (in production).
  Submissions arrive as *Suggested* / *Public form*. Light spam guards: a hidden
  "spam-trap" field (see note below) and a simple per-IP rate limit.

### The "honeypot" / spam-trap field, briefly

The public form includes one extra input that is **hidden from people** (moved
off-screen by CSS) but still present in the page. Real users never see or fill
it; automated spam bots fill in every field they find. If that hidden field
comes back with anything in it, we treat the submission as a bot and drop it.
It's a zero-friction alternative to a CAPTCHA — nothing for genuine users to do.
We can add a CAPTCHA later if bots get clever (see decision #4).

## 4. Decisions / inputs we need from the group

1. **Zotero group library + a read-only API key** — who owns/creates it
   (likely Palak)? This is the blocker for the sync layer.
2. **Definition of "gap value"** — region, period, or language thinness? This
   drives the auto-priority metric.
3. **Should accepted leads be publicly browsable**, or admin-only for now?
4. **Spam-protection appetite** for the public form — hidden-field trap only, or
   add a CAPTCHA?
5. **Confirm the division of truth** in §2 (Zotero = bibliography, WHG = workflow,
   write-back deferred).

## 5. Roadmap after this meeting

In order, each unblocked by the decisions above:

1. **Zotero read-sync** — pull the group library and upsert leads (needs #1).
2. **Gap-value automation** — query the `places` index for coverage (needs #2).
3. *(Optional)* **Zulip ping** on each new public submission, so triage isn't
   pull-only.
4. **Public browse** of accepted leads (needs #3).

## 6. How to try it (≈2 minutes)

1. Log into the **dev server** → `/admin/` → **Leads › Dataset leads**: the 72
   rows, with filters, inline status/priority editing, and bulk actions.
2. In the site header: **Contribute › Suggest a dataset** → fill it in → submit →
   thank-you page. *(The public form is open to everyone on dev; the rest of the
   dev site stays restricted to logged-in staff.)*
3. Back in Admin: the new row appears as *Suggested* / *Public form* — the
   public-intake → triage loop, end to end.
