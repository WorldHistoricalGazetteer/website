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
   Pitt (no new subscription) and isn't a *SaaS* product — **SaaS** ("Software as a
   Service") means a hosted, subscription tool someone else runs for us (Airtable,
   Trello, etc.). Because this layer is our own code, it can do something a rented
   tool can't: **read directly from the Zotero group library** so the bibliography
   and the tracker stay in step, rather than us re-keying records into a second
   place.

**Suggested division of truth:** Zotero is the bibliographic source; the WHG app
owns workflow (status/assignee/priority). Writing *back* to Zotero is deferred.

### How the Zotero sync would work

It's **one-directional: Zotero → WHG**, and it never overwrites our triage data.

- **What's synced:** the *bibliographic* fields only — title, author/compiler,
  date, source URL, repository, tags. Each WHG lead remembers the Zotero item it
  came from (its item key), so re-syncs match existing leads instead of
  duplicating them.
- **What's preserved:** everything the team owns — *status, assignee, priority,
  provenance, notes*. A sync refreshes the citation details and **leaves workflow
  untouched** ("augment, don't overwrite").
- **New items** in the Zotero library become new leads with status *Suggested*;
  items already linked just get their citation fields refreshed.
- **When:** on demand to begin with (a "sync from Zotero" button/command), and —
  once we're happy with it — on a schedule (e.g. nightly) so it's hands-off.
- **Not synced back:** we do **not** push WHG changes into Zotero (deferred), so
  there's no risk of the tracker clobbering anyone's library.

## 3. What's already built and live on the dev server

- **Triage board = Django Admin** → *Leads › Dataset leads*. Filter by status /
  provenance / scan status / assignee; edit status, priority, and assignee inline
  from the list; working bulk actions to **Approve** / **Reject** selected leads.
  (Two more actions — *sync from Zotero* and *recompute gap value* — are stubbed
  in the menu as placeholders for the roadmap items below; they don't do anything
  yet.)
- **72 candidate rows already seeded** from the candidate-bibliography spreadsheet,
  so the board starts with real data (all currently *In triage*).
- **The two fields Ruth asked for:**
  - **Lead provenance** — own research vs community recommendation vs public form,
    plus *who* recommended it.
  - **Priority score** — set by a reviewer, weighing how well a source **fills a
    gap** in our coverage against the **difficulty** of using it (cleaning, rights,
    etc.). For now these are human judgements; see the note on gap value below.
- **Public suggestion form** at `/leads/suggest/`, reachable from the new
  **Contribute** menu in the site header. No login required (in production).
  Submissions arrive as *Suggested* / *Public form*. Light spam guards for
  anonymous users: a hidden "spam-trap" field (see note below) and a simple
  per-IP rate limit. **Logged-in users skip both** — they're trusted, so the form
  has no anti-spam friction, and their submission is automatically linked to their
  account.

### The "honeypot" / spam-trap field, briefly

The public form includes one extra input that is **hidden from people** (moved
off-screen by CSS) but still present in the page. Real users never see or fill
it; automated spam bots fill in every field they find. If that hidden field
comes back with anything in it, we treat the submission as a bot and drop it.
It's a zero-friction alternative to a CAPTCHA — nothing for genuine users to do.
We can add a CAPTCHA later if bots get clever (see decision #4).

**This only affects anonymous submitters.** Logged-in users are trusted: the
spam-trap and rate limit are bypassed entirely (and any future CAPTCHA would be
too), so decision #4 is really only about *anonymous* submissions.

## 4. Decisions / inputs we need from the group

1. **Zotero group library + a read-only API key** — who owns/creates it
   (likely Palak)? This is the blocker for the sync layer.
2. **"Gap value" — is it worth automating at all?** The idea of having the app
   score how well a source fills a coverage gap (by querying our `places` index)
   is appealing, but it's **not obvious it can be done well** without brittle,
   over-specific rules — "fills a gap" could mean region, period, or language
   thinness, and each is hard to measure fairly. **Default position: keep gap
   value a human judgement** that a reviewer records, and only revisit automation
   later if someone can describe a metric that's both simple and meaningful.
   Question for the group: is there a *narrow, well-defined* version worth trying?
3. **Should accepted leads be publicly browsable**, or admin-only for now?
4. **Spam-protection appetite** for *anonymous* submissions — hidden-field trap
   only, or add a CAPTCHA? (Logged-in users already skip all of this.)
5. **Confirm the division of truth** in §2 (Zotero = bibliography, WHG = workflow,
   write-back deferred).

## 5. Roadmap after this meeting

1. **Zulip notification on every new submission** *(planned — agreed)*. As soon
   as someone submits the public form, post a message to a Zulip channel with the
   lead's title and submitter and a **direct link to its review screen** in the
   admin, so triage is push, not pull — nobody has to remember to check the board.
2. **Zotero read-sync** — pull the group library and upsert leads (needs #1 in the
   decisions list above).
3. **Public browse** of accepted leads (needs decision #3).
4. *(Only if the group wants it)* a **gap-value helper** — and only for a narrow,
   well-defined metric (see decision #2). Otherwise gap value stays a field a
   reviewer fills in by hand.

## 6. How to try it (≈2 minutes)

1. Log into the **dev server** → `/admin/` → **Leads › Dataset leads**: the 72
   rows, with filters, inline status/priority editing, and bulk actions.
2. In the site header: **Contribute › Suggest a dataset** → fill it in → submit →
   thank-you page. *(The public form is open to everyone on dev; the rest of the
   dev site stays restricted to logged-in staff.)*
3. Back in Admin: the new row appears as *Suggested* / *Public form* — the
   public-intake → triage loop, end to end.
