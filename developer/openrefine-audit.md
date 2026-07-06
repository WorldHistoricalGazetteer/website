# OpenRefine feature audit → Gazetteer Workbench UI suggestions

Requested by Stephen, 2026-07-06. A deliberate pass over OpenRefine's feature set, asking for each:
**does the Workbench want it, and how should it look in our UI?** The Workbench already *exceeds*
OpenRefine on reconciliation itself (in-browser phonetic Symphonym matching, multi-column containment
chaining, a map review with geometry editing, per-row individual matching). The gaps are mostly in
**data cleaning, faceting, and history** — the parts of OpenRefine that make a messy table tractable
*before* and *around* reconciliation.

## Feature-by-feature

| OpenRefine feature | Want it? | Workbench UI suggestion |
|---|---|---|
| **Faceting & filtering** (text / numeric / timeline / custom facets; text search) | **Yes — top priority** | A **facet sidebar** on the results + review panes: facet by reconciliation **status** (matched / auto / accepted / no-match / pending), **source namespace**, **score band**, **ccode**, **feature type**, and a **date-range** facet; free-text search over names. Facets drive *which rows you review* — the single biggest usability win for thousand-row datasets. |
| **Clustering** (key-collision, nearest-neighbour — to merge variant spellings) | **Adapt, don't copy** | We deliberately do *not* merge rows. But a per-column **"cluster & clean"** tool (powered by our Symphonym embeddings or edit distance) to normalise *data-entry* variants in the source values (e.g. "Newton" / "Neuton") is genuinely useful — a **cleaning** step, clearly separate from reconciliation. Opt-in, applied as a cell transform. |
| **Cell transforms / GREL** (trim, case, split, find/replace, expressions) | **Yes (light first)** | A **column menu** with one-click common transforms (trim whitespace, collapse spaces, change case, split on delimiter) plus **find & replace** (regex optional). A full expression language is a bigger bet — the plan already flags **JSONata** as the transform substrate (§1d); start with the common ops and grow toward an expression editor. |
| **Reconciliation** (against services) | **Core — we lead here** | Nothing to copy wholesale; we're ahead. Worth borrowing OpenRefine's crisp **judgment facets** and its **keyboard-first accept** (we already have both). |
| **Add column from reconciled values** (pull coords / IDs / types into columns) | **Yes — we have it** | This is our **Enrich from WHG** (export columns). Improvement: surface the enriched columns **in the live preview**, not only at export, so users see the added data as they work. |
| **Undo / Redo history** (full reproducible operation log) | **Yes (scoped)** | Add an **undo stack + operation history** for cell transforms and role changes (we already have per-decision undo in review). A fully reproducible/replayable history (OpenRefine's `.json` recipe) is a larger investment — worthwhile later for trust and for **collaborative** editing (#112). |
| **Sort & column reorder / hide-show** | **Partly have; extend** | Make the **results table sortable** (by score, status, name). We already have an ignore-columns show/hide toggle; add drag-reorder if cheap. |
| **Import wizard** (encoding, header row, skip lines, separators, quote char, JSON path) | **Yes (as override)** | We auto-detect delimiter and parse. Add an **"import options"** disclosure for messy files: header-row toggle, delimiter/quote/encoding override, skip *N* lines, and a **JSON path** for nested JSON. Keep auto as the default; expose the overrides only when needed. |
| **Rows vs Records mode** (group related rows) | **Skip** | Our per-row model is deliberate (users have pre-disambiguated). Not needed. |
| **Templated export** (custom text/JSON templates) | **Skip** | Our fixed CSV / JSON / LPF exporters cover the need; LPF is the WHG upload path. A column-picker for CSV is the only likely nicety. |
| **Common cell edits** (bulk edit one value → all cells; mass edit) | **Yes (light)** | A **"edit all cells with value X"** action from the column/cell menu — cheap, high-value for tidying admin names before the parent-column reconciliation. |
| **Data-package metadata / provenance** | **Adapt** | Ties into contribution: capture dataset **title / source / creator / licence** once (we already have a citation/licensing initiative) and carry it into the LPF/Contribute step. |

## Prioritised suggestions

1. **Facet/filter sidebar** on results + review (status · score · ccode · feature-type · date-range · text). *Biggest win for large tables.*
2. **Per-column quick transforms + find/replace** (trim, case, split, replace) — light cleaning before reconciling.
3. **Sortable results table** (score / status / name).
4. **Import-options override** (header row, delimiter/quote/encoding, skip lines, JSON path).
5. **Undo/history** for transforms + role changes (foundation for reproducibility and #112 collaboration).
6. **Column "cluster & clean"** (Symphonym-powered) for source spelling variants — reuses the embedding worker we already ship.
7. **Enriched columns visible in the preview**, not just at export.
8. **Bulk "edit all cells = value"** and a **CSV column-picker** — small, cheap wins.

## Notes
- Items 1–3 are the highest-leverage and fit our existing panes with modest work.
- Item 6 reuses the in-browser Symphonym worker (already built for phonetic reconciliation) — cheap incremental value.
- A full reproducible **operation history** (item 5 at its most ambitious) is worth deferring until the
  collaborative feature (#112) forces the question, since shared editing needs an operation log anyway.
