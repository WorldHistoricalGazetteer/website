# Phase 2 — CRediT contributions

**Parent design:** `developer/plan-citations-licences-credit.prompt.md` (§3.3 Contribution model, §4.1 CSL, §4.2 DataCite, §4.6 submission UI). Implements issue [place#99](https://github.com/WorldHistoricalGazetteer/place/issues/99).
**Goal:** model attribution as first-class, role-tagged contributions (CRediT) that can attach to datasets, collections, *and* gazetteer-registry entries, replacing the free-text `creator`/`contributors` split as the source of truth.

`persons.Person` already carries `orcid` + `affiliation` (first-class), so no Person change is needed — Phase 2 builds on it.

## Split (foundation first; the wiring touches production rendering/DOI)

### Phase 2a — foundation (this PR; additive, low-risk)
A new `persons.CreditRole` vocabulary + `persons.Contribution` through-model, admin, tests. **No change to existing CSL/DOI/templates**, so nothing in the live render/DOI path moves yet.

- **`CreditRole`** — `TextChoices` of the 14 NISO CRediT roles, values = canonical CRediT slugs (so `https://credit.niso.org/contributor-roles/<slug>/` is derivable):
  `conceptualization, data-curation, formal-analysis, funding-acquisition, investigation, methodology, project-administration, resources, software, supervision, validation, visualization, writing-original-draft, writing-review-editing`.
- **`Contribution`** — generic-relation through-model:
  - `person` FK → `persons.Person` (PROTECT, related_name `contributions`)
  - `role` (CreditRole), `degree` (lead/equal/supporting, blank), `is_corresponding` (bool), `order` (PositiveSmallInteger)
  - generic FK: `content_type` + `object_id` + `GenericForeignKey('target')` → targets `Dataset` | `Collection` | `GazetteerRegistryEntry`
  - `UniqueConstraint(person, role, content_type, object_id)`; index on `(content_type, object_id)`; `Meta.ordering = ['order']`
  - `credit_uri` property → `https://credit.niso.org/contributor-roles/{role}/`
- **Admin:** register `Contribution` (autocomplete `person`); a generic `Contribution` inline could be added to Dataset/Collection admin later.
- **Tests:** role-slug sanity, generic relation to a `Dataset`, uniqueness constraint, `credit_uri`.

Migration: `persons/0003_contribution` (depends on `persons/0002`, `contenttypes/0002`).

### Phase 2b — wiring (separate PR; changes live code paths, deploy carefully)
1. **CSL** (`utils/csl_citation_formatter.py`): read `Contribution` rows (and authority `contributors_csl`) → emit CSL `author` (creator-equivalent roles) + `contributor` arrays; **fix the `person.first/.middle/.last` bug** (use `Person.given/family/literal`); fall back to `parse_names()` only when no structured contributions exist.
2. **DataCite** (`utils/doi.py`): add a `contributors` array (currently only `creators`), `contributorType` from CRediT via the JATS4R mapping, `nameIdentifiers` (ORCID) + `affiliation`.
3. **Importer:** management command `import_freetext_contributors` — reuse `parse_names()` to seed `Person` + `Contribution` from existing `creator`/`contributors` strings (idempotent; default role mapping creator→Conceptualization, contributors→Data curation; editable after).
4. **Submission/edit UI** (§4.6): repeatable contributor rows (person + ORCID + affiliation × roles × degree × corresponding) on dataset upload/validate + collection forms, backed by `Contribution`.

## CRediT → DataCite contributorType mapping (for 2b)
Per JATS4R: most CRediT roles map to DataCite `contributorType` values (e.g. Data curation→DataCurator, Software→Software, Supervision→Supervisor, Project administration→ProjectManager, Funding acquisition→…); roles without a clean DataCite equivalent fall back to `Other` with the CRediT role recorded in `nameIdentifiers`/a note. Finalise the table in 2b against the JATS4R reference.

## Validation
`python3 manage.py test persons` in a one-off container against the dev Postgres (throwaway test_ DB), per [[reference_deploy_script]] harness. No prod migration until reviewed; Phase 2a migration is additive (new table only).

## Leapfrog
Branch `credit` off `main`. Phase 2a touches only `persons` (models/admin/migration) — additive, no collision risk. Same merge→deploy path as Phase 1 (`deploy prod pull → migrate → restart`).
