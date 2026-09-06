# Phonetic rule review (`/phonetics/`) — place#252

Reviewers correct the Epitran grapheme→IPA rule sets that turn a place name into
IPA for cross-script matching. On **staging/dev**; not on prod.

## The one thing to know first

**Nothing in this app edits a rule set.** The CSVs live in
`WorldHistoricalGazetteer/indexing` and change there. Django mirrors them and
records *proposals*; installing anything is a separate, deliberate act upstream.

## Two blockers before launch

1. **`panphon==0.22.0` is in `requirements.txt` but not in the Docker image.**
   Dev is running an *ephemeral* `pip install` inside the container, which dies
   on the next `recreate`. A new image tag is needed — and as of 2026-09-06 the
   image **cannot be built**: `python:3.10.7-slim-bullseye`'s apt sources fail
   because Debian 11's security *index* still advertises package versions whose
   pool objects have been removed (404 on `deb.debian.org` and on
   `archive.debian.org` alike). That blocks every future dependency change, not
   just this one. Options are pinning apt at an archive snapshot (no more
   security updates) or moving the base image to bookworm (GDAL 3.2 → 3.6).
   Neither should be chosen quietly.
   The app boots without panphon — the import is lazy — and only IPA validation
   fails.

2. **Contribution terms are seeded as a DRAFT** (`2026-09-draft`, CC BY 4.0,
   `signed_off=False`). `PHONETICS_PUBLIC=True` alone will **not** open the app:
   `phonetics/visibility.py` also requires `signed_off`. That is
   non-negotiable 6 of the issue, enforced rather than remembered.

## Running the tests without a dev database

`manage.py test phonetics` needs PostGIS. In the dev container it just works.
Locally, a throwaway container plus a settings shim:

```bash
docker run -d --name whg-phon-test -e POSTGRES_PASSWORD=test -e POSTGRES_USER=test \
  -e POSTGRES_DB=test -p 55432:5432 postgis/postgis:15-3.4
cat > whg/test_local_settings.py <<'PY'
from whg.settings import *  # noqa
DATABASES = {'default': {'ENGINE': 'django.contrib.gis.db.backends.postgis',
                         'HOST': '127.0.0.1', 'NAME': 'test', 'USER': 'test',
                         'PASSWORD': 'test', 'PORT': 55432}}
PY
python3 manage.py test phonetics --settings=whg.test_local_settings
```

The shim is deliberately **not** committed: it names a container that only
exists on whoever's machine created it.

## Operations

| Command | When |
|---|---|
| `manage.py sync_epitran_rules` | Pull rule sets from `indexing`. Also a Celery task (`phonetics.sync_rulesets`) and a staff "sync now" button. Two API calls when nothing changed. |
| `manage.py lint_epitran_rules` | Report machine-detectable defects. `--fail-on-defect` for CI. |
| `manage.py lint_epitran_rules --recompute` | **After changing the lint rules.** The sync is content-addressed, so a file whose bytes have not moved is never re-read and its cached verdicts never change. |
| `manage.py import_corpus_stats --path stats.json` | Per-rule corpus frequencies + worked examples from the indexing side. |
| `manage.py phonetics_build_iso_table` | Regenerate the bundled ISO/autonym table (needs `pycountry` + `langcodes`, dev only). |

`/phonetics/suggestions.json` (staff) is the machine-readable feed of logged
suggestions for agents working in `indexing`. They need not report back what
they applied: the next sync notices a value that now equals a standing proposal
and stamps it adopted.

## Things that will look like bugs and are not

- **A rule set's identity is its `slug`, not its `code.`** `mya-Mymr` exists both
  as a shipped list and as a WHG draft proposing to replace it
  (`mya-Mymr.draft`), with separate review histories. Keying on the code would
  let a draft sync overwrite live rows and re-point every review made against
  them.
- **A value that is only a modifier is valid.** PanPhon finds no segment in `ː`
  or `̃`, because on their own they are not segments. 22 shipped rows are this.
- **Everything is NFD.** Composed `ẽ` is reported lossy and decomposed `ẽ` is
  not; and Gurmukhi `ਸ਼` precomposed vs decomposed will not merge under NFC.
- **Unreviewed rows export unchanged, and so do disputed ones.** Arbitrating a
  disagreement would resolve by algorithm the thing this app collects.

## Defect count, for the record

The issue says 108 defects in 42 files. The true figure is **81 in 38** for the
shipped set (84 in 41 including the drafts). 5 of the difference were
precomposed vowels flagged by linting before NFD; 22 were modifier-only rows.
Both were independently reproduced by `indexing-db` and #251 is being corrected.

⚠ And the sharpest thing to come out of that: Burmese `ှ → ʰ` **passes every
automatic check** — a lone modifier is legitimate — and is **wrong**, because
ha-hto on a sonorant marks devoicing rather than aspiration. It affects 9,647
names. The row that most needed a human was the row the machine passed, which is
why every clean row in the UI says so rather than leaving silence to be read as
approval.
