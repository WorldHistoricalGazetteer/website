                   # CLAUDE.md — Codebase Guide for AI Agents

## Project Overview

**World Historical Gazetteer (WHG)** — a Django 4.1.7 web platform for linking, visualising, and sharing data about historical places. Python 3.10+, PostgreSQL 15/PostGIS 3.4, Elasticsearch 8, Celery 5.3+, Redis.

**Repository:** `whg3` (the `website` repo on GitHub at `WorldHistoricalGazetteer/website`)

## Architecture

### Stack
- **Backend:** Django 4.1.7 + Django REST Framework 3.14 + Celery
- **Database:** PostgreSQL 15 with PostGIS 3.4 (via `psycopg2-binary`)
- **Search:** Elasticsearch 9 hosted on Pitt CRC
- **Frontend:** Bootstrap 5.2, jQuery 3.6, Webpack 5 (bundles to `static/webpack/`)
- **Mapping:** MapLibre GL JS, Turf.js
- **Task queue:** Celery + Redis (broker), django-celery-results (backend)
- **Error tracking:** Sentry/GlitchTip
- **Analytics:** Plausible (self-hosted)

### Key Settings
- `settings.ES_CONN` — pre-configured `elasticsearch8.Elasticsearch` instance (auth included); defined in `local_settings.py`
- `settings.CRC_GATEWAY_URL` — CRC ES gateway at `http://index.whgazetteer.org:9200`
- `settings.APP_VERSION` — read from `VERSION` file (currently `3.5-beta`)
- Base template: `main/templates/main/base_webpack.html` (all pages extend this)
- Static files: `STATIC_ROOT = <BASE_DIR>/static`, Webpack output → `static/webpack/`
- Templates: `APP_DIRS=True`, plus `main/templates`, `whgmail/templates`, `templates/`

### ES Indexes
- **`places`** — 47M+ place records from GeoNames, Wikidata, OSM, OHM, TGN, Pleiades, etc.
- **`types`** — ~5,800 AAT place-type concepts with cross-vocabulary mapping fields (`gn_fcodes`, `wd_qids`, `osm_tags`, `ohm_tags`, `pleiades_types`)
- **`pub`** / **`whg`** — publication/reconciliation indexes

### URL Structure
URLs are defined in `whg/urls.py`. Key app mounts:
- `/` — home (`main`)
- `/api/` — REST API (`api`)
- `/datasets/` — dataset management
- `/collections/` — collection management
- `/places/` — place detail views
- `/search/` — search UI
- `/types/` — placetypes app (AAT type tree + mapping UI)
- `/validation/` — dataset validation

## Django Apps

| App | Purpose |
|-----|---------|
| `accounts` | Auth, ORCiD OIDC login, profile |
| `api` | REST API, reconciliation, CRC gateway client |
| `areas` | Geographic study areas |
| `collection` | Place collections |
| `datasets` | Dataset upload, management, publishing |
| `elastic` | ES utilities |
| `ingestion` | Data ingestion pipeline |
| `main` | Home page, dashboard, static pages, base templates |
| `periods` | Historical period management |
| `persons` | Person records |
| `places` | Place model, detail views |
| `placetypes` | AAT type hierarchy, type-tree widget, type mapping UI |
| `regions` | Region definitions |
| `resources` | Teaching resources |
| `search` | Search UI |
| `sitemap` | Sitemap generation |
| `traces` | Trace/annotation records |
| `users` | Custom user model |
| `validation` | Dataset validation |

## Frontend / Webpack

- Entry points defined in `webpack.config.js` under `entry: { ... }`
- Source JS: `whg/webpack/js/*.js`
- Output: `static/webpack/*.bundle.js` + `*.bundle.css`
- CDN fallbacks copied to `static/webpack/CDNfallbacks/`
- jQuery is an external (loaded via CDN/fallback, not bundled)
- Bootstrap 5.2 loaded via CDN in base template
- For app-specific JS/CSS not using Webpack: use `{% block extra_head %}` or `{% block deferredScripts %}` in templates, or put files in `<app>/static/<app>/` and use `{% static %}` tag

## Code Conventions

- Python: PEP 8, Django conventions
- Templates: extend `main/base_webpack.html`, use Bootstrap 5 classes
- AJAX: use `fetch()` with CSRF token from `<meta name="csrf-token">` tag
- ES queries: always use `settings.ES_CONN` (never construct new clients)
- Views: function-based views preferred for simple endpoints; class-based for complex ones
- Auth: `@login_required` for protected views; `user.is_authenticated` in templates
- Logging: use `logging.getLogger(__name__)` or named loggers defined in settings

## Testing

- Run tests: `python manage.py test`
- Test settings: `CELERY_ALWAYS_EAGER = True` when `'test' in sys.argv`

## Development Setup

1. Copy `whg/local_settings_template.py` → `whg/local_settings.py` and configure DB, ES, etc.
2. `pip install -r requirements.txt`
3. `npm install && npm run build` (or `npm run watch` for dev)
4. `python manage.py migrate`
5. `python manage.py runserver`

## Docker

- `Dockerfile` + `docker-compose-autocontext.yml` available
- Compose services: web, db (postgres), redis, celery worker

