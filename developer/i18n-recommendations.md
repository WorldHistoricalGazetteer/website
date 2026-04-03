# WHG Internationalisation (i18n) — Recommendations

> **Audience:** WHG development team  
> **Date:** April 2026  
> **Scope:** Recommendations for making the WHG web interface multilingual,
> starting with the Search page.  No code changes — design guidance only.

---

## 1. Recommended Library: django-modeltranslation

**Primary recommendation: [django-modeltranslation](https://django-modeltranslation.readthedocs.io/)**
combined with Django's built-in `django.utils.translation` (i18n) framework.

### Why this combination

| Requirement | Solution |
|-------------|----------|
| **Static UI strings** (button labels, headings, tooltips, placeholder text) | Django's built-in `{% trans %}` and `{% blocktrans %}` template tags + `gettext` `.po` files |
| **User-contributed translations** (trusted users provide translations via an admin interface) | `django-modeltranslation` — adds per-language fields to Django models, editable via the Django admin or custom forms |
| **JavaScript strings** (search.js, filtersTour.js, widget labels) | Django's `JavaScriptCatalog` view (`django.views.i18n.JavaScriptCatalog`) which exposes `gettext()` / `ngettext()` in JS |
| **Django infrastructure** | All three tools are native to or designed for Django — no framework mismatch |

### Alternatives considered

| Library | Verdict |
|---------|---------|
| **django-rosetta** | Web-based `.po` file editor; good for translators who don't use Git, but does not support user-contributed model-level translations. Could complement the main approach. |
| **django-parler** | Model translation via separate translation tables (like `modeltranslation` but with a different storage strategy). Less mature, smaller community. |
| **Wagtail Localize** | Excellent if migrating to Wagtail CMS; overkill for the current Django-templates architecture. |
| **i18next (JS-only)** | Popular in React/Vue SPAs. Would require maintaining a parallel translation system outside Django. Not recommended when Django templates drive the UI. |
| **Format.js / react-intl** | React-specific. Not applicable. |

---

## 2. Architecture Overview

### 2.1 Static strings (templates + JS)

1. **Mark strings for translation** in Django templates using
   `{% load i18n %}` and `{% trans "Search" %}` /
   `{% blocktrans %}...{% endblocktrans %}`.

2. **Extract messages** with `django-admin makemessages -l fr -l de -l ar ...`
   into `locale/<lang>/LC_MESSAGES/django.po` files.

3. **Translate** the `.po` files (manually, or via a translation management
   platform like Pontoon, Weblate, or Crowdin).

4. **Compile** with `django-admin compilemessages`.

5. **JavaScript strings**: Use the `JavaScriptCatalog` view to serve
   compiled translations to the browser.  In JS code, call
   `gettext('Search')` instead of hard-coding English strings.

### 2.2 User-contributed model translations

For content that trusted users can translate (e.g. dataset descriptions,
place type labels, help text, tour step content):

1. Install `django-modeltranslation`.
2. Register models in `translation.py` files specifying which fields
   should be translatable.
3. `modeltranslation` automatically creates per-language database columns
   (e.g. `title_en`, `title_fr`, `title_ar`) and patches the model so
   that `obj.title` returns the value for the active language.
4. The Django admin automatically shows per-language fields.  Custom
   forms can also be built for trusted translators.

### 2.3 Language selection

- Add `django.middleware.locale.LocaleMiddleware` to `MIDDLEWARE`.
- Set `LANGUAGE_CODE = 'en'` as the default.
- Define `LANGUAGES` in settings (e.g. `[('en', 'English'), ('fr', 'Français'), ('ar', 'العربية')]`).
- Add a language switcher widget to the site header (e.g. a dropdown
  that POSTs to `django.views.i18n.set_language`).
- User language preference can be stored in their profile (an
  `Account.preferred_language` field).

---

## 3. Starting with the Search Page

The Search page is a good starting point because it has a bounded set
of translatable strings and exercises all three string categories:

### 3.1 Template strings (search.html)

| String | Tag |
|--------|-----|
| "Search" (input label) | `{% trans "Search" %}` |
| "Enter place name" (placeholder) | `{% trans "Enter place name" %}` |
| "Exact" (button label) | `{% trans "Exact" %}` |
| "Filters" (button label) | `{% trans "Filters" %}` |
| "data sources" / "time & space" / "place types" | `{% trans "data sources" %}` etc. |
| "clustering" | `{% trans "clustering" %}` |
| "Group linked records" | `{% trans "Group linked records" %}` |
| Tooltip texts | `{% trans "..." %}` — note: long tooltips benefit from `{% blocktrans %}` |
| Landing page text | `{% blocktrans %}...{% endblocktrans %}` |
| "No results" message | `{% trans "No results — please modify your search terms or filters." %}` |

### 3.2 JavaScript strings (search.js, regionSelector.js, etc.)

| String | Location |
|--------|----------|
| "Exact match: on/off..." | search.js tooltip updates |
| "Zoom the map first..." | search.js `disableSelectorInputs` |
| "Search for a region…" | regionSelector.js placeholder |
| "Select a region…" | regionSelector.js UN select placeholder |
| "Backend not yet connected" | Various selector stubs |
| Tour step titles and descriptions | filtersTour.js |

These should use `gettext()` from the `JavaScriptCatalog`.

### 3.3 Data-driven content

| Content | Approach |
|---------|----------|
| Authority names ("GeoNames", "Wikidata", etc.) | Template `{% trans %}` tags |
| Authority descriptions (title tooltips) | Template `{% blocktrans %}` |
| AAT place type labels | `django-modeltranslation` on the type label model, or a JSON translation file |
| UN geoscheme region names | A static JS translation map, or gettext |
| Result field labels ("Country Codes:", "Chronology:", etc.) | JS `gettext()` |

---

## 4. RTL Support

If Arabic, Hebrew, or other RTL languages are planned:

- Add `dir="auto"` or `dir="rtl"` to the `<html>` element when RTL is active.
- Bootstrap 5 has built-in RTL support via `bootstrap.rtl.min.css`.
- MapLibre GL renders correctly in RTL contexts.
- The CSS grid and flexbox layouts used in the filter panel will
  mostly auto-adapt, but specific `margin-left` / `padding-left`
  values may need logical property equivalents (`margin-inline-start`).

---

## 5. Translation Workflow for Trusted Users

### Option A: Django Admin

Trusted translators log in to the Django admin and edit translatable
fields directly.  `django-modeltranslation` adds tabbed language fields
to each model admin page.

**Pros:** No new UI to build; leverages existing permissions.  
**Cons:** The admin is not user-friendly for non-technical translators.

### Option B: Dedicated Translation Interface

Build a lightweight "Translation Dashboard" page where trusted users:

1. See all translatable strings for a given page/section.
2. See the English source text alongside empty or existing translation
   fields for their language.
3. Submit translations which are saved to the `.po` files (via
   `django-rosetta` or a custom view) or to model fields.

**Pros:** Much better UX for translators.  
**Cons:** Requires development effort.

### Option C: External Platform (Weblate / Crowdin)

Host `.po` files on Weblate (open-source, self-hostable) or Crowdin
(SaaS).  Translators use the platform's web interface.  Translations
are synced to the Git repository automatically.

**Pros:** Professional translation workflow, translation memory,
glossaries, review process.  
**Cons:** Additional infrastructure; model translations still need
a separate mechanism.

### Recommendation

Start with **Option A** (Django Admin) for model translations and
**Option C** (Weblate) for `.po` file translations.  This gives the
best balance of effort vs. translator experience.  Migrate to
Option B if the admin proves too cumbersome.

---

## 6. Implementation Steps (suggested order)

1. **Enable Django i18n** — add middleware, set `LANGUAGES`, configure
   `LOCALE_PATHS`.
2. **Mark Search page template strings** — apply `{% trans %}` tags.
3. **Set up JavaScriptCatalog** — add URL, configure search.js and
   widget modules to use `gettext()`.
4. **Extract and translate** — run `makemessages`, translate the
   `.po` file for one target language as a proof of concept.
5. **Add language switcher** to the site header.
6. **Install django-modeltranslation** — register models with
   translatable fields.
7. **Expand** to other pages incrementally.

---

## 7. Key Django Settings

```python
# settings.py additions for i18n

USE_I18N = True  # Already True by default in Django
USE_L10N = True

LANGUAGE_CODE = 'en'

LANGUAGES = [
    ('en', 'English'),
    ('fr', 'Français'),
    ('de', 'Deutsch'),
    ('es', 'Español'),
    ('ar', 'العربية'),
    # Add more as needed
]

LOCALE_PATHS = [
    BASE_DIR / 'locale',
]

MIDDLEWARE = [
    # ... existing middleware ...
    'django.middleware.locale.LocaleMiddleware',  # After SessionMiddleware, before CommonMiddleware
    # ...
]
```

---

## 8. Estimated Effort

| Task | Effort |
|------|--------|
| Enable i18n infrastructure | 1–2 hours |
| Mark Search page strings (template + JS) | 3–4 hours |
| First language translation (e.g. French) | 4–8 hours (depending on translator availability) |
| Language switcher UI | 1–2 hours |
| django-modeltranslation setup | 2–3 hours |
| RTL testing and fixes | 2–4 hours |
| **Total for Search page MVP** | **~2–3 days** |

