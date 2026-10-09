#!/usr/bin/env python3
"""Headless smoke test of the Atlas (/atlas/), to run before and after a deploy.

    /usr/bin/python3 scripts/atlas_smoke.py https://dev.whgazetteer.org
    /usr/bin/python3 scripts/atlas_smoke.py https://whgazetteer.org --json out.json
    /usr/bin/python3 scripts/atlas_smoke.py https://dev.whgazetteer.org --prove-it-fails
    /usr/bin/python3 scripts/atlas_smoke.py https://dev.whgazetteer.org --bundle static/webpack/atlas.bundle.js

Anonymous only. It drives Playwright's own bundled Chromium (never the user's
Chrome, never a visible window) through the scenarios below, each in a fresh
browser context so no check inherits state from another:

  plain          /atlas/ at 1400x900: HTTP 200, Atlas DOM, the app's own
                 readiness flag, the globe is actually rendering (spinning),
                 planned controls are labelled by name, the first-visit tour
                 starts, no page errors.
  beta_gates     the beta-only JSON endpoints answer 403 with the honest
                 message for an anonymous caller; /atlas/status/ (ungated)
                 answers 200 from the same client, as the positive control.
  deeplink_gn    /atlas/?gazetteer=gn opens Places > Explore on GeoNames, the
                 gn marker layer exists, the anonymous place list says "beta
                 feature" AND the 403 behind it was observed, and the map
                 settles (loaded + tiles) because Explore stops the spin.
  deeplink_osm   the same for a namespace that is also a base-style source.
  mobile_375 /   /atlas/ in a touch viewport: no horizontal scroll, no visible
  mobile_768     control off-screen, no tour. Controls must be FOUND for the
                 off-screen check to mean anything.
  places_typing  switch to Places and type: the text stays (tour marked seen,
                 so the mode switch is tested on its own). Also the
                 tooltip-over-sibling defect, place#321 (see KNOWN_FAILURES).
  first_visit /  the same two actions with NOTHING in localStorage, as a new
  first_visit_link
                 visitor: the tour auto-starts 1.5 s after boot and resets the
                 search bar, place#320. Known failures until that is fixed.

Scenarios that test something other than first-visit behaviour mark the tour
as seen and the welcome panel as dismissed, so the tour cannot reset state
under them.

Readiness. The harness waits on window.__whgAtlas.booted, which atlas.js sets as
the last statement of its boot when the debug gate is on. The gate is enabled by
an injected localStorage value, not ?debug, because the page rewrites its own
URL with replaceState. MapLibre's map.loaded()/idle is deliberately NOT the
signal: on a plain load the globe spins until the first interaction, so the map
never reports idle on a perfectly healthy page (measured 2026-10-09: stopping
the spin made loaded() true within 1.6 s).

--legacy-ready accepts, as a labelled second-best, "map instance present, style
loaded, loading overlay cleared" when the deployed bundle predates the flag.
Each such pass is marked LEGACY in the output. Drop that switch once every
target carries the flag.

Prove it can fail. --prove-it-fails points every page scenario at a page with
no map on it (data: URL) and every API check at a path that cannot answer, and
requires EVERY check to fail; it exits non-zero naming any check that passed.
--prove-it-fails=home does the same against the site's own home page, a live
healthy page that is not the Atlas, where only boot.http_200 may pass.

Beta-only checks. Nothing here can log in (ORCiD). --storage-state FILE takes a
Playwright storage-state JSON exported from a logged-in beta session and runs
the `beta` scenario with it. Untested at the time of writing; it is a hook.

Exit codes: 0 all checks pass (known failures excepted), 1 a check failed,
2 the harness could not run (Playwright missing, target unreachable).

Playwright is a development instrument, not a project dependency: it is a
--user install for /usr/bin/python3 on the development machine and is kept out
of requirements.txt on purpose. Never run this against a target where it could
create content: it only ever reads.
"""
import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone

try:
    from playwright.sync_api import sync_playwright
except ImportError:  # pragma: no cover
    sys.stderr.write(
        "playwright is not importable from this interpreter (%s).\n"
        "It is a --user install for /usr/bin/python3; run with that, or\n"
        "  /usr/bin/python3 -m pip install --user playwright && /usr/bin/python3 -m playwright install chromium\n"
        % sys.executable)
    sys.exit(2)

# Software-GL flags. Measured elsewhere as not load-bearing on this machine's
# bundled Chromium (it already reports a SwiftShader ANGLE backend without
# them), kept as insurance against another build. --no-gl-flags re-measures.
GL_FLAGS = ["--enable-unsafe-swiftshader", "--use-gl=angle", "--use-angle=swiftshader"]

# Debug gate on (exposes window.heroMapInstance and the readiness flag).
INIT_DEBUG = "try { localStorage.setItem('whg.debug', '1'); } catch (e) {}"
# ...plus the tour marked seen and the welcome panel dismissed: a returning
# visitor. First-visit scenarios use INIT_DEBUG alone.
INIT_QUIET = INIT_DEBUG + ("try { localStorage.setItem('whg_atlas_tour_seen', 'true');"
                           " localStorage.setItem('whg_atlas_welcome_dismissed', 'true'); } catch (e) {}")

BETA_MESSAGE_JSON = "beta access required"
BETA_MESSAGE_DOM = "This is a beta feature"
NO_MAP_PAGE = "data:text/html,<title>no map here</title><p>no map here</p>"

# Checks that fail on purpose until the named issue is fixed. They are run and
# reported; they do not affect the exit code. When one PASSES the run says so,
# which is the cue to remove it from this table.
KNOWN_FAILURES = {
    "places_typing.tooltip_not_over_sibling": "place#321",
    "first_visit.typed_text_survives_tour": "place#320",
    "first_visit_link.deeplink_survives_tour": "place#320",
}

# Console errors an anonymous run is EXPECTED to produce, per scenario. An
# allow-list is an absence claim, so each scenario that allows one must also
# assert the presence that justifies it (the observed 403).
GATE_403_PATTERNS = [
    re.compile(r"status of 403"),
    re.compile(r"HTTP 403"),
]


class Results:
    def __init__(self):
        self.rows = []

    def add(self, scenario, name, ok, detail="", **extra):
        key = f"{scenario}.{name}"
        row = {"check": key, "ok": bool(ok), "detail": detail}
        if key in KNOWN_FAILURES:
            row["known_failure"] = KNOWN_FAILURES[key]
        row.update(extra)
        self.rows.append(row)
        if ok and "known_failure" in row:
            tag = "XPASS"
        elif ok:
            tag = "LEGACY" if extra.get("legacy") else "ok"
        elif "known_failure" in row:
            tag = "KNOWN"
        else:
            tag = "FAIL"
        print(f"  {tag:6s} {key:44s} {detail}")
        return bool(ok)


# ── Page helpers ─────────────────────────────────────────────────────────────

def attach_listeners(page):
    state = {"pageerrors": [], "console": [], "responses": []}
    page.on("pageerror", lambda e: state["pageerrors"].append(str(e)[:300]))
    page.on("console", lambda m: m.type == "error" and state["console"].append(m.text[:300]))
    page.on("response", lambda r: state["responses"].append((r.url, r.status)))
    return state


READY_STATE_JS = """() => {
    const m = window.heroMapInstance;
    const ov = document.getElementById('map_loading_overlay');
    const ld = document.getElementById('atlas_loading');
    return {
        flag: (typeof window.__whgAtlas === 'object' && window.__whgAtlas) ? window.__whgAtlas : null,
        hasMap: !!m,
        styleLoaded: !!(m && m.isStyleLoaded()),
        overlayCleared: !ov || ov.style.display === 'none' || ov.classList.contains('fade-out'),
        loadingHidden: !ld || ld.style.display === 'none',
        hidden: document.hidden,
        progress: (ov ? ov.className : '-') + '|' + (m ? m.getStyle().layers.length + ' layers' : 'no map'),
    };
}"""


def wait_ready(page, budget_s, legacy_ok):
    """Wait for the Atlas's own flag. Returns (ok, detail, extra)."""
    t0 = time.monotonic()
    first = last = None
    while True:
        try:
            st = page.evaluate(READY_STATE_JS)
        except Exception as e:  # navigation or a dead page
            st = {"flag": None, "hasMap": False, "styleLoaded": False, "overlayCleared": False,
                  "loadingHidden": False, "hidden": None, "progress": f"evaluate failed: {str(e)[:80]}"}
        if first is None:
            first = st["progress"]
        last = st["progress"]
        elapsed = time.monotonic() - t0
        flag = st["flag"]
        if flag is not None:
            if flag.get("booted"):
                return True, f"booted at {flag.get('t')} ms (page clock), {elapsed:.1f}s", {"elapsed": round(elapsed, 1)}
            return False, f"boot FAILED: {flag.get('error')!r} after {elapsed:.1f}s", {"elapsed": round(elapsed, 1)}
        if legacy_ok and st["hasMap"] and st["styleLoaded"] and st["overlayCleared"]:
            return True, (f"LEGACY signal (no window.__whgAtlas on this bundle): map present, style loaded, "
                          f"overlay cleared, {elapsed:.1f}s"), {"elapsed": round(elapsed, 1), "legacy": True}
        if elapsed >= budget_s:
            over = elapsed - budget_s
            diag = {k: st[k] for k in ("hasMap", "styleLoaded", "overlayCleared", "loadingHidden", "hidden")}
            moved = "progress moved" if first != last else "progress did NOT move"
            detail = (f"timeout {budget_s}s ({moved}: {first!r} -> {last!r}); {json.dumps(diag)}"
                      + (f"; overshoot {over:.1f}s" if over > 1 else ""))
            return False, detail, {"elapsed": round(elapsed, 1), "overshoot": round(over, 1)}
        page.wait_for_timeout(250)


def bounded_wait(page, js, budget_s):
    """page.wait_for_function with the timeout reported as a result, not raised."""
    t0 = time.monotonic()
    try:
        page.wait_for_function(js, timeout=budget_s * 1000)
        return True, time.monotonic() - t0
    except Exception:
        return False, time.monotonic() - t0


def unexpected_console(state, allow):
    return [c for c in state["console"] if not any(p.search(c) for p in allow)]


def observed(state, path_fragment, status):
    return [u for (u, s) in state["responses"] if path_fragment in u and s == status]


def new_context(browser, opts, first_visit=False, **kw):
    ctx = browser.new_context(**kw)
    ctx.add_init_script(INIT_DEBUG if first_visit else INIT_QUIET)
    if opts.bundle:
        bundle_bytes = open(opts.bundle, "rb").read()
        ctx.route("**/static/webpack/atlas.bundle.js*",
                  lambda route: route.fulfill(status=200, content_type="application/javascript", body=bundle_bytes))
    return ctx


def open_page(ctx, url, state_holder):
    page = ctx.new_page()
    st = attach_listeners(page)
    state_holder.update(st)
    try:
        resp = page.goto(url, wait_until="domcontentloaded")
        status = resp.status if resp else None
    except Exception as e:
        status = f"goto failed: {str(e)[:100]}"
    return page, status


def snapshot(page, opts, name):
    if opts.shots:
        try:
            page.screenshot(path=f"{opts.shots}/{name}.png")
        except Exception:
            pass


ATLAS_DOM_JS = """() => ({
    search: !!document.getElementById('atlas_search_input'),
    toggle: !!document.querySelector('.search-mode-toggle .btn[data-search-mode="toponyms"]'),
})"""

# The bundles are appended by base_webpack's deferred loader AFTER
# domcontentloaded, so the version is read once the page has booted.
BUNDLE_VERSION_JS = """() => [...document.scripts].map(s => s.src).filter(s => /atlas\\.bundle\\.js/.test(s))
    .map(s => (s.match(/[?&]v=([^&]+)/) || [, '?'])[1])[0] || null"""


def check_boot(R, S, page, status, opts):
    R.add(S, "http_200", status == 200, f"status {status}")
    dom = page.evaluate(ATLAS_DOM_JS)
    R.add(S, "atlas_dom", dom["search"] and dom["toggle"], f"search input {dom['search']}, mode toggle {dom['toggle']}")
    ok, detail, extra = wait_ready(page, opts.ready_timeout, opts.legacy_ready)
    v = page.evaluate(BUNDLE_VERSION_JS)
    R.add(S, "ready_flag", ok, f"{detail}; atlas bundle v={v}" + (" (served from --bundle)" if opts.bundle else ""), bundle_v=v, **extra)
    return ok


# ── Scenarios ────────────────────────────────────────────────────────────────

def scenario_plain(browser, R, opts, url_for):
    S = "plain"
    ctx = new_context(browser, opts, first_visit=True, viewport={"width": 1400, "height": 900})
    state = {}
    page, status = open_page(ctx, url_for("/atlas/"), state)
    try:
        booted = check_boot(R, S, page, status, opts)

        # The map is RENDERING, not just instantiated: on a plain load the globe
        # spins (centre longitude moves) and MapLibre emits render events.
        spin = page.evaluate("""() => new Promise(res => {
            const m = window.heroMapInstance; if (!m) return res({hasMap: false});
            let renders = 0; const h = () => renders++; m.on('render', h);
            const lng0 = m.getCenter().lng;
            setTimeout(() => { m.off('render', h); res({hasMap: true, renders, dlng: Math.abs(m.getCenter().lng - lng0)}); }, 1500);
        })""")
        R.add(S, "map_renders_and_spins", spin.get("hasMap") and spin["renders"] > 0 and spin["dlng"] > 0.05,
              f"{spin.get('renders', 0)} renders in 1.5s, centre moved {spin.get('dlng', 0):.2f} deg" if spin.get("hasMap") else "no map instance")

        planned = page.evaluate("""() => [...document.querySelectorAll('.gaz-planned[aria-disabled="true"]')]
            .map(b => ({pill: b.dataset.gazTypePill, tag: (b.querySelector('.whg-planned-tag') || {}).textContent || ''}))""")
        by_pill = {p["pill"]: p["tag"].strip().lower() for p in planned}
        R.add(S, "planned_controls_labelled",
              by_pill.get("itinerary") == "planned" and by_pill.get("network") == "planned",
              f"itinerary={by_pill.get('itinerary')!r} network={by_pill.get('network')!r} (by name, {len(planned)} aria-disabled pills)")

        ok, secs = bounded_wait(page, "!!document.querySelector('.driver-popover')", opts.tour_timeout)
        R.add(S, "tour_autostarts_desktop", ok, f"driver.js popover {'appeared' if ok else 'absent'} after {secs:.1f}s "
              "(positive control for the mobile no-tour checks)")

        unexpected = unexpected_console(state, [])
        R.add(S, "no_page_errors", booted and not state["pageerrors"] and not unexpected,
              f"booted {booted}; {len(state['pageerrors'])} page errors, {len(unexpected)} console errors"
              + (": " + "; ".join((state["pageerrors"] + unexpected)[:3]) if (state["pageerrors"] or unexpected) else ""))
        snapshot(page, opts, S)
    finally:
        ctx.close()


def scenario_beta_gates(browser, R, opts, url_for):
    S = "beta_gates"
    ctx = new_context(browser, opts)
    try:
        req = ctx.request
        for name, path in (("search", "/atlas/search/?q=smoke"),
                           ("place", "/atlas/place/?id=gn:2643743"),
                           ("boundaries", "/atlas/boundaries/?q=smoke")):
            url = url_for(path, api=True)
            try:
                r = req.get(url, timeout=20000)
                status = r.status
                try:
                    body = r.json()
                except Exception:
                    body = {"_text": r.text()[:80]}
            except Exception as e:
                status, body = f"request failed: {str(e)[:80]}", {}
            msg = body.get("error") if isinstance(body, dict) else None
            R.add(S, f"{name}_403_honest", status == 403 and msg == BETA_MESSAGE_JSON,
                  f"{path.split('?')[0]} -> {status}, error={msg!r}")
        url = url_for("/atlas/status/", api=True)
        try:
            r = req.get(url, timeout=20000)
            status = r.status
            is_json = isinstance(r.json(), dict)
        except Exception as e:
            status, is_json = f"request failed: {str(e)[:80]}", False
        R.add(S, "status_ungated_200", status == 200 and is_json,
              f"/atlas/status/ -> {status}, json={is_json} (same anonymous client reaches an ungated endpoint)")
    finally:
        ctx.close()


DEEPLINK_JS = """(ns) => {
    const m = window.heroMapInstance;
    const heading = document.querySelector('#atlas_placelists_view .placelist-title, .placelist-title');
    return {
        placesActive: !!document.querySelector('.search-mode-toggle .btn[data-search-mode="toponyms"].active'),
        modeClass: (document.getElementById('floating_search') || {}).className || '',
        exploreActive: !!document.querySelector('#gazetteers_offcanvas .gazetteer-mode-toggle .btn[data-gazetteer-mode="explore"].active'),
        checked: [...document.querySelectorAll('#gazetteers_offcanvas .authority-cb:checked')].map(e => e.value + ':' + e.type),
        layer: !!(m && m.getLayer(ns + '_circle')),
        heading: heading ? heading.textContent.trim() : null,
        betaText: document.body.textContent.includes(%r),
        url: location.href,
    };
}""" % BETA_MESSAGE_DOM


def scenario_deeplink(browser, R, opts, url_for, ns, heading_expected, expect_layer, expect_settle):
    S = f"deeplink_{ns}"
    ctx = new_context(browser, opts, viewport={"width": 1400, "height": 900})
    state = {}
    page, status = open_page(ctx, url_for(f"/atlas/?gazetteer={ns}"), state)
    try:
        booted = check_boot(R, S, page, status, opts)
        # The deep-link handler polls (<=2 s) for the radio, then opens the Place
        # List, which fetches /atlas/search/. Wait for THAT request to answer.
        ok, secs = bounded_wait(page, "document.body.textContent.includes(%r)" % BETA_MESSAGE_DOM, 30)
        d = page.evaluate(DEEPLINK_JS, ns)
        R.add(S, "places_mode_active", d["placesActive"] and "mode-toponyms" in d["modeClass"], f"mode class {d['modeClass']!r}")
        R.add(S, "explore_tab_active", d["exploreActive"], "Explore tab active" if d["exploreActive"] else "Explore tab NOT active")
        R.add(S, "gazetteer_radio_checked", d["checked"] == [f"{ns}:radio"], f"checked inputs {d['checked']}")
        R.add(S, "placelist_heading", bool(d["heading"]) and heading_expected in d["heading"], f"heading {d['heading']!r}")
        if expect_layer:
            R.add(S, "marker_layer_present", d["layer"], f"map.getLayer('{ns}_circle') -> {d['layer']}")
        gate_hits = observed(state, "/atlas/search/", 403)
        R.add(S, "placelist_honest_beta_message", ok and d["betaText"] and bool(gate_hits),
              f"DOM says {BETA_MESSAGE_DOM!r}: {d['betaText']} after {secs:.1f}s; /atlas/search/ 403 observed {len(gate_hits)}x")
        R.add(S, "url_keeps_gazetteer", f"gazetteer={ns}" in d["url"], d["url"][-60:])
        if expect_settle:
            ok, secs = bounded_wait(page, "window.heroMapInstance && window.heroMapInstance.loaded() && window.heroMapInstance.areTilesLoaded()",
                                    opts.settle_timeout)
            R.add(S, "map_settles", ok, f"loaded() && areTilesLoaded() {'after' if ok else 'NOT within'} {secs:.1f}s "
                  "(Explore stops the globe spin, so settling is expected here)")
        unexpected = unexpected_console(state, GATE_403_PATTERNS if gate_hits else [])
        R.add(S, "no_page_errors_beyond_gate", booted and not state["pageerrors"] and not unexpected,
              f"booted {booted}; {len(state['pageerrors'])} page errors, {len(unexpected)} console errors beyond the observed 403"
              + (": " + "; ".join((state["pageerrors"] + unexpected)[:3]) if (state["pageerrors"] or unexpected) else ""))
        snapshot(page, opts, S)
    finally:
        ctx.close()


MOBILE_JS = """() => {
    const vis = [...document.querySelectorAll('button, input, select, a.btn')].filter(e => e.offsetParent && e.getBoundingClientRect().width > 0);
    const off = vis.filter(e => { const r = e.getBoundingClientRect(); return r.right > innerWidth + 1 || r.left < -1; });
    return {
        innerWidth, scrollWidth: document.documentElement.scrollWidth,
        visible: vis.length,
        offscreen: off.map(e => (e.id || e.className || e.tagName).toString().slice(0, 40)).slice(0, 8),
        tour: !!document.querySelector('.driver-popover'),
    };
}"""


def scenario_mobile(browser, R, opts, url_for, width, height):
    S = f"mobile_{width}"
    ctx = new_context(browser, opts, first_visit=True, viewport={"width": width, "height": height}, is_mobile=True, has_touch=True)
    state = {}
    page, status = open_page(ctx, url_for("/atlas/"), state)
    try:
        booted = check_boot(R, S, page, status, opts)
        page.wait_for_timeout(int(opts.tour_timeout * 1000 // 2))   # the desktop tour starts 1.5 s after boot; give it longer
        m = page.evaluate(MOBILE_JS)
        # Layout checks are generic, so they only count on a page that IS the
        # booted Atlas with controls on it (the home page passes them otherwise).
        subject = booted and m["visible"] >= 5
        R.add(S, "controls_found", subject, f"booted {booted}; {m['visible']} visible Atlas controls (the next three checks need both)")
        R.add(S, "no_horizontal_scroll", subject and m["scrollWidth"] <= m["innerWidth"] + 1,
              f"scrollWidth {m['scrollWidth']} vs innerWidth {m['innerWidth']}")
        R.add(S, "no_offscreen_controls", subject and not m["offscreen"], f"off-screen: {m['offscreen'] or 'none'}")
        R.add(S, "no_tour", subject and not m["tour"],
              f"driver.js popover present: {m['tour']} (control: plain.tour_autostarts_desktop)")
        R.add(S, "no_page_errors", booted and not state["pageerrors"] and not state["console"],
              f"booted {booted}; {len(state['pageerrors'])} page errors, {len(state['console'])} console errors"
              + (": " + "; ".join((state["pageerrors"] + state["console"])[:3]) if (state["pageerrors"] or state["console"]) else ""))
        snapshot(page, opts, S)
    finally:
        ctx.close()


def scenario_places_typing(browser, R, opts, url_for):
    S = "places_typing"
    ctx = new_context(browser, opts, viewport={"width": 1400, "height": 900})
    state = {}
    page, status = open_page(ctx, url_for("/atlas/"), state)
    try:
        booted = check_boot(R, S, page, status, opts)
        has_btn = page.evaluate("!!document.querySelector('.search-mode-toggle .btn[data-search-mode=\"toponyms\"]')")
        if booted and has_btn:
            # Switch mode by a DOM click (bypasses the tooltip defect checked below),
            # focus the box and type. The text must survive the mode switch's own
            # input.value = '' and the basemap swap that follows.
            page.evaluate("document.querySelector('.search-mode-toggle .btn[data-search-mode=\"toponyms\"]').click()")
            mode = page.evaluate("document.getElementById('floating_search').className")
            page.evaluate("document.getElementById('atlas_search_input').focus()")
            page.keyboard.type("Jerusalem")
            v0 = page.input_value("#atlas_search_input")
            page.wait_for_timeout(2000)
            v1 = page.input_value("#atlas_search_input")
            R.add(S, "text_retained_after_mode_switch", "mode-toponyms" in mode and v0 == "Jerusalem" and v1 == "Jerusalem",
                  f"mode {mode!r}; value immediately {v0!r}, after 2 s {v1!r}")

            # Real-mouse path: rest on Areas (its tooltip opens to the RIGHT, over
            # Places), move to Places and click within the tooltip's fade.
            page.evaluate("document.querySelector('.search-mode-toggle .btn[data-search-mode=\"areas\"]').click()")
            areas = page.locator('.search-mode-toggle .btn[data-search-mode="areas"]').bounding_box()
            places = page.locator('.search-mode-toggle .btn[data-search-mode="toponyms"]').bounding_box()
            pc = (places["x"] + places["width"] / 2, places["y"] + places["height"] / 2)
            page.mouse.move(areas["x"] + areas["width"] / 2, areas["y"] + areas["height"] / 2)
            page.wait_for_timeout(400)
            tip_shown = page.evaluate("!!document.querySelector('.tooltip.show')")
            page.mouse.move(*pc)
            page.wait_for_timeout(120)
            under = page.evaluate("([x, y]) => { const e = document.elementFromPoint(x, y); return e ? ((e.getAttribute('class') || e.tagName) + '').slice(0, 40) : null; }", list(pc))
            page.mouse.down(); page.mouse.up()
            page.wait_for_timeout(150)
            mode2 = page.evaluate("document.getElementById('floating_search').className")
            R.add(S, "tooltip_not_over_sibling", tip_shown and "tooltip" not in (under or "") and "mode-toponyms" in mode2,
                  f"Areas tooltip shown: {tip_shown}; under cursor 120 ms after moving to Places: {under!r}; click -> {mode2!r}")
        else:
            R.add(S, "text_retained_after_mode_switch", False, "not attempted: page did not boot / no mode toggle")
            R.add(S, "tooltip_not_over_sibling", False, "not attempted: page did not boot / no mode toggle")
        snapshot(page, opts, S)
    finally:
        ctx.close()


def scenario_first_visit(browser, R, opts, url_for):
    """A new visitor: nothing in localStorage, so the tour auto-starts 1.5 s
    after boot. Both checks are KNOWN failures until place#320 is fixed."""
    S = "first_visit"
    # (a) click Places and type straight away
    ctx = new_context(browser, opts, first_visit=True, viewport={"width": 1400, "height": 900})
    state = {}
    page, status = open_page(ctx, url_for("/atlas/"), state)
    try:
        booted = check_boot(R, S, page, status, opts)
        tour_already = page.evaluate("!!document.querySelector('.driver-popover')")
        if tour_already:
            # Typing AFTER the tour's reset would pass for the wrong reason. The
            # legacy readiness signal (overlay cleared) can land after the
            # 1.5 s auto-start; only the app flag gives this check its ordering.
            R.add(S, "typed_text_survives_tour", False,
                  "not measurable: the tour was already running before the action (readiness came after the auto-start)")
        elif booted and page.evaluate("!!document.querySelector('.search-mode-toggle .btn[data-search-mode=\"toponyms\"]')"):
            page.evaluate("document.querySelector('.search-mode-toggle .btn[data-search-mode=\"toponyms\"]').click()")
            page.evaluate("document.getElementById('atlas_search_input').focus()")
            page.keyboard.type("Jerusalem")
            v0 = page.input_value("#atlas_search_input")
            # The auto-start timer is 1.5 s after boot; wait for the popover
            # itself (bounded), then one more second for its cleanup to land.
            tour, secs = bounded_wait(page, "!!document.querySelector('.driver-popover')", opts.tour_timeout)
            page.wait_for_timeout(1000)
            v1 = page.input_value("#atlas_search_input")
            mode = page.evaluate("document.getElementById('floating_search').className")
            R.add(S, "typed_text_survives_tour", v0 == "Jerusalem" and v1 == "Jerusalem" and "mode-toponyms" in mode,
                  f"value immediately {v0!r}, after the tour {'started' if tour else 'did not start'} ({secs:.1f}s) + 1 s: {v1!r}; mode {mode!r}")
        else:
            R.add(S, "typed_text_survives_tour", False, "not attempted: page did not boot / no mode toggle")
    finally:
        ctx.close()
    # (b) follow a shared ?gazetteer= link
    S = "first_visit_link"
    ctx = new_context(browser, opts, first_visit=True, viewport={"width": 1400, "height": 900})
    state = {}
    page, status = open_page(ctx, url_for("/atlas/?gazetteer=gn"), state)
    try:
        booted = check_boot(R, S, page, status, opts)
        tour, secs = bounded_wait(page, "!!document.querySelector('.driver-popover')", opts.tour_timeout)
        page.wait_for_timeout(1000)
        d = page.evaluate(DEEPLINK_JS, "gn")
        R.add(S, "deeplink_survives_tour", booted and d["placesActive"] and "mode-toponyms" in d["modeClass"] and d["exploreActive"] and d["checked"] == ["gn:radio"],
              f"after the tour {'started' if tour else 'did not start'} ({secs:.1f}s) + 1 s: mode {d['modeClass']!r}, Explore active {d['exploreActive']}, checked {d['checked']}")
        snapshot(page, opts, S)
    finally:
        ctx.close()


def scenario_beta(browser, R, opts, url_for):
    """Hook for checks that need a beta login. Needs --storage-state from a real
    logged-in session; nothing here can obtain one headlessly (ORCiD)."""
    S = "beta"
    ctx = new_context(browser, opts, storage_state=opts.storage_state)
    try:
        r = ctx.request.get(url_for("/atlas/search/?q=Jerusalem", api=True), timeout=60000)
        body = {}
        try:
            body = r.json()
        except Exception:
            pass
        R.add(S, "search_answers_for_beta_user", r.status == 200 and isinstance(body, dict) and "hits" in body,
              f"/atlas/search/ -> {r.status}, keys {sorted(body)[:6] if isinstance(body, dict) else type(body).__name__}")
    finally:
        ctx.close()


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("base", help="https://dev.whgazetteer.org or https://whgazetteer.org (no trailing slash)")
    ap.add_argument("--prove-it-fails", nargs="?", const="nomap", choices=["nomap", "home"],
                    help="run against a subject that cannot satisfy the checks and require every check to fail")
    ap.add_argument("--legacy-ready", action="store_true", help="accept the pre-flag readiness signal (labelled LEGACY)")
    ap.add_argument("--no-gl-flags", action="store_true", help="launch Chromium without the software-GL flags")
    ap.add_argument("--bundle", help="serve this local atlas.bundle.js in place of the deployed one (pre-deploy check)")
    ap.add_argument("--storage-state", help="Playwright storage-state JSON of a logged-in beta session; enables the beta scenario")
    ap.add_argument("--json", help="write the results and run metadata here")
    ap.add_argument("--shots", help="directory for one screenshot per scenario (to look at, never to compare)")
    ap.add_argument("--ready-timeout", type=float, default=90, help="seconds to wait for the app readiness flag")
    ap.add_argument("--settle-timeout", type=float, default=60, help="seconds for loaded()+tiles on the gn deep link")
    ap.add_argument("--tour-timeout", type=float, default=12, help="seconds for the desktop tour to auto-start")
    ap.add_argument("--only", help="comma-separated scenario names to run")
    opts = ap.parse_args()
    base = opts.base.rstrip("/")

    prove = opts.prove_it_fails

    def url_for(path, api=False):
        if not prove:
            return base + path
        if api:
            return base + "/__atlas_smoke_no_such_prefix__" + path
        return (base + "/") if prove == "home" else NO_MAP_PAGE

    scenarios = [
        ("plain", lambda b, R: scenario_plain(b, R, opts, url_for)),
        ("beta_gates", lambda b, R: scenario_beta_gates(b, R, opts, url_for)),
        ("deeplink_gn", lambda b, R: scenario_deeplink(b, R, opts, url_for, "gn", "GeoNames", True, True)),
        ("deeplink_osm", lambda b, R: scenario_deeplink(b, R, opts, url_for, "osm", "OpenStreetMap", False, False)),
        ("mobile_375", lambda b, R: scenario_mobile(b, R, opts, url_for, 375, 812)),
        ("mobile_768", lambda b, R: scenario_mobile(b, R, opts, url_for, 768, 1024)),
        ("places_typing", lambda b, R: scenario_places_typing(b, R, opts, url_for)),
        ("first_visit", lambda b, R: scenario_first_visit(b, R, opts, url_for)),
    ]
    if opts.storage_state:
        scenarios.append(("beta", lambda b, R: scenario_beta(b, R, opts, url_for)))
    if opts.only:
        wanted = set(opts.only.split(","))
        scenarios = [s for s in scenarios if s[0] in wanted]

    R = Results()
    started = datetime.now(timezone.utc)
    print(f"atlas_smoke: {base}  mode={'PROVE-IT-FAILS:' + prove if prove else 'check'}"
          f"  legacy_ready={opts.legacy_ready}  gl_flags={not opts.no_gl_flags}  bundle={opts.bundle or '-'}")
    t0 = time.monotonic()
    with sync_playwright() as pw:
        browser = pw.chromium.launch(args=[] if opts.no_gl_flags else GL_FLAGS)
        try:
            for name, fn in scenarios:
                print(f"[{name}]")
                try:
                    fn(browser, R)
                except Exception as e:   # a harness fault is a failure, never a skip
                    R.add(name, "harness", False, f"scenario raised {type(e).__name__}: {str(e)[:160]}")
        finally:
            browser.close()
    elapsed = time.monotonic() - t0

    rows = R.rows
    n = len(rows)
    if prove:
        allowed = {"plain.http_200", "deeplink_gn.http_200", "deeplink_osm.http_200",
                   "mobile_375.http_200", "mobile_768.http_200", "places_typing.http_200", "first_visit.http_200",
                   "first_visit_link.http_200"} if prove == "home" else set()
        could_not_fail = [r["check"] for r in rows if r["ok"] and r["check"] not in allowed]
        passed = sum(1 for r in rows if r["ok"])
        print(f"\nprove-it-fails ({prove}): {n} checks, {n - passed} failed as required, "
              f"{len(could_not_fail)} COULD NOT FAIL" + (": " + ", ".join(could_not_fail) if could_not_fail else "")
              + (f"; {len(allowed & {r['check'] for r in rows if r['ok']})} allowed passes (http_200 on a live non-Atlas page)" if prove == "home" else "")
              + f"; {elapsed:.0f}s")
        code = 0 if not could_not_fail else 1
    else:
        failed = [r for r in rows if not r["ok"] and "known_failure" not in r]
        known = [r for r in rows if not r["ok"] and "known_failure" in r]
        xpass = [r for r in rows if r["ok"] and "known_failure" in r]
        legacy = [r for r in rows if r.get("legacy")]
        print(f"\n{n} checks: {n - len(failed) - len(known)} pass ({len(legacy)} LEGACY), "
              f"{len(known)} known-fail ({', '.join(sorted({r['known_failure'] for r in known})) or '-'}), {len(failed)} fail; {elapsed:.0f}s")
        if xpass:
            print("XPASS, remove from KNOWN_FAILURES once the fix is confirmed: " + ", ".join(r["check"] for r in xpass))
        code = 0 if not failed else 1

    if opts.json:
        with open(opts.json, "w") as f:
            json.dump({"base": base, "started": started.isoformat(), "elapsed_s": round(elapsed, 1),
                       "mode": prove or "check", "legacy_ready": opts.legacy_ready, "gl_flags": not opts.no_gl_flags,
                       "bundle_override": opts.bundle, "exit": code, "results": rows}, f, indent=1)
    sys.exit(code)


if __name__ == "__main__":
    main()
