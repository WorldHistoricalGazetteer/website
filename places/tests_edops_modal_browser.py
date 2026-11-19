"""The beta EDOPS modal (Profile / Signature / Charts), driven in HEADLESS Chromium.

Opt-in, like ``places.tests_enrichment_browser`` whose fixtures and helpers it reuses:

    WHG_BROWSER_TESTS=1 GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test places.tests_edops_modal_browser --settings=whg.settings_localtest

``WHG_BROWSER_SHOTS=<dir>`` keeps screenshots. Needs the committed bundles rebuilt
(``npm run build:prod``): a stale bundle has no modal and every check fails. EDOPS is
never called (recorded fixtures through a fake transport). ``test_edops_modal_harness_can_fail`` runs
the same checks with the page's bundle blocked and requires them to fail.
"""
from unittest.mock import patch

from api.tests_edop_client import AREA, BASIN_T8, FakeTransport
from places import tests_enrichment_browser as base

fake_fetch = base.fake_fetch


class EdopsModalBrowserTests(base.EnrichmentBrowserTests):
    def modal_checks(self, page, pk):
        """Open the Record page of ``pk`` (a basin with Band T), the modal, and read the
        three tabs. Every check is a boolean; a missing control makes them False."""
        page.goto(f"{self.live_server_url}/atlas/place/{pk}/#record_environment")
        r = {}
        r["gloss_ready"] = self.wait_for(page, "window.__whgEnrichment && window.__whgEnrichment.gloss === 'ready'", 30000)
        _idle, r["env_ready"] = self.show_environment(page)
        r["icon"] = page.evaluate("""() => !!document.querySelector('#record_environment .enr-env-show .edops-icon[data-placeholder="true"]')
            && !!document.querySelector('#record_environment .rs-card-title .edops-icon')""")
        r["three_tabs"] = page.evaluate("""() => Array.from(document.querySelectorAll('.enr-env-modal [role="tab"]')).map(b => b.dataset.tab).join()""") == "profile,signature,charts"
        # Placeholder prose: the Signature tab is the default and shows the existing display.
        r["default_signature"] = page.evaluate("""() => {
            const p = document.querySelector('.enr-env-modal [data-pane="signature"]');
            return !!p && !p.hidden && p.querySelector('.enr-band[data-band="B"]') !== null
                && document.querySelector('.enr-env-modal [data-pane="profile"]').hidden === true; }""")
        try:
            page.click(".enr-env-modal [data-tab='profile']", timeout=5000)
            prof = page.evaluate("""() => { const p = document.querySelector('.enr-env-modal [data-pane="profile"]');
                return {hidden: p.hidden, text: p.innerText,
                        scales: Array.from(p.querySelectorAll('.em-section')).map(s => s.dataset.scale)}; }""")
        except Exception:  # noqa: BLE001 — a missing tab is a finding
            prof = {"hidden": True, "text": "", "scales": []}
        r["profile_generated_label"] = (not prof["hidden"]) and "Generated text" in prof["text"] and "not by a language model" in prof["text"]
        r["profile_scales"] = "place" in prof["scales"] and "basin" in prof["scales"]
        r["profile_scale_labels"] = "this place" in prof["text"].lower() and "whole basin" in prof["text"].lower()   # CSS upper-cases the badge
        r["profile_version_citation"] = "signature v0.4" in prof["text"] and "2026-10-21" in prof["text"] and "Cite as" in prof["text"]
        r["profile_hedge"] = "reconstructed" in prof["text"].lower()
        try:
            page.click(".enr-env-modal [data-tab='charts']", timeout=5000)
            ch = page.evaluate("""() => { const p = document.querySelector('.enr-env-modal [data-pane="charts"]');
                const figs = Array.from(p.querySelectorAll('figure'));
                const lineLens = Array.from(p.querySelectorAll('.ec-line')).map(e => (e.getAttribute('d') || '').split('L').length);
                return {hidden: p.hidden, ids: figs.map(f => f.dataset.chart), bars: p.querySelectorAll('[data-chart="monthly_precipitation"] .ec-bar').length,
                    lineLens, roles: figs.every(f => f.querySelector('svg[role="img"] title') && f.querySelector('svg desc')),
                    tables: figs.every(f => f.querySelectorAll('details.ec-table tbody tr').length > 0),
                    sized: figs.every(f => { const b = f.querySelector('svg').getBoundingClientRect(); return b.width > 200 && b.height > 80; }),
                    cap: (p.querySelector('[data-chart="timeline_pdsi"] figcaption') || {}).innerText || '',
                    events: p.querySelectorAll('[data-chart="timeline_pdsi"] .ec-event').length}; }""")
            self.shot(page, "edops_modal_charts", ".enr-env-modal .modal-content")
        except Exception:  # noqa: BLE001
            ch = {"hidden": True, "ids": [], "bars": 0, "lineLens": [], "roles": False, "tables": False, "sized": False, "cap": "", "events": 0}
        r["charts_all_drawn"] = (not ch["hidden"]) and set(ch["ids"]) == {
            "monthly_precipitation", "monthly_temperature", "timeline_pdsi", "timeline_temperature",
            "timeline_precipitation", "land_use"}
        r["charts_data_drawn"] = ch["bars"] == 12 and len(ch["lineLens"]) >= 4 and min(ch["lineLens"] or [0]) >= 12 and ch["events"] == 3
        r["charts_accessible"] = ch["roles"] and ch["tables"] and ch["sized"]
        r["charts_scale_and_hedge"] = "Scale:" in ch["cap"] and "Reconstructed" in ch["cap"]
        return r

    def test_modal_tabs_and_charts(self):
        page = self.new_page()
        with patch("api.edop_client._requests_transport", side_effect=FakeTransport(BASIN_T8)), \
                patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch):
            results = self.modal_checks(page, self.Lonely.pk)
            self.shot(page, "edops_modal_profile")
        failed = {k: v for k, v in results.items() if not v}
        self.assertEqual(failed, {}, f"{failed}; errors={self.errors}")
        self.assertEqual(self.external, [])
        self.assertEqual(self.errors, [])

    def test_area_scope_has_no_charts_and_says_why(self):
        page = self.new_page()
        with patch("api.edop_client._requests_transport", side_effect=FakeTransport(AREA)), \
                patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch):
            page.goto(f"{self.live_server_url}/atlas/place/{self.A.pk}/#record_environment")
            self.wait_for(page, "window.__whgEnrichment && window.__whgEnrichment.gloss === 'ready'", 30000)
            _idle, ok = self.show_environment(page)
            page.click(".enr-env-modal [data-tab='charts']", timeout=5000)
            d = page.evaluate("""() => { const p = document.querySelector('.enr-env-modal [data-pane="charts"]');
                return {hidden: p.hidden, figs: p.querySelectorAll('figure').length, text: p.innerText,
                        tabs: document.querySelectorAll('.enr-env-modal [role="tab"]').length}; }""")
        self.assertTrue(ok)
        self.assertEqual(d["tabs"], 3)                                    # presence: the modal is there ...
        self.assertFalse(d["hidden"])
        self.assertEqual(d["figs"], 0)                                    # ... and draws nothing, saying why
        self.assertIn("distribution summaries", d["text"])

    def test_edops_modal_harness_can_fail(self):
        page = self.new_page(block="/atlas_record.bundle.")
        with patch("api.edop_client._requests_transport", side_effect=FakeTransport(BASIN_T8)), \
                patch("api.crc_client.crc_fetch_places", side_effect=fake_fetch):
            results = self.modal_checks(page, self.Lonely.pk)
        self.assertTrue(results and not any(results.values()), f"checks passed without the page bundle: {results}")


# Inherit the helpers, not the parent's tests (they have their own module).
for _name in dir(base.EnrichmentBrowserTests):
    if _name.startswith("test"):
        setattr(EdopsModalBrowserTests, _name, None)
for _name in ("test_modal_tabs_and_charts", "test_area_scope_has_no_charts_and_says_why", "test_edops_modal_harness_can_fail"):
    setattr(EdopsModalBrowserTests, _name, EdopsModalBrowserTests.__dict__[_name])
