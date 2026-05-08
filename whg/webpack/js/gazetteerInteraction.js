// /whg/webpack/js/gazetteerInteraction.js
//
// Hover-cursor + click-popup behaviour for gazetteer features in Atlas
// Explore mode. Owned by heroMap; one instance per heroMap.
//
// Lifecycle: heroMap.showGazetteer() calls attach(id, baseIds) once the
// gazetteer's layers are in place; hideGazetteer() and the "switching
// gazetteer" branch call detach() before the source is erased.
//
// Click target layers: ``_fill``, ``_line``, ``_circle`` for each base ID
// produced by whg_maplibre.loadGazetteerStyle. Heatmap is excluded because
// cluster centroids carry no individual ``place_id``; labels are excluded
// because labels-without-shapes feel ambiguous.

const PREVIEW_PATH = '/entity/place:'; // + encoded place_id + '/preview?variant=popup'
const SHAPE_SUFFIXES = ['_fill', '_line', '_circle'];
const POPUP_CLASS = 'whg-gazetteer-popup-anchor';
const LOADING_HTML = '<div class="whg-gazetteer-popup"><div class="popup-loading">Loading…</div></div>';

export default class GazetteerInteraction {
    constructor(map) {
        this.map = map;
        this._currentId = null;
        this._handlers = [];
        this._popup = null;
        this._abortController = null;
    }

    /**
     * Wire mouseenter/mouseleave/click on every shape layer of the
     * supplied gazetteer. Idempotent — calls detach() first.
     *
     * @param {string} id — gazetteer source id (e.g. ``"gn"``, ``"whg-892"``)
     * @param {string[]} baseIds — per-source-layer base IDs that
     *     loadGazetteerStyle constructed (single-layer: ``[id]``;
     *     multi-layer: ``[`${id}__layerA`, `${id}__layerB`, …]``).
     */
    attach(id, baseIds) {
        this.detach();
        if (!this.map || !id || !Array.isArray(baseIds) || baseIds.length === 0) return;
        this._currentId = id;
        for (const baseId of baseIds) {
            for (const suffix of SHAPE_SUFFIXES) {
                const layerId = `${baseId}${suffix}`;
                if (!this.map.getLayer(layerId)) continue;
                const enter = () => this._onEnter();
                const leave = () => this._onLeave();
                const click = (e) => this._onClick(e);
                this.map.on('mouseenter', layerId, enter);
                this.map.on('mouseleave', layerId, leave);
                this.map.on('click', layerId, click);
                this._handlers.push({ layerId, enter, leave, click });
            }
        }
    }

    /** Remove all registered handlers, abort pending fetch, hide popup. */
    detach() {
        if (this._abortController) {
            try { this._abortController.abort(); } catch (e) {}
            this._abortController = null;
        }
        if (this._popup) {
            try { this._popup.remove(); } catch (e) {}
        }
        if (this.map) {
            for (const { layerId, enter, leave, click } of this._handlers) {
                try { this.map.off('mouseenter', layerId, enter); } catch (e) {}
                try { this.map.off('mouseleave', layerId, leave); } catch (e) {}
                try { this.map.off('click', layerId, click); } catch (e) {}
            }
            this.map.getCanvas().style.cursor = '';
        }
        this._handlers = [];
        this._currentId = null;
    }

    _onEnter() {
        if (!this.map) return;
        this.map.getCanvas().style.cursor = 'pointer';
    }

    _onLeave() {
        if (!this.map) return;
        this.map.getCanvas().style.cursor = '';
    }

    _onClick(e) {
        if (!this.map || !e.features || e.features.length === 0) return;
        const placeId = e.features[0].properties && e.features[0].properties.place_id;
        if (!placeId) return;

        if (this._abortController) {
            try { this._abortController.abort(); } catch (err) {}
        }
        this._abortController = new AbortController();
        const signal = this._abortController.signal;

        if (!this._popup) {
            this._popup = new whg_maplibre.Popup({
                closeButton: true,
                // Manage close manually so back-to-back feature clicks
                // re-target the same popup instead of MapLibre's
                // outside-click listener tearing it down before our layer
                // click handler can reposition it.
                closeOnClick: false,
                maxWidth: '400px',
                className: POPUP_CLASS,
            });
        }
        this._popup
            .setLngLat(e.lngLat)
            .setHTML(LOADING_HTML)
            .addTo(this.map);

        const url = `${PREVIEW_PATH}${encodeURIComponent(placeId)}/preview?variant=popup`;
        // ``EntityPreviewView`` extends ``AuthenticatedAPIView``. Anonymous
        // visitors authenticate via the CSRF-token fallback in
        // ``TokenQueryOrBearerAuthentication``: a request bearing a valid
        // ``X-CSRFToken`` is accepted as ``CSRFUser`` (an AnonymousUser
        // whose ``is_authenticated`` is True). ``window.csrfToken`` is set
        // in base.js from the page's ``<meta name="csrf-token">`` tag.
        const headers = {};
        if (typeof window !== 'undefined' && window.csrfToken) {
            headers['X-CSRFToken'] = window.csrfToken;
        }
        fetch(url, { signal, credentials: 'same-origin', headers })
            .then((r) => {
                if (!r.ok) throw new Error(`HTTP ${r.status}`);
                return r.text();
            })
            .then((html) => {
                if (signal.aborted) return;
                this._popup.setHTML(html);
            })
            .catch((err) => {
                if (err.name === 'AbortError') return;
                if (signal.aborted) return;
                this._popup.setHTML(
                    '<div class="whg-gazetteer-popup"><div class="popup-loading">Failed to load place details.</div></div>'
                );
            });
    }
}
