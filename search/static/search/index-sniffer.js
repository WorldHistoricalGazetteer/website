// Index sniffer for the search page.
//
// The site depends on the Elasticsearch indexing server; when it is down,
// search and place lookups cannot run. This script polls a cheap server-side
// liveness endpoint and, while the index is unreachable, overlays a
// hard-blocking notice on the page <main>. It removes the overlay automatically
// once the index recovers, so no page reload is needed.
//
// Deliberately dependency-free (no jQuery/Bootstrap) so it still works if the
// webpack bundle fails to load.

(function () {
    var ENDPOINT = '/search/health/';
    var POLL_MS = 15000;
    var OVERLAY_ID = 'index-down-overlay';

    function getMain() {
        return document.querySelector('main');
    }

    function showOverlay() {
        var host = getMain();
        if (!host || document.getElementById(OVERLAY_ID)) {
            return;
        }
        host.classList.add('index-down-host');

        var overlay = document.createElement('div');
        overlay.id = OVERLAY_ID;
        overlay.setAttribute('role', 'alertdialog');
        overlay.setAttribute('aria-live', 'assertive');
        overlay.setAttribute('aria-label', 'Search temporarily unavailable');
        overlay.innerHTML =
            '<div class="index-down-card">' +
                '<div class="index-down-icon"><i class="fas fa-exclamation-triangle"></i></div>' +
                '<h2>Search is temporarily unavailable</h2>' +
                '<p>Our indexing server is not responding, so search and place lookups cannot run right now.</p>' +
                '<p>This page will re-enable itself automatically as soon as the service is restored — no need to reload.</p>' +
                '<p class="index-down-status">Re-checking every 15&nbsp;seconds…</p>' +
            '</div>';
        host.appendChild(overlay);
    }

    function hideOverlay() {
        var overlay = document.getElementById(OVERLAY_ID);
        if (overlay) {
            overlay.remove();
        }
        var host = getMain();
        if (host) {
            host.classList.remove('index-down-host');
        }
    }

    function check() {
        fetch(ENDPOINT, { headers: { Accept: 'application/json' }, cache: 'no-store' })
            .then(function (resp) {
                return resp.ok ? resp.json() : { status: 'down' };
            })
            .then(function (data) {
                if (data && data.status === 'up') {
                    hideOverlay();
                } else {
                    showOverlay();
                }
            })
            .catch(function () {
                // Network error / endpoint unreachable: treat as down.
                showOverlay();
            });
    }

    function start() {
        check();
        setInterval(check, POLL_MS);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', start);
    } else {
        start();
    }
})();
