"""Serving a scratch-built ``locus.bundle.*`` to a Playwright page (place#325 Stage 5).

``WHG_LOCUS_BUNDLE_DIR=<dir>`` makes a browser test fetch ``locus.bundle.js`` and
``locus.bundle.css`` from that directory instead of ``static/webpack/``, so source
not yet bundled can be tested without writing into ``static/webpack/`` (the same
mechanism ``search.tests_atlas_toponym_deeplink`` uses for the Atlas bundle).
Unset, the committed bundles are served. Build one with the scratchpad's
``webpack.only.js``:

    env -u ENV_CONTEXT ONLY=locus OUT_DIR=<dir> node_modules/.bin/webpack --config <scratch>/webpack.only.js
"""
import os

LOCUS_BUNDLE_DIR = os.environ.get("WHG_LOCUS_BUNDLE_DIR") or ""


def _bundle_route(route, request):
    name = request.url.rsplit("/", 1)[-1].split("?", 1)[0]
    path = os.path.join(LOCUS_BUNDLE_DIR, name)
    if os.path.isfile(path):
        ctype = "text/css" if name.endswith(".css") else "application/javascript"
        with open(path, "rb") as f:
            route.fulfill(status=200, content_type=ctype, body=f.read())
    else:
        route.continue_()


def route_locus_bundle(ctx):
    """Point ``ctx`` (a Playwright BrowserContext) at the scratch bundle, if one is set.
    Returns where the bundle comes from, for the test's log line."""
    if LOCUS_BUNDLE_DIR:
        ctx.route("**/static/webpack/locus.bundle.*", _bundle_route)
        return LOCUS_BUNDLE_DIR
    return "static/webpack/"
