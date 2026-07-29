#!/usr/bin/env node
/**
 * Audit the licences of the JavaScript WHG actually SHIPS.
 *
 * `license-checker --production` reports the whole production dependency tree
 * (~660 packages here), but webpack only compiles what is genuinely imported —
 * so that number materially overstates what reaches a user. This script asks
 * webpack itself which modules ended up in the bundles, maps them back to their
 * packages, and reads each package's own metadata.
 *
 * Writes licensing/data/js_licenses.json, which `manage.py audit_licenses`
 * merges with the Python half. Run it whenever dependencies change:
 *
 *     npm run audit:licenses
 */
const fs = require('fs');
const path = require('path');
const webpack = require('webpack');

const ROOT = path.resolve(__dirname, '..');
const OUT = path.join(ROOT, 'licensing', 'data', 'js_licenses.json');

/** Package name from a module path, handling @scope/name. */
function packageOf(modulePath) {
  const parts = modulePath.split(/[\\/]node_modules[\\/]/);
  if (parts.length < 2) return null;                 // first-party source
  const tail = parts[parts.length - 1].split(/[\\/]/);
  return tail[0].startsWith('@') ? `${tail[0]}/${tail[1]}` : tail[0];
}

/** Read a package's declared licence + repository from its own package.json. */
function describe(name) {
  try {
    // Read the manifest by PATH, not via require.resolve: modern packages declare
    // an `exports` map that deliberately does not expose ./package.json, so
    // resolving it throws ERR_PACKAGE_PATH_NOT_EXPORTED and the package looks
    // undeclared when it is nothing of the kind (@hocuspocus/provider,
    // onnxruntime-web and five others all tripped this).
    let pkgPath = path.join(ROOT, 'node_modules', ...name.split('/'), 'package.json');
    if (!fs.existsSync(pkgPath)) {
      pkgPath = require.resolve(`${name}/package.json`, { paths: [ROOT] });
    }
    const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf8'));
    let license = pkg.license;
    if (!license && Array.isArray(pkg.licenses)) {
      // Long-deprecated array form, still used by a few old packages.
      license = pkg.licenses.map((l) => l.type || l).join(' OR ');
    }
    if (license && typeof license === 'object') license = license.type || '';
    let url = '';
    if (typeof pkg.repository === 'string') url = pkg.repository;
    else if (pkg.repository && pkg.repository.url) url = pkg.repository.url;
    url = url.replace(/^git\+/, '').replace(/\.git$/, '').replace(/^git:\/\//, 'https://');
    return { version: pkg.version || '', license: license || '', url: url || pkg.homepage || '' };
  } catch (e) {
    return { version: '', license: '', url: '' };
  }
}

const config = require(path.join(ROOT, 'webpack.config.js'));
// The config may export a function of (env, argv) — normalise to an object, and
// force production so we audit what actually ships.
const resolved = typeof config === 'function' ? config({}, { mode: 'production' }) : config;
const configs = Array.isArray(resolved) ? resolved : [resolved];
configs.forEach((c) => {
  c.mode = 'production';
  // Disable the filesystem cache for the audit. A fully-cached build emits stats
  // with an EMPTY module list — nothing was rebuilt, so nothing is reported — and
  // the audit would silently conclude that WHG bundles no third-party code at all.
  // Slower, but the whole point of this script is that its output is trustworthy.
  c.cache = false;
});

console.log('Compiling to discover bundled modules (this takes a minute)…');
webpack(configs, (err, stats) => {
  if (err) { console.error(err); process.exit(1); }
  if (stats.hasErrors()) {
    console.error(stats.toString({ errors: true, all: false }));
    process.exit(1);
  }

  const bundled = new Set();
  const json = stats.toJson({ all: false, modules: true, chunks: false });
  const walk = (mods) => (mods || []).forEach((m) => {
    if (m.identifier) {
      const pkg = packageOf(m.identifier);
      if (pkg) bundled.add(pkg);
    }
    if (m.modules) walk(m.modules);              // concatenated/inner modules
  });
  (json.children || [json]).forEach((child) => walk(child.modules));

  const direct = new Set(Object.keys(
    JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8')).dependencies || {}));

  const packages = [...bundled].sort().map((name) => {
    const d = describe(name);
    return {
      name,
      version: d.version,
      license: d.license || 'Not declared',
      url: d.url,
      direct: direct.has(name),
    };
  });

  if (!packages.length) {
    console.error('Resolved 0 bundled packages — refusing to write. The stats had no '
                  + 'module list (a cached build will do this).');
    process.exit(1);
  }

  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify({
    generated: new Date().toISOString().slice(0, 10),
    count: packages.length,
    packages,
  }, null, 1) + '\n');

  const undeclared = packages.filter((p) => p.license === 'Not declared');
  console.log(`Wrote ${path.relative(ROOT, OUT)} — ${packages.length} bundled packages.`);
  if (undeclared.length) {
    console.log(`${undeclared.length} declare no licence: ${undeclared.map((p) => p.name).join(', ')}`);
  }
});
