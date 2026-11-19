// whg/webpack/js/edopsCharts.js
//
// Client-side SVG charts for the EDOPS modal, drawn from the whgmodal payload's
// ``series`` block (api/edops_whgmodal.py; developer/edops-whgmodal-payload-proposal.md).
// d3-scale + d3-shape + d3-array only (ISC); no DOM library: every chart is a pure
// function from data to an HTML string, so it is accessible by construction:
//   * <svg role="img"> with <title> and <desc>, and the same numbers in a
//     "Show the values" <table>, so no value is available only as a picture;
//   * the caption says what is drawn, at what scale, from which source, and, for
//     reconstructed values, says so and carries EDOPS's own caveats.
// Colours are CSS variables (edopsModal.css) so a theme can change them.

import { scaleLinear, scaleBand } from 'd3-scale';
import { line as d3line } from 'd3-shape';
import { extent, max } from 'd3-array';

const W = 520;
const H = 190;
const M = { t: 12, r: 14, b: 30, l: 46 };

export function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

const fmt = (v) => (v == null || Number.isNaN(Number(v)) ? '' : String(Math.round(Number(v) * 100) / 100));
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
let seq = 0;

function axisY(y, unit) {
    return y.ticks(4).map((t) => `<g><line class="ec-grid" x1="${M.l}" x2="${W - M.r}" y1="${y(t)}" y2="${y(t)}"/>`
        + `<text class="ec-tick" x="${M.l - 6}" y="${y(t)}" text-anchor="end" dominant-baseline="middle">${esc(fmt(t))}</text></g>`).join('')
        + `<text class="ec-unit" x="4" y="${M.t + 4}" dominant-baseline="hanging">${esc(unit)}</text>`;
}

function figure({ id, title, desc, svg, rows, head, caption, caveats = [] }) {
    const table = `<details class="ec-table"><summary>Show the values as a table</summary><table><thead><tr>${head.map((h) => `<th scope="col">${esc(h)}</th>`).join('')}</tr></thead>`
        + `<tbody>${rows.map((r) => `<tr>${r.map((c, i) => (i === 0 ? `<th scope="row">${esc(c)}</th>` : `<td>${esc(c)}</td>`)).join('')}</tr>`).join('')}</tbody></table></details>`;
    const cv = caveats.length ? `<ul class="ec-caveats">${caveats.map((c) => `<li>${esc(c)}</li>`).join('')}</ul>` : '';
    return `<figure class="ec-figure" data-chart="${esc(id)}"><figcaption><strong>${esc(title)}</strong> <span class="ec-cap">${caption}</span></figcaption>`
        + `${svg}${cv}${table}</figure>`;
}

function frame(n, title, desc, body) {
    return `<svg class="ec-svg" viewBox="0 0 ${W} ${H}" role="img" aria-labelledby="ec${n}t ec${n}d" preserveAspectRatio="xMidYMid meet">`
        + `<title id="ec${n}t">${esc(title)}</title><desc id="ec${n}d">${esc(desc)}</desc>${body}</svg>`;
}

/** Bars for monthly values (precipitation) or a line for monthly temperature. */
function monthly(kind, values, { id, title, unit, source, period }) {
    const n = ++seq;
    const x = scaleBand().domain(MONTHS).range([M.l, W - M.r]).padding(0.2);
    const [lo, hi] = extent(values);
    const y = scaleLinear().domain(kind === 'bar' ? [0, max(values)] : [Math.min(0, lo), hi]).nice().range([H - M.b, M.t]);
    let marks;
    if (kind === 'bar') {
        marks = values.map((v, i) => `<rect class="ec-bar ec-s1" x="${x(MONTHS[i])}" y="${y(v)}" width="${x.bandwidth()}" height="${Math.max(0, y(0) - y(v))}"><title>${MONTHS[i]}: ${esc(fmt(v))} ${esc(unit)}</title></rect>`).join('');
    } else {
        const ln = d3line().x((_, i) => x(MONTHS[i]) + x.bandwidth() / 2).y((v) => y(v));
        marks = `<path class="ec-line ec-s2" d="${ln(values)}"/>` + values.map((v, i) => `<circle class="ec-dot ec-s2" cx="${x(MONTHS[i]) + x.bandwidth() / 2}" cy="${y(v)}" r="2.6"><title>${MONTHS[i]}: ${esc(fmt(v))} ${esc(unit)}</title></circle>`).join('');
    }
    const xAxis = MONTHS.map((m) => `<text class="ec-tick" x="${x(m) + x.bandwidth() / 2}" y="${H - M.b + 14}" text-anchor="middle">${m}</text>`).join('');
    const desc = `${kind === 'bar' ? 'Bar' : 'Line'} chart of twelve monthly values from ${fmt(lo)} to ${fmt(hi)} ${unit}.`;
    return figure({
        id, title, desc, head: ['Month', unit],
        rows: MONTHS.map((m, i) => [m, fmt(values[i])]),
        svg: frame(n, title, desc, axisY(y, unit) + marks + xAxis),
        caption: `<span class="ec-scale">Scale: basin.</span> ${esc(source)}; ${esc(period)}.`,
    });
}

/** Per-year line (anomaly) with zero line, optional volcanic-event ticks. */
function timeline(years, values, { id, title, unit, events, caveats, source }) {
    const n = ++seq;
    const pts = years.map((yr, i) => [yr, values[i]]).filter((p) => p[1] != null);
    const x = scaleLinear().domain(extent(years)).range([M.l, W - M.r]);
    const [lo, hi] = extent(pts, (p) => p[1]);
    const y = scaleLinear().domain([Math.min(lo, 0), Math.max(hi, 0)]).nice().range([H - M.b, M.t]);
    const ln = d3line().x((p) => x(p[0])).y((p) => y(p[1]));
    const zero = `<line class="ec-zero" x1="${M.l}" x2="${W - M.r}" y1="${y(0)}" y2="${y(0)}"/>`;
    const ev = (events || []).filter((e) => e.year >= years[0] && e.year <= years[years.length - 1])
        .map((e) => `<line class="ec-event" x1="${x(e.year)}" x2="${x(e.year)}" y1="${M.t}" y2="${H - M.b}"><title>${e.year} CE: volcanic event, ${esc(fmt(e.vssi_tg))} Tg sulphate${e.location ? ', ' + esc(e.location) : ''}</title></line>`).join('');
    const xAxis = x.ticks(6).map((t) => `<text class="ec-tick" x="${x(t)}" y="${H - M.b + 14}" text-anchor="middle">${t}</text>`).join('');
    const desc = `Line chart of ${pts.length} yearly values, ${years[0]} to ${years[years.length - 1]} CE, ranging ${fmt(lo)} to ${fmt(hi)} ${unit}.`;
    return figure({
        id, title, desc, head: ['Year (CE)', unit],
        rows: pts.map((p) => [p[0], fmt(p[1])]),
        svg: frame(n, title, desc, axisY(y, unit) + zero + ev + `<path class="ec-line ec-s1" d="${ln(pts)}"/>` + xAxis),
        caption: `<span class="ec-scale">Scale: one ${esc('2°×2°')} climate-model grid cell, not the place.</span> <em>Reconstructed</em>, not observed; ${esc(source)}.${(events || []).length ? ' Dashed verticals: volcanic events.' : ''}`,
        caveats,
    });
}

/** Grouped bars: land-use area per HYDE epoch. */
function landUse(block) {
    const n = ++seq;
    const keys = [['cropland_km2', 'Cropland', 'ec-s1'], ['grazing_km2', 'Grazing', 'ec-s2'], ['pasture_km2', 'Pasture', 'ec-s3'], ['rangeland_km2', 'Rangeland', 'ec-s4']]
        .filter(([k]) => block.epochs.some((e) => e[k] > 0));
    const eps = block.epochs.map((e) => String(e.year));
    const x0 = scaleBand().domain(eps).range([M.l, W - M.r]).padding(0.25);
    const x1 = scaleBand().domain(keys.map((k) => k[0])).range([0, x0.bandwidth()]).padding(0.08);
    const top = max(block.epochs, (e) => max(keys, (k) => e[k[0]] || 0)) || 1;
    const y = scaleLinear().domain([0, top]).nice().range([H - M.b, M.t]);
    const bars = block.epochs.map((e) => keys.map(([k, lbl, cls]) => `<rect class="ec-bar ${cls}" x="${x0(String(e.year)) + x1(k)}" y="${y(e[k] || 0)}" width="${x1.bandwidth()}" height="${Math.max(0, y(0) - y(e[k] || 0))}"><title>${e.year} CE ${lbl}: ${esc(fmt(e[k]))} km²</title></rect>`).join('')).join('');
    const xAxis = eps.map((e) => `<text class="ec-tick" x="${x0(e) + x0.bandwidth() / 2}" y="${H - M.b + 14}" text-anchor="middle">${e} CE</text>`).join('');
    const legend = `<ul class="ec-legend">${keys.map(([, lbl, cls]) => `<li><span class="ec-swatch ${cls}"></span>${lbl}</li>`).join('')}</ul>`;
    const desc = `Grouped bar chart of land use in km², for ${eps.join(' and ')} CE.`;
    const f = figure({
        id: 'land_use', title: 'Land use in the basin (km²)', desc, head: ['Year (CE)', ...keys.map((k) => `${k[1]} (km²)`)],
        rows: block.epochs.map((e) => [e.year, ...keys.map((k) => fmt(e[k[0]]))]),
        svg: frame(n, 'Land use in the basin', desc, axisY(y, 'km²') + bars + xAxis),
        caption: `<span class="ec-scale">Scale: basin${block.basin_area_km2 ? ` (${esc(fmt(block.basin_area_km2))} km²)` : ''}.</span> <em>Modelled reconstruction</em>; ${esc(block.source)}.`,
        caveats: block.caveats,
    });
    return f.replace('</svg>', `</svg>${legend}`);
}

/** All charts the payload supports, as {html, count}. ``series`` is whgmodal.series. */
export function renderCharts(series) {
    seq += 1;
    const out = [];
    if (!series) return { html: '', count: 0 };
    const m = series.monthly;
    if (m && m.precipitation_mm) out.push(monthly('bar', m.precipitation_mm, { id: 'monthly_precipitation', title: 'Monthly precipitation', unit: 'mm', source: m.source, period: m.period }));
    if (m && m.temperature_c) out.push(monthly('line', m.temperature_c, { id: 'monthly_temperature', title: 'Monthly temperature', unit: '°C', source: m.source, period: m.period }));
    const t = series.timeline;
    const events = series.volcanic && series.volcanic.events;
    if (t) {
        [['pdsi', 'timeline_pdsi', 'Drought index (PDSI) anomaly', 'index; below 0 is drier'],
            ['air_anom_k', 'timeline_temperature', 'Temperature anomaly', 'K'],
            ['prate_anom_mm_day', 'timeline_precipitation', 'Precipitation-rate anomaly', 'mm/day']].forEach(([k, id, title, unit]) => {
            if (t[k]) out.push(timeline(t.years, t[k], { id, title, unit, events, caveats: t.caveats, source: t.source }));
        });
    }
    if (series.land_use && series.land_use.epochs && series.land_use.epochs.length) out.push(landUse(series.land_use));
    return { html: out.join(''), count: out.length };
}
