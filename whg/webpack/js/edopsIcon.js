// whg/webpack/js/edopsIcon.js
//
// The EDOPS mark for buttons and headings (the beta Environment card, modal and Locus button).
//
// Source: edops-editable.svg, the vector logo supplied by Karl Grossner (2026-11-20), served as
// the static file /static/images/edops.svg (whg/static/images/edops.svg). The viewBox is cropped to
// the artwork (the paths are untouched); the SVG is transparent, so no blend mode is needed.
// Used at Stephen Gadd's direction; Karl Grossner agreed in principle (email, 2026-11-16).
// STILL OPEN: terms of use / attribution wording. When it arrives, change ONLY this file.

const MARK = '/static/images/edops.svg';

/** @param {string} [cls] extra class names. Decorative: alt is empty, the label beside it names it. */
export function edopsIconHTML(cls = '') {
    return `<img class="edops-icon ${cls}" src="${MARK}" alt="" width="33" height="24" aria-hidden="true">`;
}
