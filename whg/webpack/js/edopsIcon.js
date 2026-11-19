// whg/webpack/js/edopsIcon.js
//
// The icon for EDOPS buttons and headings (the beta Environment card and Locus button).
//
// 🛑 PLACEHOLDER. This is NOT the EDOPS logo: it is a deliberately plain stand-in (a
// dashed outline with a small wave) so that no one mistakes it for, or ships it as, the
// real mark. Karl Grossner has agreed in principle that WHG may use the EDOPS logo here;
// still needed from him: a transparent SVG (or PNG) that reads at 16-24 px, and the
// terms of use. When it arrives, put the file in whg/webpack/images/ and change ONLY the
// body of edopsIconHTML() (everything that shows the icon calls it).

/** @param {string} [cls] extra class names. Decorative: aria-hidden, the label beside it names it. */
export function edopsIconHTML(cls = '') {
    return `<svg class="edops-icon ${cls}" data-placeholder="true" viewBox="0 0 24 24" width="1.15em" height="1.15em"
        aria-hidden="true" focusable="false"><rect x="2" y="2" width="20" height="20" rx="5" fill="none"
        stroke="currentColor" stroke-width="1.6" stroke-dasharray="3 2.2"/><path d="M5.5 14.5c2-3.2 3.7-3.2 5.7 0s3.7 3.2 5.7 0"
        fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/></svg>`;
}
