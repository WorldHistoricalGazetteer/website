// tooltipHygiene.js
//
// Bootstrap only dismisses a hover-triggered tooltip when a `mouseleave` fires
// on its trigger. Two common situations never produce one, and the tooltip is
// then stranded on screen until the next click:
//
//   1. Wheel-scrolling with a stationary pointer. The trigger slides out from
//      under the cursor, but the browser fires no boundary event (Chrome only
//      re-evaluates the hovered element on the next real pointer movement), so
//      Popper simply keeps re-anchoring the tip to a trigger the user is no
//      longer pointing at.
//   2. The trigger is removed from the DOM while its tooltip is shown — e.g. a
//      virtualised list (the Atlas Gazetteer place list) recycling its rows.
//      No further event can ever reach the detached trigger, so the tip element
//      is orphaned in <body> at its last computed position.
//
// Dismissing tooltips on scroll is also what users expect (it is what Material
// and the native `title` tooltip both do), so we treat any scroll anywhere as
// a dismissal. Cost in the common case is one boolean test per scroll event.

/** Hide every shown Bootstrap tooltip; remove the ones whose trigger is gone.
 *  Returns true if any were found. */
export function dismissTooltips() {
    const bs = window.bootstrap;
    // `role="tooltip"` distinguishes Bootstrap's tip element from our own
    // `.tooltip` markup (e.g. the dateline slider readout).
    const tips = document.querySelectorAll('.tooltip.show[role="tooltip"]');
    tips.forEach(tip => {
        // A shown tooltip's trigger carries aria-describedby="<tip id>"; if no
        // element does, the trigger has been detached and nothing will ever
        // hide this tip — drop it. (Bootstrap re-appends its cached tip element
        // on the next show, so removing it here is safe.)
        const trigger = tip.id
            ? document.querySelector(`[aria-describedby="${tip.id}"]`)
            : null;
        const inst = trigger && bs && bs.Tooltip ? bs.Tooltip.getInstance(trigger) : null;
        if (inst) {
            try { inst.hide(); } catch (e) { tip.remove(); }
        } else {
            tip.remove();
        }
    });
    return tips.length > 0;
}

/** Install the global scroll safety-net. Idempotent. */
export function initTooltipHygiene() {
    if (window.__whgTooltipHygiene) return;
    window.__whgTooltipHygiene = true;

    // Hint flag so the scroll handler is a no-op unless a tooltip has actually
    // been shown since the last clean sweep. `shown.bs.tooltip` is dispatched on
    // the trigger and does not bubble, but capture-phase listeners on `document`
    // see every event targeted at a node in the document regardless.
    let maybeShown = false;
    document.addEventListener('shown.bs.tooltip', () => { maybeShown = true; }, true);

    let queued = false;
    document.addEventListener('scroll', () => {
        if (!maybeShown || queued) return;
        queued = true;
        requestAnimationFrame(() => {
            queued = false;
            maybeShown = dismissTooltips();
        });
    }, {
        // `scroll` does not bubble, so a listener on an inner scroll container
        // (the place list, a modal body, a table) would otherwise be missed —
        // capture catches them all with a single listener.
        capture: true,
        passive: true,
    });

    // Same reasoning for a full-page navigation restore / resize: the anchor
    // geometry changes without any pointer event.
    window.addEventListener('resize', () => { if (maybeShown) maybeShown = dismissTooltips(); }, { passive: true });
}

/** Change a tooltip's text after its trigger has been hovered at least once.
 *
 *  Setting `data-bs-title` is not enough on its own, and the reason is not
 *  obvious. base.js configures ONE delegated tooltip on <body> whose `title` is
 *  a callback reading that attribute at show time — but delegation builds a
 *  separate Tooltip instance per element on first hover, and Bootstrap's
 *  `_getDelegateConfig()` omits any config value equal to the shared Default,
 *  which is exactly where our callback lives. The per-element config therefore
 *  falls back to the `data-bs-title` data attribute, captured once as a plain
 *  string, and `getOrCreateInstance` reuses that instance for the life of the
 *  element. A toggle button relabelled on click keeps announcing the state the
 *  user has just left.
 *
 *  Disposing the instance is what actually works: it takes down any tip on
 *  screen and lets the next hover build a fresh one from the current attribute.
 */
export function setTooltipText(el, text) {
    const node = (el && el.jquery) ? el[0] : el;
    if (!node) return;
    node.setAttribute('data-bs-title', text);
    // Bootstrap writes this one itself when a trigger uses a plain `title`;
    // keep it in step for anything that reads it.
    if (node.hasAttribute('data-bs-original-title')) {
        node.setAttribute('data-bs-original-title', text);
    }
    const Tip = (window.bootstrap && window.bootstrap.Tooltip) ||
        (window.jQuery && window.jQuery.fn.tooltip && window.jQuery.fn.tooltip.Constructor);
    try {
        const inst = Tip && Tip.getInstance(node);
        if (inst) inst.dispose();
    } catch (e) { /* never yet hovered, or already disposed */ }
}
