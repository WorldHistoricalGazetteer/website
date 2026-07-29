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
