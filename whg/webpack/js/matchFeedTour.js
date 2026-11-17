// matchFeedTour.js
/**
 * Guided tour of "Match your places" (place#338), driver.js, same pattern as the phonetics tour:
 * a localStorage "seen" flag, auto-start once on a first visit, relaunchable from the page.
 *
 * It only highlights. It never clicks, marks, or saves anything, and it uses whatever record the
 * person already has on screen (their own data), so it does not need a demo record. A step whose
 * element is not on the page right now (for example the map for a record with no location) is left
 * out rather than narrated. If the tab has no record at all, it says so and offers to try again.
 *
 * Written for readers whose first language may not be English: short sentences, plain words.
 */
import { driver } from 'driver.js';
import 'driver.js/dist/driver.css';

const SEEN_KEY = 'whg_match_tour_seen';
const present = (sel) => !sel || document.querySelector(sel) !== null;

function steps() {
  const all = [
    { popover: {
      title: 'Match your places',
      description: 'Some places in your dataset may already be in WHG, from source gazetteers such as GeoNames or Wikidata. ' +
        'This page suggests them. <strong>You decide.</strong> Nothing is merged and nothing in your dataset changes.' } },
    { element: '[data-tour="tabs"]', popover: { side: 'bottom', title: 'Your records, sorted',
      description: 'We sort your records by how sure we are. Start with <strong>Likely the same place</strong>. ' +
        '<strong>Nothing found</strong> is normal for many records; you can leave them.' } },
    { element: '[data-tour="queue"]', popover: { side: 'right', title: 'One record at a time',
      description: 'These are the records in this tab. Pick one to look at its suggestions. Decided records move to the end.' } },
    { element: '[data-tour="record"]', popover: { side: 'bottom', title: 'Your record',
      description: 'This is your record: its names, type and place. Compare it with each suggestion below.' } },
    { element: '[data-tour="candidates"]', popover: { side: 'top', title: 'The suggestions',
      description: 'Each row is a place from another source. We say why it was suggested, and we warn you if something does not agree, ' +
        'for example a different type of place or a long distance.' } },
    { element: '[data-tour="row-state"]', popover: { side: 'top', title: 'Say what you think',
      description: '<strong>Same place</strong>, <strong>Not the same</strong> or <strong>Can\'t tell</strong>. ' +
        'Nothing is chosen for you. Only the rows you mark are saved; the others are left as they are.' } },
    { element: '[data-tour="map"]', popover: { side: 'left', title: 'The map',
      description: 'Your record and its suggestions. A row you point at is lit on the map. ' +
        'Clicking a marker only takes you to its row. <strong>It decides nothing.</strong> ' +
        'Arrows at the edge point to suggestions that are too far away to show.' } },
    { element: '[data-tour="summary"]', popover: { side: 'top', title: 'What Save will do',
      description: 'This line tells you exactly what will be recorded before you press Save.' } },
    { element: '[data-tour="save"]', popover: { side: 'top', title: 'Save your decision',
      description: '"Same place" records your own statement that the two records describe one place. ' +
        'Your record is not changed. You can undo it.' } },
    { element: '[data-tour="none"]', popover: { side: 'top', title: 'None of these',
      description: 'If no suggestion is right, say so. That is useful to us too.' } },
    { element: '[data-tour="later"]', popover: { side: 'top', title: 'Not sure yet',
      description: 'Not ready to decide? Move the record to <strong>Not sure yet</strong>. Nothing expires.' } },
    { element: '[data-tour="search"]', popover: { side: 'top', title: 'Search by hand',
      description: 'If you know another spelling, search for it here. The results appear on the map as well.' } },
    { element: '#mf_shortcuts', popover: { side: 'top', title: 'Keyboard shortcuts',
      description: 'The keyboard shortcuts are off. Switch them on if you want to work fast without the mouse.' } },
    { element: '#mf_tour_button', popover: { side: 'bottom', title: 'See this again',
      description: 'Use this button whenever you want to repeat the tour.' } },
  ];
  return all.filter((s) => present(s.element));
}

export function startMatchTour() {
  const flag = window.__whgMatchFeed;
  const list = steps();
  if (!document.querySelector('[data-tour="candidates"] .mf-row')) {
    // No suggestions on screen: do not narrate controls that are not there.
    list.splice(1, list.length, ...list.filter((s) => !s.element || /tabs|queue|tour_button|shortcuts/.test(s.element)));
    list.splice(1, 0, { popover: { title: 'Open a record to see more',
      description: 'This tab has no suggestions on screen. Open a record in another tab, then take the tour again from the button at the top.' } });
  }
  if (flag) flag.touring = true;
  const instance = driver({
    showProgress: true, allowClose: true, nextBtnText: 'Next', prevBtnText: 'Back', doneBtnText: 'Done',
    steps: list,
    // onDestroyStarted runs for every way out (Escape, close button, Done, overlay click);
    // onDestroyed does not run when the step had no element of its own.
    onDestroyStarted: (el, step, ctx) => {
      if (flag) flag.touring = false;
      try { localStorage.setItem(SEEN_KEY, 'true'); } catch (e) { /* private mode */ }
      ctx.driver.destroy();
    },
  });
  instance.drive();
  return instance;
}

export function hasSeenMatchTour() {
  try { return localStorage.getItem(SEEN_KEY) === 'true'; } catch (e) { return true; }  // cannot tell: do not nag
}
