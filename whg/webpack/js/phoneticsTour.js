// phoneticsTour.js
/**
 * Guided tour of the phonetic rule review UI (place#252).
 *
 * Follows the Atlas pattern (driver.js, a localStorage "seen" flag, auto-start
 * on first visit, relaunchable from a button), but deliberately much shorter:
 * this tool asks people for a few seconds of expert judgement, and a long tour
 * spends the attention we are trying to collect.
 *
 * The steps are chosen to answer the three questions a newcomer actually has,
 * in the order they have them: what is this for, what will I be asked to do,
 * and what happens to my answer. Everything else can be discovered.
 *
 * Written for readers whose first language is not English: short sentences,
 * plain words, no idiom.
 */

import { driver } from 'driver.js';
import 'driver.js/dist/driver.css';

const TOUR_SEEN_KEY = 'whg_phonetics_tour_seen';

/** Only include a step whose element is actually on this page. driver.js skips
 *  a missing selector silently, which would leave the tour narrating things the
 *  reader cannot see. */
function present(selector) {
  return selector ? document.querySelector(selector) !== null : true;
}

function steps() {
  const all = [
    {
      popover: {
        title: 'Help us read place names',
        description:
          'WHG matches place names across different writing systems. To do that, it ' +
          'turns each name into sounds. The rules that do this were written by hand, ' +
          'and some of them are wrong or missing.<br><br>' +
          '<strong>We cannot fix them ourselves.</strong> We need people who read the ' +
          'language. That is what this page is for.',
      },
    },
    {
      element: '.phonetics nav a[href$="/phonetics/competence/"]',
      popover: {
        title: 'First, tell us what you read',
        description:
          'Choose the languages you can read. We will then show you only letters from ' +
          'those languages.<br><br>' +
          'You say this about yourself. We do not check it, and we do not treat it as ' +
          'authority.',
        side: 'bottom',
      },
    },
    {
      element: '.phonetics nav a[href$="/phonetics/queue/"]',
      popover: {
        title: 'Then look at one letter at a time',
        description:
          'Each task is small: one letter, and the sound we think it makes. ' +
          'You can accept it, correct it, or say you are not sure.<br><br>' +
          '<strong>You can always skip.</strong> Skipping records nothing. We never ' +
          'treat silence as agreement.',
        side: 'bottom',
      },
    },
    {
      element: '.phonetics nav a[href$="/phonetics/lint/"]',
      popover: {
        title: 'Some rows are simply broken',
        description:
          'A computer can find these without knowing any language — for example, a ' +
          'letter typed with the wrong character.<br><br>' +
          'We list them separately so they never waste your time.',
        side: 'bottom',
      },
    },
    {
      popover: {
        title: 'Missing letters matter most',
        description:
          'In several writing systems the consonants were added and the vowels were ' +
          'forgotten. That is the biggest problem we have found.<br><br>' +
          'On any rule set you can type a real place name, see which letters we cannot ' +
          'read, and <strong>add the ones that are missing</strong>.',
      },
    },
    {
      popover: {
        title: 'What happens to your answer',
        description:
          'Nothing here changes WHG straight away. Your answer is a <strong>suggestion</strong>. ' +
          'A person looks at the suggestions and decides.<br><br>' +
          'If two people disagree, we keep both answers. Your name is kept with your work, ' +
          'if you want it to be.',
      },
    },
    {
      element: '#phonetics-tour-button',
      popover: {
        title: 'You can see this again',
        description: 'Use this button whenever you want to repeat the tour.',
        side: 'top',
      },
    },
  ];
  return all.filter((step) => present(step.element));
}

export function startPhoneticsTour() {
  const instance = driver({
    showProgress: true,
    allowClose: true,
    nextBtnText: 'Next',
    prevBtnText: 'Back',
    doneBtnText: 'Start',
    steps: steps(),
    onDestroyed: () => {
      try { localStorage.setItem(TOUR_SEEN_KEY, 'true'); } catch (e) { /* private mode */ }
    },
  });
  instance.drive();
  return instance;
}

export function hasSeenPhoneticsTour() {
  // A browser that refuses storage must not be shown the tour on every page
  // load; treat "cannot tell" as "seen".
  try { return localStorage.getItem(TOUR_SEEN_KEY) === 'true'; } catch (e) { return true; }
}

export function resetPhoneticsTourFlag() {
  try { localStorage.removeItem(TOUR_SEEN_KEY); } catch (e) { /* no-op */ }
}
