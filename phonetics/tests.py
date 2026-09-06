"""Tests for the grapheme→IPA review app (place#252).

The acceptance criteria of that issue are mostly *negative* — things that must
be impossible — so most of what follows asserts that a bad thing fails. Where a
test asserts an absence it also asserts a presence in the same method, because
an absence on its own passes just as well when the subject never loaded.
"""

import hashlib
import os
import unicodedata

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.urls import reverse

from .export import build, resolve, suggestions_payload
from .iso import parse_accept_language
from .lint import lint_rows, lint_value
from .models import (Confidence, ContributionTerms, NewRuleProposal, PolicyAnswer,
                     PolicyQuestion, Posture, Review, ReviewerAgreement,
                     ReviewerCompetence, Rule, RuleSet, RuleSetVersion, Verdict,
                     active_terms)
from .routing import queue, ruleset_progress, suggested_rulesets
from .sync import apply_ruleset, parse_csv
from .transcribe import build_map, compare, transcribe
from .validation import codepoints, nfd, panphon_provenance, validate_ipa

User = get_user_model()


def make_user(username, **extra):
    """WHG's UserManager requires email, given_name and surname positionally."""
    return User.objects.create_user(
        username=username, email=f'{username}@example.org', password='x',
        given_name=username.title(), surname='Reviewer', **extra)


class PanphonPinTests(TestCase):
    """The pin is a parity claim, so it is checked rather than commented.

    ``panphon==0.22.0`` is not the newest release: 0.22.1+ need pandas 2.1 and
    this project is on 1.4.1. The pin is only defensible because 0.22.0 ships the
    same segment inventory as the 0.22.2 the indexing host installs — measured
    across every distinct value in the 115 shipped rule sets, with no
    disagreement. If a future bump changes ``ipa_all.csv``, validation here stops
    predicting what the consumer will do, and this test is the thing that says so.
    """

    # sha256 of panphon/data/ipa_all.csv, identical in 0.22.0 and 0.22.2.
    EXPECTED_IPA_ALL = '0ec0052edf4e58c8c23eda10c0195687eb167ce9bd206cf9a85b9cce8b181f0a'

    def test_ipa_inventory_matches_the_consumer(self):
        provenance = panphon_provenance()
        self.assertEqual(provenance['panphon_version'], '0.22.0')
        self.assertEqual(
            provenance['ipa_all_sha256'], self.EXPECTED_IPA_ALL,
            "PanPhon's segment inventory has changed. Validation here no longer "
            "predicts what the indexing host's PanPhon will do with a proposed "
            "value. Re-measure parity against the version installed there before "
            "updating this digest.")

    def test_the_pin_is_the_file_we_actually_loaded(self):
        # Guards against the digest being asserted from a stale constant while
        # a different panphon is on the path.
        import panphon
        path = os.path.join(os.path.dirname(panphon.__file__), 'data', 'ipa_all.csv')
        with open(path, 'rb') as fh:
            self.assertEqual(hashlib.sha256(fh.read()).hexdigest(), self.EXPECTED_IPA_ALL)


class ValidationTests(TestCase):

    def test_a_good_value_passes_and_reports_its_segments(self):
        value, errors, segments = validate_ipa('kʰ')
        self.assertEqual(errors, [])
        self.assertEqual(segments, ['kʰ'])
        self.assertEqual(value, 'kʰ')

    def test_silently_truncated_values_are_rejected(self):
        """The defect class no ``try: parse()`` check can see.

        PanPhon raises nothing for either of these and returns something
        shorter. Accepting them would record a contrast that never reaches the
        matching model.
        """
        for value, survives in [('ⁿɡ', 'ɡ'), ('dʒʰ', 'dʒ'), ('ɡʱ', 'ɡ'), ('r̩ː', 'r̩')]:
            with self.subTest(value=value):
                _, errors, _ = validate_ipa(value)
                self.assertEqual([e['code'] for e in errors], ['lossy'])
                self.assertIn(survives, errors[0]['message'])

    def test_ascii_g_is_named_not_merely_rejected(self):
        _, errors, _ = validate_ipa('g')
        self.assertEqual([e['code'] for e in errors], ['confusable'])
        self.assertIn('U+0261', errors[0]['message'])
        # …and the right character passes, so this is not a test that rejects everything.
        self.assertEqual(validate_ipa('ɡ')[1], [])

    def test_withdrawn_ligatures_are_named_and_expanded(self):
        """Two shipped rows use ʤ. "Not recognised" would not tell anyone what to do."""
        _, errors, _ = validate_ipa('ʤ')
        self.assertEqual([e['code'] for e in errors], ['ligature'])
        self.assertIn('dʒ', errors[0]['message'])
        self.assertEqual(validate_ipa('dʒ')[1], [])

    def test_the_empty_set_glyph_is_rejected_but_a_blank_value_is_not(self):
        self.assertEqual([e['code'] for e in validate_ipa('∅')[1]], ['empty_set_glyph'])
        # Blank is how Epitran spells "produces nothing"; 37 shipped rows use it.
        self.assertEqual(validate_ipa('')[1], [])

    def test_composed_and_decomposed_forms_agree(self):
        """Without NFD normalisation this pair disagrees, and the composed form —
        the one a reviewer's keyboard produces — is wrongly called lossy."""
        composed = unicodedata.normalize('NFC', 'ẽ')
        decomposed = unicodedata.normalize('NFD', 'ẽ')
        self.assertNotEqual(composed, decomposed)  # the trap is real, not hypothetical
        self.assertEqual(validate_ipa(composed)[1], [])
        self.assertEqual(validate_ipa(decomposed)[1], [])
        self.assertEqual(validate_ipa(composed)[0], validate_ipa(decomposed)[0])

    def test_codepoints_are_shown_for_confusables(self):
        self.assertEqual(codepoints('ɡ'), 'U+0261')
        self.assertEqual(codepoints('g'), 'U+0067')


class LintTests(TestCase):

    def test_the_three_defect_classes(self):
        self.assertEqual(lint_value('g'), ['ascii_g'])
        self.assertEqual(lint_value('∅'), ['empty_set_glyph'])
        self.assertEqual(lint_value('dʒʰ'), ['lossy'])
        self.assertEqual(lint_value('ɡ'), [])
        self.assertEqual(lint_value(''), [])

    def test_precomposed_vowels_are_not_defects(self):
        """Linting before normalising invents defects that are not there.

        These five values are the whole difference between "108 defective rows in
        42 files" and the truth, which is 103 in 41: a lint run over the raw file
        calls every precomposed nasal vowel silently-truncated, and one rule set
        (wol-Latn) has no other defect at all, so it drops out of the count
        entirely. All of them are correct IPA the moment they are decomposed.
        """
        for value in ['ã', 'ẽ', 'õ', 'ë']:
            with self.subTest(value=value):
                composed = unicodedata.normalize('NFC', value)
                self.assertEqual(len(composed), 1)  # genuinely precomposed
                self.assertEqual(lint_value(composed), [])
        # …while a real defect in the same shape is still caught.
        self.assertEqual(lint_value('dʒʰ'), ['lossy'])

    def test_a_modifier_on_its_own_is_a_correct_rule(self):
        """PanPhon finds no segment in these, and that is not a defect.

        A length mark, a nasal tilde or a palatalisation modifies the sound
        before it. 22 rows across the shipped rule sets are exactly this —
        Sinhala anusvara, Tatar soft sign, Burmese visarga — and flagging them
        would hand 22 non-questions to people whose time we are asking for.
        """
        from .validation import is_modifier_only
        for value in ['ː', '̃', 'ʲ', 'ʰ', 'ʷ']:
            with self.subTest(value=value):
                self.assertTrue(is_modifier_only(value))
                self.assertEqual(lint_value(value), [])
                self.assertEqual(validate_ipa(value)[1], [])
        # …while a base segment losing its modifier is still a defect, which is
        # the distinction the exemption has to preserve.
        self.assertFalse(is_modifier_only('zʰ'))
        self.assertEqual(lint_value('zʰ'), ['lossy'])
        self.assertEqual(lint_value('dʒʰ'), ['lossy'])

    def test_nfd_equivalent_duplicates_are_caught_where_nfc_would_miss_them(self):
        """The Gurmukhi case that stops a rule set loading.

        U+0A36 and U+0A38 U+0A3C render identically. Unicode's composition
        exclusions mean NFC will not merge them, so an NFC-based check reports a
        clean file; NFD merges them and the duplicate appears.
        """
        precomposed, decomposed = 'ਸ਼', 'ਸ਼'
        self.assertNotEqual(unicodedata.normalize('NFC', decomposed), precomposed)
        self.assertEqual(unicodedata.normalize('NFD', precomposed), decomposed)
        defects = lint_rows([(precomposed, 'ʃ'), (decomposed, 'ʃ'), ('ਕ', 'k')])
        self.assertIn('duplicate_grapheme', defects[0])
        self.assertIn('duplicate_grapheme', defects[1])
        self.assertEqual(defects[2], [])  # and a clean row stays clean


class TranscribeTests(TestCase):

    PAIRS = [('က', 'k'), ('ရ', 'r'), ('ပ', 'p'), ('်', ''), ('ွ', 'w')]

    def test_longest_match_and_the_worked_example_from_the_issue(self):
        result = transcribe('ကရပ်ကွက်', build_map(self.PAIRS))
        self.assertEqual(result['output'], 'krpkwk')
        self.assertTrue(result['complete'])

    def test_unmatched_characters_are_surfaced_not_dropped(self):
        result = transcribe('ကဆ', build_map(self.PAIRS))
        self.assertEqual(result['residue'], ['ဆ'])
        self.assertFalse(result['complete'])
        self.assertIn('(ဆ)', result['output'])

    def test_an_override_changes_the_output(self):
        result = compare('ကရ', self.PAIRS, {'ရ': 'j'})
        self.assertEqual(result['before']['output'], 'kr')
        self.assertEqual(result['after']['output'], 'kj')
        self.assertTrue(result['changed'])


class IsoTests(TestCase):

    def test_browser_languages_map_to_iso_639_3(self):
        parsed = parse_accept_language('my,en-GB;q=0.9,pa-Guru;q=0.8')
        self.assertEqual(parsed[0], ('mya', '', 1.0))
        self.assertIn(('pan', 'Guru', 0.8), parsed)


class SyncBase(TestCase):

    ROWS = [('က', 'k'), ('ရ', 'r'), ('ဂ', 'g')]

    def make_ruleset(self, code='mya-Mymr', rows=None, posture=Posture.SHIPPED,
                     blob='a' * 40, notes=None):
        path = ('developer/epitran-drafts' if posture == Posture.PROPOSED
                else 'zenodo/epitran_extensions')
        ruleset, version, _ = apply_ruleset(
            code, rows if rows is not None else self.ROWS, posture=posture,
            repo='org/indexing', ref='main', path=path,
            blob_sha=blob, commit_sha='c' * 40, notes=notes)
        return ruleset, version


class SyncTests(SyncBase):

    def test_parse_csv_requires_the_epitran_header(self):
        self.assertEqual(parse_csv('Orth,Phon\nက,k\n'), [('က', 'k')])
        with self.assertRaises(Exception):
            parse_csv('grapheme,ipa\nက,k\n')

    def test_rules_are_stored_nfd_and_linted(self):
        ruleset, _ = self.make_ruleset()
        self.assertEqual(ruleset.rules.count(), 3)
        broken = ruleset.rules.get(orth='ဂ')
        self.assertEqual(broken.lint_codes, ['ascii_g'])
        self.assertEqual(ruleset.rules.get(orth='က').lint_codes, [])

    def test_resyncing_the_same_blob_creates_no_new_version(self):
        ruleset, version = self.make_ruleset()
        self.make_ruleset(blob='a' * 40)
        self.assertEqual(ruleset.versions.count(), 1)

    def test_a_row_removed_upstream_is_marked_not_deleted(self):
        ruleset, _ = self.make_ruleset()
        self.make_ruleset(rows=[('က', 'k')], blob='b' * 40)
        self.assertEqual(ruleset.rules.count(), 3)
        self.assertFalse(ruleset.rules.get(orth='ရ').present_upstream)
        self.assertTrue(ruleset.rules.get(orth='က').present_upstream)


class SourceCollisionTests(SyncBase):
    """A code exists in both sources, and the two must not collapse.

    This is the bug the slug exists to prevent: `mya-Mymr` is both a shipped rule
    set and a WHG draft proposing to replace it. Keyed on the code alone, syncing
    the draft would overwrite the live rows in place — flipping their posture and
    silently re-pointing every review made against them onto a value the reviewer
    never saw.
    """

    def test_a_draft_does_not_overwrite_the_shipped_set_of_the_same_code(self):
        shipped, _ = self.make_ruleset('mya-Mymr', posture=Posture.SHIPPED)
        draft, _ = self.make_ruleset('mya-Mymr', rows=[('က', 'k'), ('ရ', 'j')],
                                     posture=Posture.PROPOSED, blob='e' * 40)
        self.assertNotEqual(shipped.pk, draft.pk)
        self.assertEqual(shipped.slug, 'mya-Mymr')
        self.assertEqual(draft.slug, 'mya-Mymr.draft')
        shipped.refresh_from_db()
        self.assertEqual(shipped.posture, Posture.SHIPPED)
        self.assertEqual(shipped.rules.count(), 3)
        self.assertEqual(shipped.rules.get(orth='ရ').current_ipa, 'r')
        self.assertEqual(draft.rules.get(orth='ရ').current_ipa, 'j')

    def test_reviews_stay_with_the_set_they_were_made_against(self):
        shipped, version = self.make_ruleset('mya-Mymr', posture=Posture.SHIPPED)
        rule = shipped.rules.get(orth='ရ')
        ContributionTerms.objects.update(is_active=False)
        terms = ContributionTerms.objects.create(version='t', title='t', body='b',
                                                 is_active=True, signed_off=True)
        user = make_user('c')
        agreement = ReviewerAgreement.objects.create(user=user, terms=terms)
        Review.objects.create(rule=rule, reviewer=user, verdict=Verdict.ACCEPT,
                              reviewed_ipa=rule.current_ipa, reviewed_version=version,
                              agreement=agreement)
        self.make_ruleset('mya-Mymr', rows=[('ရ', 'j')], posture=Posture.PROPOSED,
                          blob='e' * 40)
        rule.refresh_from_db()
        self.assertEqual(rule.current_ipa, 'r')          # untouched by the draft
        self.assertEqual(rule.review_count, 1)
        self.assertEqual(rule.stale_review_count, 0)     # and not falsely superseded

    def test_each_set_can_see_what_the_other_says(self):
        shipped, _ = self.make_ruleset('mya-Mymr', posture=Posture.SHIPPED)
        draft, _ = self.make_ruleset('mya-Mymr', rows=[('ရ', 'j')],
                                     posture=Posture.PROPOSED, blob='e' * 40)
        self.assertEqual(draft.counterpart, shipped)
        self.assertEqual(shipped.counterpart, draft)

    def test_a_new_draft_with_no_shipped_equivalent_has_no_counterpart(self):
        draft, _ = self.make_ruleset('zgh-Tfng', rows=[('ⴰ', 'a')],
                                     posture=Posture.PROPOSED, blob='f' * 40)
        self.assertIsNone(draft.counterpart)

    def test_the_drafts_directory_does_not_withdraw_the_shipped_sets(self):
        """The withdrawal sweep is scoped by posture as well as by source."""
        shipped, _ = self.make_ruleset('sin-Sinh', rows=[('ක', 'k')],
                                       posture=Posture.SHIPPED, blob='g' * 40)
        from .models import RuleSet as RS
        RS.objects.filter(posture=Posture.SHIPPED, source_path__startswith='zenodo/').exclude(
            code__in=['mya-Mymr']).update(present_upstream=False)
        shipped.refresh_from_db()
        self.assertFalse(shipped.present_upstream)  # the sweep does work…
        shipped.present_upstream = True
        shipped.save()
        # …and does not reach across postures.
        self.make_ruleset('mya-Mymr', rows=[('ရ', 'j')], posture=Posture.PROPOSED,
                          blob='h' * 40)
        shipped.refresh_from_db()
        self.assertTrue(shipped.present_upstream)


class NotesAndOrcidTests(SyncBase):

    def test_notes_are_imported_and_the_csv_stays_authoritative(self):
        from .sync import parse_notes
        notes = parse_notes('orth\tproposed_ipa\tnote\nက\tk\tLETTER KA — commonest\n')
        self.assertEqual(notes, {'က': 'LETTER KA — commonest'})
        ruleset, _ = self.make_ruleset('mya-Mymr', rows=[('က', 'k')],
                                       posture=Posture.PROPOSED, blob='n' * 40,
                                       notes=notes)
        rule = ruleset.rules.get(orth='က')
        self.assertEqual(rule.draft_note, 'LETTER KA — commonest')
        # The value comes from the CSV, never from the notes column.
        self.assertEqual(rule.current_ipa, 'k')

    def test_a_malformed_notes_file_is_refused_rather_than_half_read(self):
        from .sync import parse_notes, SyncError
        with self.assertRaises(SyncError):
            parse_notes('grapheme\tipa\twhy\nက\tk\tx\n')

    def test_orcid_is_canonicalised_from_any_accepted_spelling(self):
        from .forms import canonical_orcid
        for given in ['0000-0002-1825-0097',
                      'https://orcid.org/0000-0002-1825-0097',
                      'http://sandbox.orcid.org/0000-0002-1825-0097',
                      '  0000-0002-1825-0097  ']:
            with self.subTest(given=given):
                self.assertEqual(canonical_orcid(given),
                                 'https://orcid.org/0000-0002-1825-0097')
        self.assertEqual(canonical_orcid('0000-0002-1825-009X'),
                         'https://orcid.org/0000-0002-1825-009X')
        for bad in ['', 'not-an-orcid', '0000-0002-1825', 'https://example.org/x']:
            self.assertEqual(canonical_orcid(bad), '')


class RelintTests(SyncBase):
    """Changing the lint rules must be able to reach rows already stored.

    The sync is content-addressed, so a file whose bytes have not moved is never
    re-read and its cached verdicts never change. That is right for the data and
    wrong for the verdicts: when 22 rows stopped being defects, nothing would
    have told the database.
    """

    def test_recompute_moves_a_stale_verdict(self):
        from django.core.management import call_command
        from io import StringIO
        ruleset, _ = self.make_ruleset()
        rule = ruleset.rules.get(orth='က')
        self.assertEqual(rule.lint_codes, [])
        # Simulate a verdict cached under an older lint.
        Rule.objects.filter(pk=rule.pk).update(lint_codes=['unparseable'])
        call_command('lint_epitran_rules', '--recompute', stdout=StringIO())
        rule.refresh_from_db()
        self.assertEqual(rule.lint_codes, [])
        # …and a row that IS defective keeps its verdict, so this is not just
        # a function that clears the column.
        self.assertEqual(ruleset.rules.get(orth='ဂ').lint_codes, ['ascii_g'])


class ReviewTests(SyncBase):

    def setUp(self):
        self.ruleset, self.version = self.make_ruleset()
        self.rule = self.ruleset.rules.get(orth='ရ')
        ContributionTerms.objects.update(is_active=False)
        self.terms = ContributionTerms.objects.create(
            version='t1', title='t', body='b', is_active=True, signed_off=True)
        self.alice = make_user('alice')
        self.bob = make_user('bob')

    def review(self, user, verdict, proposed='', ipa=None):
        agreement, _ = ReviewerAgreement.objects.get_or_create(user=user, terms=self.terms)
        return Review.objects.create(
            rule=self.rule, reviewer=user, verdict=verdict, proposed_ipa=proposed,
            reviewed_ipa=self.rule.current_ipa if ipa is None else ipa,
            reviewed_version=self.version, agreement=agreement)

    def test_an_unreviewed_row_is_not_an_accepted_row(self):
        self.assertEqual(self.rule.status, Rule.UNREVIEWED)
        self.review(self.alice, Verdict.ACCEPT)
        self.rule.refresh_from_db()
        self.assertEqual(self.rule.status, Rule.ACCEPTED)
        # …and the two are distinguishable in the data, not only on screen.
        other = self.ruleset.rules.get(orth='က')
        self.assertEqual(other.review_count, 0)
        self.assertNotEqual(other.status, self.rule.status)

    def test_disagreement_is_retained_rather_than_resolved(self):
        self.review(self.alice, Verdict.ACCEPT)
        self.review(self.bob, Verdict.CORRECT, proposed='j')
        self.rule.refresh_from_db()
        self.assertEqual(self.rule.status, Rule.DISPUTED)
        self.assertEqual(self.rule.review_count, 2)
        self.assertEqual(Review.objects.filter(rule=self.rule, is_latest=True).count(), 2)

    def test_two_different_corrections_are_a_disagreement(self):
        self.review(self.alice, Verdict.CORRECT, proposed='j')
        self.review(self.bob, Verdict.CORRECT, proposed='ɹ')
        self.rule.refresh_from_db()
        self.assertEqual(self.rule.distinct_proposal_count, 2)
        self.assertEqual(self.rule.status, Rule.DISPUTED)

    def test_a_reviewer_changing_their_mind_supersedes_without_deleting(self):
        first = self.review(self.alice, Verdict.ACCEPT)
        self.review(self.alice, Verdict.CORRECT, proposed='j')
        first.refresh_from_db()
        self.rule.refresh_from_db()
        self.assertFalse(first.is_latest)
        self.assertEqual(self.rule.review_count, 1)
        self.assertEqual(Review.objects.filter(rule=self.rule).count(), 2)

    def test_a_review_is_about_the_value_it_was_made_against(self):
        """An upstream edit must not silently re-point an old judgement."""
        review = self.review(self.alice, Verdict.ACCEPT)
        self.make_ruleset(rows=[('က', 'k'), ('ရ', 'j'), ('ဂ', 'g')], blob='b' * 40)
        self.rule.refresh_from_db()
        review.refresh_from_db()
        self.assertEqual(self.rule.current_ipa, 'j')
        self.assertEqual(review.reviewed_ipa, 'r')
        self.assertTrue(review.is_stale)
        self.assertEqual(self.rule.stale_review_count, 1)

    def test_adoption_upstream_is_detected_not_asserted(self):
        review = self.review(self.alice, Verdict.CORRECT, proposed='j')
        self.assertIsNone(review.adopted_upstream_at)
        self.make_ruleset(rows=[('က', 'k'), ('ရ', 'j'), ('ဂ', 'g')], blob='b' * 40)
        review.refresh_from_db()
        self.assertIsNotNone(review.adopted_upstream_at)


class ExportTests(ReviewTests):

    def test_export_round_trips_to_epitran_format(self):
        text, _report = build(self.ruleset)
        self.assertTrue(text.startswith('Orth,Phon\n'))
        self.assertEqual(parse_csv(text), self.ROWS)

    def test_an_unreviewed_row_is_exported_unchanged(self):
        _text, report = build(self.ruleset)
        self.assertEqual(report['unchanged'], 3)
        self.assertEqual(report['corrected'], 0)

    def test_an_agreed_correction_is_applied(self):
        self.review(self.alice, Verdict.CORRECT, proposed='j')
        text, report = build(self.ruleset)
        self.assertEqual(report['corrected'], 1)
        self.assertIn(('ရ', 'j'), parse_csv(text))

    def test_a_disputed_row_is_held_back_with_both_answers_recorded(self):
        self.review(self.alice, Verdict.CORRECT, proposed='j')
        self.review(self.bob, Verdict.CORRECT, proposed='ɹ')
        text, report = build(self.ruleset)
        self.assertEqual(report['disputed'], 1)
        self.assertIn(('ရ', 'r'), parse_csv(text))  # unchanged, not arbitrated
        self.assertEqual(len(report['held_back']), 1)

    def test_a_superseded_review_does_not_drive_the_export(self):
        self.review(self.alice, Verdict.CORRECT, proposed='j', ipa='OLD')
        _ipa, decision, _detail = resolve(self.rule)
        self.assertEqual(decision, 'unchanged')

    def test_the_suggestions_feed_carries_provenance(self):
        self.review(self.alice, Verdict.CORRECT, proposed='j')
        payload = suggestions_payload()
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]['proposed_ipa'], 'j')
        self.assertEqual(payload[0]['reviewed_ipa'], 'r')
        # The plumbing, not the vocabulary choice: the feed reports whatever
        # licence the contributor actually agreed to.
        self.assertEqual(payload[0]['licence'], self.terms.licence_spdx)


class SeededTermsTests(TestCase):

    def test_the_shipped_terms_are_cc_by_4_0_and_anchored_to_the_licence_vocabulary(self):
        terms = ContributionTerms.objects.get(version='2026-09-draft')
        self.assertEqual(terms.licence_spdx, 'CC-BY-4.0')
        self.assertIsNotNone(terms.licence, 'not linked to the licensing app vocabulary')
        self.assertEqual(terms.licence.spdx_id, 'CC-BY-4.0')
        self.assertIn('Creative Commons Attribution 4.0', terms.body)
        # It says how a CC BY row can live inside Epitran's MIT package, because
        # that is the one question the choice actually raises.
        self.assertIn('MIT', terms.body)

    @override_settings(PHONETICS_PUBLIC=True)
    def test_draft_terms_cannot_open_the_app_to_the_public(self):
        from .visibility import is_visible
        from django.contrib.auth.models import AnonymousUser
        terms = ContributionTerms.objects.get(version='2026-09-draft')
        self.assertFalse(terms.signed_off)
        self.assertFalse(is_visible(AnonymousUser()))
        # …and signing them off is the only thing that changes it.
        terms.signed_off = True
        terms.save()
        self.assertTrue(is_visible(AnonymousUser()))


class MyanmarQuestionTests(TestCase):

    def test_q13_is_linked_to_the_register_question(self):
        q13 = PolicyQuestion.objects.get(slug='mya-ha-hto-sonorants')
        q1 = PolicyQuestion.objects.get(slug='mya-register')
        self.assertIn(q1, q13.related.all())
        # Symmetrical, so a reviewer arriving at either one is told about the other.
        self.assertIn(q13, q1.related.all())

    def test_linking_two_questions_links_them_both_ways(self):
        a = PolicyQuestion.objects.create(slug='a', title='A', body='b', options=[])
        b = PolicyQuestion.objects.create(slug='b', title='B', body='b', options=[])
        a.related.add(b)
        self.assertIn(a, b.related.all())


class RoutingTests(SyncBase):

    def setUp(self):
        self.mya, _ = self.make_ruleset('mya-Mymr')
        self.sin, _ = self.make_ruleset('sin-Sinh', rows=[('ක', 'k')], blob='d' * 40)
        self.user = make_user('r')
        ReviewerCompetence.objects.create(user=self.user, language_code='mya',
                                          script_code='Mymr', level='native')

    def test_reviewers_are_offered_only_what_they_declared(self):
        codes = {r.ruleset.code for r in queue(user=self.user)}
        self.assertEqual(codes, {'mya-Mymr'})

    def test_broken_rows_come_first(self):
        first = queue(user=self.user).first()
        self.assertEqual(first.orth, 'ဂ')  # the ASCII-g row
        self.assertEqual(first.lint_codes, ['ascii_g'])

    def test_unmeasured_reach_sorts_after_measured_reach_but_is_not_zero(self):
        rule = self.mya.rules.get(orth='ရ')
        rule.corpus_frequency = 500
        rule.save()
        ordered = list(queue(ruleset=self.mya))
        self.assertEqual(ordered[0].orth, 'ဂ')      # lint still wins
        self.assertEqual(ordered[1].orth, 'ရ')      # then measured reach
        self.assertIsNone(ordered[2].corpus_frequency)

    def test_browser_language_only_suggests(self):
        suggested = suggested_rulesets('si,en;q=0.9')
        self.assertEqual([r.code for r in suggested], ['sin-Sinh'])
        # A suggestion does not create competence, so the queue is unmoved.
        self.assertEqual({r.ruleset.code for r in queue(user=self.user)}, {'mya-Mymr'})

    def test_progress_names_the_unreviewed_state(self):
        progress = ruleset_progress(self.mya)
        self.assertEqual(progress['total'], 3)
        self.assertEqual(progress['unreviewed'], 3)
        self.assertEqual(progress['accepted'], 0)


class PolicyQuestionTests(SyncBase):

    def setUp(self):
        self.ruleset, _ = self.make_ruleset()
        self.question = PolicyQuestion.objects.create(
            slug='q', title='t', body='b', ruleset=self.ruleset,
            options=[{'key': 'a', 'label': 'A'}, {'key': 'b', 'label': 'B'}])
        self.user = make_user('p')

    def test_options_nobody_chose_are_still_shown(self):
        PolicyAnswer.objects.create(question=self.question, user=self.user, option_key='a')
        tally = {t['key']: t['count'] for t in self.question.tally()}
        self.assertEqual(tally, {'a': 1, 'b': 0})

    def test_changing_an_answer_keeps_the_old_one(self):
        PolicyAnswer.objects.create(question=self.question, user=self.user, option_key='a')
        PolicyAnswer.objects.create(question=self.question, user=self.user, option_key='b')
        self.assertEqual(self.question.answers.count(), 2)
        self.assertEqual(self.question.answers.filter(is_latest=True).count(), 1)


class FormAndViewTests(SyncBase):

    def setUp(self):
        self.ruleset, self.version = self.make_ruleset()
        self.rule = self.ruleset.rules.get(orth='ရ')
        ContributionTerms.objects.update(is_active=False)
        self.terms = ContributionTerms.objects.create(
            version='t1', title='t', body='b', is_active=True, signed_off=True)
        self.user = make_user('v', is_staff=True)
        self.client.force_login(self.user)
        ReviewerAgreement.objects.create(user=self.user, terms=self.terms)

    def url(self):
        return reverse('phonetics:rule', args=[self.rule.pk])

    def test_a_lossy_value_cannot_be_submitted(self):
        response = self.client.post(self.url(), {'verdict': 'correct', 'proposed_ipa': 'dʒʰ'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(Review.objects.count(), 0)
        self.assertContains(response, 'keeps only', status_code=200)

    def test_a_good_value_is_stored_with_its_validation_provenance(self):
        response = self.client.post(self.url(), {'verdict': 'correct', 'proposed_ipa': 'j'})
        self.assertEqual(response.status_code, 302)
        review = Review.objects.get()
        self.assertEqual(review.proposed_ipa, 'j')
        self.assertEqual(review.reviewed_ipa, 'r')
        self.assertEqual(review.panphon_version, '0.22.0')
        self.assertTrue(review.ipa_all_sha256)

    def test_proposing_the_current_value_is_refused_as_a_correction(self):
        response = self.client.post(self.url(), {'verdict': 'correct', 'proposed_ipa': 'r'})
        self.assertEqual(Review.objects.count(), 0)
        self.assertContains(response, 'already in the rule set')

    def test_the_validate_endpoint_agrees_with_the_form(self):
        response = self.client.get(reverse('phonetics:api-validate'), {'ipa': 'ⁿɡ'})
        payload = response.json()
        self.assertFalse(payload['ok'])
        self.assertEqual(payload['errors'][0]['code'], 'lossy')
        ok = self.client.get(reverse('phonetics:api-validate'), {'ipa': 'ɡ'}).json()
        self.assertTrue(ok['ok'])

    def test_the_export_view_serves_a_loadable_csv(self):
        response = self.client.get(reverse('phonetics:export-csv', args=[self.ruleset.slug]))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(parse_csv(response.content.decode()), self.ROWS)

    @override_settings(PHONETICS_PUBLIC=True)
    def test_a_non_beta_user_cannot_reach_the_app_while_terms_are_unsigned(self):
        self.terms.signed_off = False
        self.terms.save()
        plain = make_user('plain')
        self.client.force_login(plain)
        self.assertEqual(self.client.get(reverse('phonetics:home')).status_code, 404)
        # …and the same user gets in once the terms are signed off, so this is
        # the terms gate and not a blanket 404.
        self.terms.signed_off = True
        self.terms.save()
        self.assertEqual(self.client.get(reverse('phonetics:home')).status_code, 200)


class NewRuleProposalTests(SyncBase):
    """Adding a letter the rule set does not cover.

    The measurements in place#251 say this is the contribution that matters most:
    every underperforming rule set is missing rows rather than holding wrong ones,
    and drafting the missing Myanmar vowels alone moved it from 16.6% to 98.7%.
    """

    def setUp(self):
        self.ruleset, self.version = self.make_ruleset()
        ContributionTerms.objects.update(is_active=False)
        self.terms = ContributionTerms.objects.create(
            version='t1', title='t', body='b', is_active=True, signed_off=True)
        self.user = make_user('adder', is_staff=True)
        self.agreement = ReviewerAgreement.objects.create(user=self.user, terms=self.terms)
        self.client.force_login(self.user)

    def url(self):
        return reverse('phonetics:propose-rule', args=[self.ruleset.slug])

    def test_a_missing_letter_can_be_proposed(self):
        response = self.client.post(self.url(), {'orth': 'ဣ', 'proposed_ipa': 'ʔi',
                                                 'confidence': 'high'})
        self.assertEqual(response.status_code, 302)
        proposal = NewRuleProposal.objects.get()
        self.assertEqual(proposal.orth, 'ဣ')
        self.assertEqual(proposal.proposed_ipa, 'ʔi')
        self.assertEqual(proposal.panphon_version, '0.22.0')

    def test_a_duplicate_grapheme_cannot_be_created(self):
        response = self.client.post(self.url(), {'orth': 'ရ', 'proposed_ipa': 'j'})
        self.assertEqual(NewRuleProposal.objects.count(), 0)
        self.assertContains(response, 'already has')

    def test_an_nfd_equivalent_duplicate_cannot_be_created_either(self):
        """The Gurmukhi failure, in the one place a user could cause it.

        The two spellings render identically and NFC will not merge them, so
        without an NFD comparison this is how a rule set that will not load
        gets made.
        """
        # Written as escapes deliberately: the two spellings are visually
        # identical, so a literal in the source cannot show which is which — and
        # a copy-paste that collapses them would silently defeat this test.
        precomposed = '\u0A36'                 # GURMUKHI LETTER SHA
        decomposed = '\u0A38\u0A3C'            # SA + NUKTA
        self.assertNotEqual(precomposed, decomposed)
        self.assertEqual(unicodedata.normalize('NFD', precomposed), decomposed)
        # Composition exclusion: NFC does NOT put them back together, which is
        # why an NFC-based duplicate check reports a clean file.
        self.assertNotEqual(unicodedata.normalize('NFC', decomposed), precomposed)
        ruleset, _ = self.make_ruleset('pan-Guru', rows=[(decomposed, 'ʃ')], blob='p' * 40)
        response = self.client.post(
            reverse('phonetics:propose-rule', args=[ruleset.slug]),
            {'orth': precomposed, 'proposed_ipa': 'ʂ'})
        self.assertEqual(NewRuleProposal.objects.count(), 0)
        self.assertContains(response, 'already has')
        # …and a genuinely new letter in the same rule set still goes through.
        self.client.post(reverse('phonetics:propose-rule', args=[ruleset.slug]),
                         {'orth': '\u0A15', 'proposed_ipa': 'k'})  # GURMUKHI KA
        self.assertEqual(NewRuleProposal.objects.count(), 1)

    def test_an_unusable_value_is_refused_here_too(self):
        self.client.post(self.url(), {'orth': 'ဣ', 'proposed_ipa': 'dʒʰ'})
        self.assertEqual(NewRuleProposal.objects.count(), 0)
        # A blank value is legitimate: some letters produce nothing.
        self.client.post(self.url(), {'orth': 'ဣ', 'proposed_ipa': ''})
        self.assertEqual(NewRuleProposal.objects.count(), 1)

    def test_an_agreed_addition_is_appended_to_the_export(self):
        self.client.post(self.url(), {'orth': 'ဣ', 'proposed_ipa': 'ʔi'})
        text, report = build(self.ruleset)
        self.assertEqual(report['added'], 1)
        self.assertIn(('ဣ', 'ʔi'), parse_csv(text))
        self.assertEqual(parse_csv(text)[-1], ('ဣ', 'ʔi'))  # appended, not interleaved

    def test_two_different_proposals_for_one_letter_are_held_back(self):
        self.client.post(self.url(), {'orth': 'ဣ', 'proposed_ipa': 'ʔi'})
        other = make_user('other', is_staff=True)
        ReviewerAgreement.objects.create(user=other, terms=self.terms)
        self.client.force_login(other)
        self.client.post(self.url(), {'orth': 'ဣ', 'proposed_ipa': 'i'})
        text, report = build(self.ruleset)
        self.assertEqual(report['added'], 0)
        self.assertEqual(len(report['held_back']), 1)
        self.assertNotIn('ဣ', [orth for orth, _ in parse_csv(text)])
        self.assertEqual(NewRuleProposal.objects.filter(is_latest=True).count(), 2)

    def test_the_feed_distinguishes_an_addition_from_a_correction(self):
        self.client.post(self.url(), {'orth': 'ဣ', 'proposed_ipa': 'ʔi'})
        payload = suggestions_payload()
        self.assertEqual([p['kind'] for p in payload], ['addition'])
        self.assertIsNone(payload[0]['current_ipa'])

    def test_adoption_is_detected_when_the_letter_appears_upstream(self):
        self.client.post(self.url(), {'orth': 'ဣ', 'proposed_ipa': 'ʔi'})
        proposal = NewRuleProposal.objects.get()
        self.assertIsNone(proposal.adopted_upstream_at)
        self.make_ruleset(rows=self.ROWS + [('ဣ', 'ʔi')], blob='z' * 40)
        proposal.refresh_from_db()
        self.assertIsNotNone(proposal.adopted_upstream_at)

    def test_the_sandbox_offers_the_letters_it_could_not_read(self):
        response = self.client.post(
            reverse('phonetics:sandbox', args=[self.ruleset.slug]),
            {'names': 'ကရဣ'})
        gaps = [g['orth'] for g in response.context['gaps']]
        self.assertEqual(gaps, ['ဣ'])           # the unmapped one, and only it
        self.assertContains(response, 'add a rule')


class CorpusSampleTests(SyncBase):
    """The sandbox seed. It must be a nicety, never a dependency."""

    def test_a_name_is_in_script_when_the_rules_spell_most_of_it(self):
        from .corpus import _in_script
        alphabet = set('ကရပ်ကွ')
        self.assertTrue(_in_script('ကရပ်', alphabet))
        # Same language, romanised — the rules do not cover it, so it is not a
        # useful example for this rule set.
        self.assertFalse(_in_script('Daw Thea', alphabet))
        self.assertFalse(_in_script('', alphabet))
        self.assertFalse(_in_script('123', alphabet))

    def test_iso_639_3_maps_to_the_tag_the_index_uses(self):
        from .corpus import alpha2
        self.assertEqual(alpha2('mya'), 'my')
        self.assertEqual(alpha2('sin'), 'si')

    def test_an_unreachable_index_costs_nothing(self):
        """A sample that cannot be fetched must not take the page down with it."""
        from unittest import mock
        from .corpus import sample_names
        ruleset, _ = self.make_ruleset()
        with mock.patch('phonetics.corpus._query', side_effect=RuntimeError('ES down')):
            self.assertEqual(sample_names(ruleset), [])
        # …and the sandbox still renders.
        response = self.client.get(reverse('phonetics:sandbox', args=[ruleset.slug]))
        self.assertIn(response.status_code, (200, 404))


class PageRenderTests(SyncBase):
    """Every readable page renders, for a visitor with an account and without one.

    It also asserts the shared banner context is present on each. That is not
    decoration: the banner is how an anonymous visitor is told that reading is
    free and contributing needs an account, and a view that forgets to supply it
    still returns 200 — Django resolves the missing variable to nothing and the
    invitation silently disappears. Three views had exactly that bug.
    """

    def setUp(self):
        self.ruleset, _ = self.make_ruleset()
        self.rule = self.ruleset.rules.first()
        ContributionTerms.objects.update(is_active=False)
        ContributionTerms.objects.create(version='t1', title='t', body='b',
                                         is_active=True, signed_off=True)
        PolicyQuestion.objects.create(slug='q1', title='t', body='b',
                                      ruleset=self.ruleset, options=[{'key': 'a', 'label': 'A'}])

    def paths(self):
        return [
            reverse('phonetics:home'),
            reverse('phonetics:queue'),
            reverse('phonetics:ruleset-list'),
            reverse('phonetics:ruleset', args=[self.ruleset.slug]),
            reverse('phonetics:rule', args=[self.rule.pk]),
            reverse('phonetics:sandbox', args=[self.ruleset.slug]),
            reverse('phonetics:propose-rule', args=[self.ruleset.slug]),
            reverse('phonetics:lint'),
            reverse('phonetics:question', args=['q1']),
        ]

    @override_settings(PHONETICS_PUBLIC=True)
    def test_anonymous_visitors_can_read_everything_and_are_told_how_to_contribute(self):
        for path in self.paths():
            with self.subTest(path=path):
                response = self.client.get(path)
                self.assertEqual(response.status_code, 200)
                self.assertTrue(response.context['needs_login'])
                self.assertFalse(response.context['can_contribute'])
                self.assertContains(response, 'Sign in')

    def test_a_signed_in_reviewer_sees_every_page_including_the_ones_needing_an_account(self):
        user = make_user('reader', is_staff=True)
        self.client.force_login(user)
        for path in self.paths() + [reverse('phonetics:terms'),
                                    reverse('phonetics:competence')]:
            with self.subTest(path=path):
                response = self.client.get(path)
                self.assertEqual(response.status_code, 200)
                self.assertIn('needs_login', response.context)
                self.assertFalse(response.context['needs_login'])

    def test_the_gate_is_shut_before_launch(self):
        # Same pages, same anonymous visitor, PHONETICS_PUBLIC left at its default.
        for path in self.paths():
            with self.subTest(path=path):
                self.assertEqual(self.client.get(path).status_code, 404)


class ContributionGateTests(SyncBase):

    def test_no_review_can_be_recorded_without_active_terms(self):
        # The seed migration ships draft terms active, so deactivate them: the
        # state under test is "nothing for a contributor to agree to".
        ContributionTerms.objects.update(is_active=False)
        ruleset, _ = self.make_ruleset()
        rule = ruleset.rules.first()
        user = make_user('n', is_staff=True)
        self.client.force_login(user)
        self.assertIsNone(active_terms())
        response = self.client.post(reverse('phonetics:rule', args=[rule.pk]),
                                    {'verdict': 'accept'})
        self.assertEqual(Review.objects.count(), 0)
        self.assertEqual(response.status_code, 302)

    def test_a_reviewer_must_accept_terms_before_reviewing(self):
        ContributionTerms.objects.update(is_active=False)
        ContributionTerms.objects.create(version='t', title='t', body='b',
                                         is_active=True, signed_off=True)
        ruleset, _ = self.make_ruleset()
        rule = ruleset.rules.first()
        user = make_user('m', is_staff=True)
        self.client.force_login(user)
        response = self.client.post(reverse('phonetics:rule', args=[rule.pk]),
                                    {'verdict': 'accept'})
        self.assertRedirects(response, reverse('phonetics:terms'))
        self.assertEqual(Review.objects.count(), 0)
        # …and it goes through once they have.
        ReviewerAgreement.objects.create(user=user, terms=active_terms())
        self.client.post(reverse('phonetics:rule', args=[rule.pk]), {'verdict': 'accept'})
        self.assertEqual(Review.objects.count(), 1)
