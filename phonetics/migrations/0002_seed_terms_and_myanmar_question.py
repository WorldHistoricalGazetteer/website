"""Seed the draft contribution terms and the one policy question we already have.

The terms are seeded **active but not signed off**. That combination is
deliberate: it lets the tool be exercised by staff and beta testers while making
it impossible to open the app to the public, because
``phonetics.views._gate`` requires ``signed_off`` before ``PHONETICS_PUBLIC``
means anything. Non-negotiable 6 of place#252 is that licensing is settled
before launch rather than after, and a flag someone has to remember is not a
settlement.

⚠ **The wording below is a placeholder awaiting sign-off.** It is MIT because
Epitran's own rule sets are MIT and a contribution licence that is not
compatible with the thing it will be merged into is worthless. Replace the body
and set ``signed_off=True`` when it has actually been approved.
"""

from django.db import migrations

TERMS_BODY = """DRAFT — PENDING SIGN-OFF. Please do not treat this wording as final.

What you are contributing
-------------------------
Corrections, comments and answers you record here are proposals about how a
letter or letter-sequence should be transcribed into the International Phonetic
Alphabet. They are recorded with your name against them (unless you ask
otherwise below), together with the date, your stated competence in the
language, and the exact value you were looking at when you made them.

The licence
-----------
You license your contributions under the MIT Licence. The rule sets these
corrections feed into come from the Epitran project and are themselves MIT, so a
contribution offered under anything incompatible could not be merged into them —
which would waste your work rather than protect it.

You keep authorship of what you contribute. The MIT licence lets WHG and anyone
else use, publish and redistribute it, including commercially, provided the
attribution notice is kept.

Attribution
-----------
You choose whether to be publicly credited, and under what name. This is citable
scholarly work: contributors are listed by name, with the languages they
reviewed, unless they have asked not to be. You can contribute without
attribution, and that choice is recorded rather than assumed.

What happens to your corrections
--------------------------------
Nothing here installs anything. Your correction is a proposal. Someone reviews
proposals and decides, deliberately and separately, whether to change the rule
sets that WHG actually runs. Where reviewers disagree, all the answers are kept
and none is deleted or overridden.

What is recorded about you
--------------------------
Your account, the language competences you declare, the reviews you record, and
the date of each. Competence is self-declared and recorded as self-declared; we
do not verify it and do not treat it as authority.
"""

MYANMAR_QUESTION = {
    'slug': 'mya-register',
    'language_code': 'mya',
    'title': 'Myanmar: should these rules target modern spoken Burmese, or Pali/orthographic values?',
    'body': """The shipped Myanmar rule set consistently gives orthographic or Pali values rather
than modern spoken ones: သ as `s` rather than `θ`, ရ as `r` rather than `j`, and a
voiced-aspirate series that modern spoken Burmese does not have.

That is a decision, not a mistake, and nothing has been changed pending an answer. It
cannot be asked row by row, because answering it one way rather than the other changes
dozens of rows at once.

What turns on it: these rules produce the IPA that the cross-script matching model is
trained against. If the target is retrieval of names as people say them, the spoken
values are the right ones. If the target is a transliteration faithful to the writing
system, the orthographic values are.

⚠ One caveat worth knowing before you answer: the consumer silently discards some
aspiration contrasts (it parses `ɡʰ` and keeps only `ɡ`). Where that happens, choosing
between an aspirated and an unaspirated value buys nothing downstream — so the question
matters most for the cases where the two registers differ in more than aspiration.""",
    'options': [
        {'key': 'spoken', 'label': 'Modern spoken Burmese',
         'detail': 'သ → θ, ရ → j. Target the language as spoken now.'},
        {'key': 'orthographic', 'label': 'Pali / orthographic values',
         'detail': 'သ → s, ရ → r. Keep the current approach, faithful to the writing system.'},
        {'key': 'mixed', 'label': 'A mixture, and I will say which rows in a comment',
         'detail': 'Some rows should follow one register and some the other.'},
        {'key': 'unsure', 'label': 'I read Burmese but I do not want to decide this',
         'detail': 'Recorded as an answer, not as a skip.'},
    ],
}


def seed(apps, schema_editor):
    ContributionTerms = apps.get_model('phonetics', 'ContributionTerms')
    PolicyQuestion = apps.get_model('phonetics', 'PolicyQuestion')

    ContributionTerms.objects.get_or_create(
        version='2026-09-draft',
        defaults={
            'title': 'Contributing corrections to WHG phonetic rule sets',
            'body': TERMS_BODY,
            'licence_spdx': 'MIT',
            'is_active': True,
            'signed_off': False,
            'signed_off_note': 'Draft wording. The review UI cannot be opened to the public '
                               'until this is approved and signed_off is set.',
        })

    # ruleset is left null: the sync has not necessarily run yet, and a
    # language-level question attaches to every rule set in that language anyway.
    PolicyQuestion.objects.get_or_create(
        slug=MYANMAR_QUESTION['slug'],
        defaults={k: v for k, v in MYANMAR_QUESTION.items() if k != 'slug'})


def unseed(apps, schema_editor):
    apps.get_model('phonetics', 'ContributionTerms').objects.filter(
        version='2026-09-draft').delete()
    apps.get_model('phonetics', 'PolicyQuestion').objects.filter(
        slug=MYANMAR_QUESTION['slug']).delete()


class Migration(migrations.Migration):
    dependencies = [('phonetics', '0001_initial')]
    operations = [migrations.RunPython(seed, unseed)]
