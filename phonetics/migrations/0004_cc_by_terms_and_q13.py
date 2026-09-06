"""Switch the contribution licence to CC BY 4.0, and seed Myanmar Q13.

**CC BY 4.0 rather than MIT (SG's call, 2026-09-06).** These rule sets are
published by WHG as a Zenodo dataset, and the corrections are scholarly
contributions meant to be cited — which is what CC BY is for and what MIT is
not. The one genuine caveat, recorded here so nobody has to rediscover it:
Creative Commons advise against CC BY for *software*, and these CSVs also live
inside Epitran's MIT-licensed Python package. That is workable rather than a
conflict — CC BY 4.0 has no share-alike, so a contribution can be incorporated
into an MIT-licensed work provided the attribution and licence notice travel
with it — but a row contributed upstream to Epitran must carry that notice, and
the terms below say so explicitly rather than leaving it to be discovered at
merge time.

**Q13 is linked to Q1** because they are one decision asked twice. Q1 asks
whether Myanmar targets modern spoken Burmese or Pali/orthographic values; Q13
asks the value for ရှ/ယှ, drafted conservatively as r̥/j̥ where modern Burmese
realises both as ʃ. Answered apart they can be answered inconsistently, which
would be worse than not asking.
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
You license your contributions under the Creative Commons Attribution 4.0
International licence (CC BY 4.0). Anyone may use, adapt and redistribute them,
including commercially, provided you are credited and the licence is named.

These corrections feed rule sets that WHG publishes as a citable dataset, and
which also exist inside the Epitran project's MIT-licensed software. CC BY 4.0
does not prevent that: it carries no share-alike condition, so a corrected row
can be incorporated into MIT-licensed work — the attribution and this licence
notice simply have to travel with it. Where WHG contributes a row upstream, it
will carry that notice.

You keep authorship of what you contribute.

Attribution
-----------
You choose whether to be publicly credited, and under what name. This is citable
scholarly work: contributors are listed by name, with the languages they
reviewed, unless they have asked not to be. Your name and ORCiD are filled in
from your WHG profile as a convenience and can be edited or cleared — a byline
is not the same thing as a login name. You can contribute without attribution,
and that choice is recorded rather than assumed.

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

Q13 = {
    'slug': 'mya-ha-hto-sonorants',
    'language_code': 'mya',
    'title': 'Myanmar: what should ရှ and ယှ produce — r̥/j̥, or ʃ?',
    'body': """Ha-hto (ှ) on a sonorant marks devoicing, not aspiration. The draft therefore
writes ရှ as `r̥` and ယှ as `j̥`, which is the conservative, orthographic reading — and
which the consumer accepts, where an aspirated sonorant would be rejected outright.

Modern spoken Burmese realises both as /ʃ/.

⚠ This is the same decision as the register question (Q1), asked about two specific rows.
Please answer them together: answering one for the spoken register and the other for the
orthographic one would leave the rule set internally inconsistent, which is worse than
either choice made consistently.""",
    'options': [
        {'key': 'devoiced', 'label': 'Keep the drafted r̥ / j̥',
         'detail': 'The orthographic reading: ha-hto marks devoicing, and that is what the '
                   'value should record.'},
        {'key': 'sh', 'label': 'Use ʃ for both',
         'detail': 'The modern spoken realisation. Consistent with answering Q1 "modern '
                   'spoken Burmese".'},
        {'key': 'split', 'label': 'Different answers for ရှ and ယှ',
         'detail': 'Say which in a comment.'},
        {'key': 'unsure', 'label': 'I read Burmese but I do not want to decide this',
         'detail': 'Recorded as an answer, not as a skip.'},
    ],
}


def forwards(apps, schema_editor):
    ContributionTerms = apps.get_model('phonetics', 'ContributionTerms')
    PolicyQuestion = apps.get_model('phonetics', 'PolicyQuestion')
    License = apps.get_model('licensing', 'License')

    cc_by = License.objects.filter(spdx_id='CC-BY-4.0').first()
    ContributionTerms.objects.filter(version='2026-09-draft').update(
        licence_spdx='CC-BY-4.0', licence=cc_by, body=TERMS_BODY)

    q13, _ = PolicyQuestion.objects.get_or_create(
        slug=Q13['slug'], defaults={k: v for k, v in Q13.items() if k != 'slug'})
    q1 = PolicyQuestion.objects.filter(slug='mya-register').first()
    if q1 is not None:
        # Both directions explicitly. The field is symmetrical on the live model,
        # but a historical model in a migration does not reliably carry that, and
        # a half-linked pair would warn a reviewer arriving at one question and
        # not the other — the exact failure the link exists to prevent.
        q13.related.add(q1)
        q1.related.add(q13)


def backwards(apps, schema_editor):
    apps.get_model('phonetics', 'PolicyQuestion').objects.filter(slug=Q13['slug']).delete()
    apps.get_model('phonetics', 'ContributionTerms').objects.filter(
        version='2026-09-draft').update(licence_spdx='MIT', licence=None)


class Migration(migrations.Migration):
    dependencies = [
        ('phonetics', '0003_ruleset_slug_draft_notes_related_questions'),
        ('licensing', '0001_initial'),
    ]
    operations = [migrations.RunPython(forwards, backwards)]
