"""Give a rule set an identity that survives the same code existing twice.

``mya-Mymr``, ``sin-Sinh`` and ``pan-Guru`` exist both as shipped rule sets and
as WHG drafts proposing to replace them. Keyed on ``code`` alone — as it was —
the drafts sync would have overwritten the live rows in place, flipped their
posture from "in production" to "proposed", and silently re-pointed every review
made against them onto values nobody had seen. The identity is therefore the
slug (``mya-Mymr`` / ``mya-Mymr.draft``).

The backfill is safe because every row existing at this point came from the
shipped source, whose slug is its code unchanged.

Also here: ``Rule.draft_note`` for the reasoning in the ``.NOTES.tsv``
companions, ``PolicyQuestion.related`` for questions that must be answered
together, and the licence fields the next migration fills in.
"""

import django.db.models.deletion
from django.db import migrations, models


def populate_slugs(apps, schema_editor):
    RuleSet = apps.get_model('phonetics', 'RuleSet')
    for ruleset in RuleSet.objects.all():
        ruleset.slug = (ruleset.code if ruleset.posture == 'shipped'
                        else f'{ruleset.code}.draft')
        ruleset.save(update_fields=['slug'])


def clear_slugs(apps, schema_editor):
    apps.get_model('phonetics', 'RuleSet').objects.update(slug='')


class Migration(migrations.Migration):

    dependencies = [
        ('licensing', '0001_initial'),
        ('phonetics', '0002_seed_terms_and_myanmar_question'),
    ]

    operations = [
        # 1. Drop the uniqueness that was the bug, and add the slug as nullable
        #    so existing rows survive long enough to be given one.
        migrations.AlterField(
            model_name='ruleset',
            name='code',
            field=models.CharField(
                db_index=True, max_length=32,
                help_text="Epitran mode code, e.g. 'mya-Mymr'. NOT unique."),
        ),
        migrations.AddField(
            model_name='ruleset',
            name='slug',
            field=models.CharField(
                default='', max_length=48,
                help_text="URL key: the code, plus '.draft' for a proposed set."),
            preserve_default=False,
        ),
        migrations.RunPython(populate_slugs, clear_slugs),
        migrations.AlterField(
            model_name='ruleset',
            name='slug',
            field=models.CharField(
                max_length=48, unique=True,
                help_text="URL key: the code, plus '.draft' for a proposed set."),
        ),
        migrations.AlterUniqueTogether(
            name='ruleset',
            unique_together={('code', 'posture')},
        ),

        # 2. The drafter's reasoning, imported from the .NOTES.tsv companions.
        migrations.AddField(
            model_name='rule',
            name='draft_note',
            field=models.TextField(blank=True, default=''),
            preserve_default=False,
        ),

        # 3. Questions that turn on one decision and must be answered together.
        migrations.AddField(
            model_name='policyquestion',
            name='related',
            field=models.ManyToManyField(blank=True, to='phonetics.policyquestion',
                                         symmetrical=True),
        ),

        # 4. Licence, anchored to WHG's own vocabulary rather than a loose string.
        migrations.AlterField(
            model_name='contributionterms',
            name='licence_spdx',
            field=models.CharField(
                default='CC-BY-4.0', max_length=64,
                help_text="SPDX id, matching the licensing app's vocabulary. Must permit "
                          "inclusion in the rule sets these corrections feed, which are MIT."),
        ),
        migrations.AddField(
            model_name='contributionterms',
            name='licence',
            field=models.ForeignKey(
                blank=True, null=True, on_delete=django.db.models.deletion.PROTECT,
                related_name='phonetic_contribution_terms', to='licensing.license',
                help_text="The row in WHG's licence vocabulary, so this page and /licenses/ "
                          "cannot drift apart."),
        ),

        # 5. ORCiD is stored as the canonical https://orcid.org/… URL.
        migrations.AlterField(
            model_name='revieweragreement',
            name='orcid',
            field=models.URLField(blank=True, max_length=64),
        ),
    ]
