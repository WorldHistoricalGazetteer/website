"""Rename the pipeline record to Dataset and Contact to Person.

Palak's point 7, and she is right: a contributor brings a *dataset*; once
reconciled and published it is a *gazetteer*. Same object, two life stages —
and GRACE's record exists almost entirely during the first, because the moment
it becomes a gazetteer the authoritative record is the Register row it reads
through. So the pipeline record is a Dataset, and "gazetteer" is left to mean
what it means everywhere else at WHG: a printed reference work (a ``Source``)
or a published WHG gazetteer (a Register entry).

``Contact`` becomes ``Person`` because the register is meant to hold everyone
we track, ourselves included — "contact" implied only the people we write to.

Hand-written rather than generated: ``makemigrations`` sees a rename as a drop
plus an add unless a human confirms it interactively, and that would take the
data with it.
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [("grace", "0002_alter_actionitem_options_and_more")]

    operations = [
        # --- models -----------------------------------------------------
        migrations.RenameModel("Contact", "Person"),
        migrations.RenameModel("ContactRole", "PersonRole"),
        migrations.RenameModel("ContactStatus", "PersonStatus"),
        migrations.RenameModel("TrackedGazetteer", "TrackedDataset"),

        # --- fields -----------------------------------------------------
        migrations.RenameField("engagement", "contact", "person"),
        migrations.RenameField("engagement", "tracked_gazetteer", "dataset"),
        migrations.RenameField("interaction", "contact", "person"),
        migrations.RenameField("project", "contacts", "people"),
        migrations.RenameField("trackeddataset", "contacts", "people"),
        migrations.RenameField("source", "derived_gazetteers", "derived_datasets"),
        migrations.RenameField("content", "gazetteers", "datasets"),
        migrations.RenameField("sourcesuggestion", "promoted_to_gazetteer",
                               "promoted_to_dataset"),

        # --- the People register is everyone, so mark our own side ------
        migrations.AddField(
            model_name="personrole",
            name="is_internal",
            field=models.BooleanField(
                default=False,
                help_text="Tick for roles on WHG's own side — staff, "
                          "collaborators, technical experts. People in an "
                          "internal role are exempt from the Article 14 "
                          "notice queue.",
                verbose_name="one of us",
            ),
        ),
    ]
