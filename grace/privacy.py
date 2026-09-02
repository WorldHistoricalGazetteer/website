"""Data-protection machinery for GRACE's Catalogue.

DESIGN DECISION 6 (see ``developer/whg-tracker-review.html`` §10). GRACE holds
names, email addresses and affiliations for researchers who never asked to be
in a database. The position, settled 27 August 2026:

**Lawful basis: legitimate interests** (UK/EU GDPR Article 6(1)(f)), not
consent. Consent is circular here — it must be informed and freely given
*before* processing, which is impossible for someone we have not yet contacted
— and it is withdrawable, so relying on it would oblige us to erase the record
of a negotiation that actually happened, losing the provenance for rights we
legitimately hold. The interest is scholarly and curatorial: identifying
gazetteers and negotiating rights over them. The data is professional (name,
institutional affiliation, ORCID, work email), which is what makes the
balancing test pass. It would **not** pass for home addresses, personal email
or special-category data, so do not add fields for those.

Four obligations follow, and all four are implemented rather than left as
policy:

1. **Article 14 transparency.** We collect from third parties, not from the
   person, so a privacy notice is owed within a month or at first contact,
   whichever is sooner. ``Person.privacy_notice_sent_at`` records it and
   ``Person.objects.owed_privacy_notice()`` finds who is still waiting. Our
   own people are out of scope — Article 14 is about data obtained from
   someone other than the person, and staff and collaborators are told under
   Article 13 when they join. ``PersonRole.is_internal`` is what marks them,
   so who counts as internal stays editable rather than hard-coded.
2. **Consent stays separate for the mailing list.** A newsletter is direct
   marketing and needs consent under PECR/ePrivacy whatever our Article 6 basis
   for the record itself. Legitimate interests covers the Catalogue entry;
   consent covers the mail. One flag must never do both jobs.
3. **Erasure by pseudonymisation, not cascade delete.** See
   ``Person.pseudonymise()``. The editorial history survives; the person does
   not.
4. **Retention: three years without interaction triggers a review.** See
   ``Person.objects.needing_retention_review()`` and the
   ``grace_retention_review`` management command.

Encryption matches the standard the user model already sets: the address is
held in an ``EncryptedTextField`` with an indexed HMAC beside it for lookups,
because the encrypted column itself is unqueryable.
"""
from datetime import timedelta

from django.utils import timezone

#: Years without a recorded interaction after which a contact is surfaced for
#: review. Review, not automatic deletion — a long-dormant rights holder may
#: still be the only person who can answer a licensing question, so a human
#: decides. Obligation 4 of decision 6.
RETENTION_REVIEW_YEARS = 3

#: How long after a contact is created the Article 14 notice becomes overdue.
#: The Regulation says "within a reasonable period, at the latest within one
#: month" — or at first contact, if that comes sooner, which the engagement
#: workflow handles separately.
PRIVACY_NOTICE_DUE_DAYS = 30

#: The lawful basis recorded against every Catalogue contact. Held as a
#: constant so it appears in the admin and in any export, rather than living
#: only in a document nobody opens.
LAWFUL_BASIS = "GDPR Art. 6(1)(f) — legitimate interests"

#: One-line statement of the interest, for the Article 14 notice and the LIA.
LEGITIMATE_INTEREST = (
    "Identifying historical gazetteers and the people and institutions that "
    "hold rights in them, in order to negotiate their inclusion in the World "
    "Historical Gazetteer."
)


def retention_cutoff(years=RETENTION_REVIEW_YEARS):
    """The datetime before which a last interaction counts as stale."""
    return timezone.now() - timedelta(days=365 * years)


def privacy_notice_cutoff(days=PRIVACY_NOTICE_DUE_DAYS):
    """Contacts created before this are overdue an Article 14 notice."""
    return timezone.now() - timedelta(days=days)
