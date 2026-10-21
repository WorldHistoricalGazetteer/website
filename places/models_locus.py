"""The locus registry (place#172, place#325 Stage 4): an append-only binding of
locus PIDs to the canonical bytes they name.

A row is a permanent public obligation (place#172, "What we promise"):

1. a PID is never reused for different bytes and never leaves the registry;
2. it always resolves, to results or to a documented reason for none;
3. at a stable URI, ``/entity/locus:<PID>``;
4. its recipe is public.

Two kinds of row:

* ``recipe``: ``canonical`` is exactly ``places.locus_recipe.canonical_bytes``
  and ``pid`` is ``recipe_pid`` of those bytes (54 hash bits + check digit).
  The hash is one-way, so the registry is what makes a PID resolvable at all,
  and it is the COLLISION AUTHORITY: a PID already bound to different bytes is
  refused, never overwritten (``places.locus_registry.mint_recipe``).
* ``legacy``: a ``whg_id`` (``1[0-9]{7}``) admitted as a frozen locus from
  ``LegacyUnionRecord`` on first dereference. ``canonical`` is the canonical
  JSON of that snapshot row (``{"kind":"legacy",...}``, never parseable as a
  recipe); ``pid`` is the whg_id as text. It carries no recipe and resolves to
  its frozen membership, marked as such.

Append-only, enforced twice
---------------------------
The BINDING columns (``pid``, ``kind``, ``version``, ``canonical``, ``digest``,
``token``, ``members``, ``minted_at``, ``minted_via``; ``minted_by`` may only
become NULL when the account is deleted) can never change, and a row can never
be deleted:

* in Python, :meth:`LocusRegistryEntry.save` refuses to alter them and
  :meth:`LocusRegistryEntry.delete` always raises;
* in Postgres, the trigger ``locus_registry_append_only`` (migration 0027)
  raises on DELETE and on any UPDATE that touches them, so ``QuerySet.update``,
  ``QuerySet.delete``, raw SQL and other clients are refused too.

What CAN change, and only in one direction each (migration 0028, rulings of
2026-10-21 after the dev walkthrough):

* ``status`` goes ``active`` -> ``withdrawn`` with a reason, by whom, and when
  (place#308 tombstone). **Withdrawal is one-way**: a withdrawn row can never
  return to ``active``, and its ``withdrawn_at`` / ``withdrawn_reason`` /
  ``withdrawn_by`` are frozen once written (``withdrawn_by`` may still become
  NULL when that account is deleted).
* ``redacted_at`` / ``redacted_by`` / ``redaction_reason``: a staff take-down,
  of an active or a withdrawn row. Write-once from NULL; never undone.
* ``minter_public`` (the minter chose, in the mint modal, to be named): may go
  ``true`` -> ``false`` (the minter withdraws consent, or staff act on their
  behalf), never ``false`` -> ``true`` after the row exists. Turning it on
  later would publish a person's name without the consent recorded at mint
  time, and the database cannot tell who is asking; retraction is always safe.

A withdrawn OR redacted row is *suppressed* (:attr:`is_suppressed`): its page
and JSON show the PID, the status, the withdrawal reason and the dates, and
nothing of the recipe or the minter. The CANONICAL BYTES STAY in this table
regardless, because the PID is a hash of them and the registry is the
collision authority (a PID must never be re-bound to different bytes); they
are simply never displayed once suppressed (not on the page, not in the JSON,
not in the admin). There is no correction, merge, split or version history
(place#172: a defective recipe is not ours to fix; mint a better one).
"""
from __future__ import annotations

import hashlib

from django.conf import settings
from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.indexes import GinIndex
from django.db import models
from django.utils import timezone


class RegistryImmutable(Exception):
    """Raised by the Python guards when code tries to alter or delete a binding."""


class LocusRegistryEntry(models.Model):
    KIND_RECIPE = "recipe"
    KIND_LEGACY = "legacy"
    KIND_CHOICES = ((KIND_RECIPE, "recipe (live: re-run on every visit)"),
                    (KIND_LEGACY, "legacy whg_id (frozen membership)"))

    STATUS_ACTIVE = "active"
    STATUS_WITHDRAWN = "withdrawn"
    STATUS_CHOICES = ((STATUS_ACTIVE, "active"), (STATUS_WITHDRAWN, "withdrawn (tombstone)"))

    VIA_MODAL = "modal"              # a signed-in person confirmed the mint on the locus page
    VIA_DEREFERENCE = "dereference"  # a legacy whg_id minted on its first dereference
    VIA_CHOICES = ((VIA_MODAL, "minting modal"), (VIA_DEREFERENCE, "first dereference"))

    # Columns that make up the binding. Mirrored in the trigger (migration 0027,
    # revised in 0028, which adds the one-way rules for the serving columns).
    BINDING_FIELDS = ("pid", "kind", "version", "canonical", "digest", "token",
                      "members", "minted_at", "minted_via")

    # "XXXX-XXXX-XXXX" (14 chars) or a legacy whg_id "1NNNNNNN" (8 chars).
    pid = models.CharField(max_length=14, primary_key=True)
    kind = models.CharField(max_length=8, choices=KIND_CHOICES)
    # The recipe's "v" (places.locus_recipe.RECIPE_VERSION); 0 for a legacy row.
    version = models.PositiveSmallIntegerField()
    # The bytes the PID names. For a recipe row: exactly canonical_bytes(recipe).
    canonical = models.BinaryField()
    # sha256(canonical) as hex. Unique on its own: two rows can never name the same bytes.
    digest = models.CharField(max_length=64, unique=True)
    # base64url(canonical) for a recipe row (the unminted-URL token); "" for legacy.
    token = models.TextField(blank=True, default="")
    # Derived from `canonical` at mint time, denormalised so overlap lookups
    # ("existing loci sharing these members", place#172) are one indexed query.
    # Recipe rows: the recipe's member ids. Legacy rows: "whg:<places.id>" per member.
    members = ArrayField(models.CharField(max_length=255), default=list)

    minted_by = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True,
                                  on_delete=models.SET_NULL, related_name="minted_loci")
    minted_at = models.DateTimeField(default=timezone.now)
    minted_via = models.CharField(max_length=16, choices=VIA_CHOICES)

    status = models.CharField(max_length=12, choices=STATUS_CHOICES, default=STATUS_ACTIVE)
    withdrawn_at = models.DateTimeField(null=True, blank=True)
    withdrawn_by = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True,
                                     on_delete=models.SET_NULL, related_name="withdrawn_loci")
    withdrawn_reason = models.TextField(blank=True, default="")

    # Whether the minter chose to be named publicly (the mint modal's checkbox,
    # default off). Off: the page and JSON say "a WHG contributor"; minted_by is
    # still recorded, for staff. May only go true -> false (see module docstring).
    minter_public = models.BooleanField(default=False)

    # Staff redaction (take-down): write-once. The reason is for staff only and
    # is never shown publicly. Redaction does NOT remove `canonical` (the PID is
    # its hash); it stops it being displayed.
    redacted_at = models.DateTimeField(null=True, blank=True)
    redacted_by = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True,
                                    on_delete=models.SET_NULL, related_name="redacted_loci")
    redaction_reason = models.TextField(blank=True, default="")

    class Meta:
        db_table = "locus_registry"
        verbose_name = "locus registry entry"
        verbose_name_plural = "locus registry entries"
        indexes = [
            GinIndex(fields=["members"], name="locus_registry_members_gin"),
            models.Index(fields=["minted_by", "minted_at"], name="locus_registry_minter_idx"),
        ]

    def __str__(self):
        return f"locus:{self.pid} ({self.kind}, {self.status})"

    # ------------------------------------------------------------------ guards

    @staticmethod
    def digest_of(canonical: bytes) -> str:
        return hashlib.sha256(bytes(canonical)).hexdigest()

    @property
    def canonical_bytes(self) -> bytes:
        return bytes(self.canonical)

    @property
    def is_withdrawn(self) -> bool:
        return self.status == self.STATUS_WITHDRAWN

    @property
    def is_redacted(self) -> bool:
        return self.redacted_at is not None

    @property
    def is_suppressed(self) -> bool:
        """Withdrawn or redacted: served as a tombstone, recipe text never shown."""
        return self.is_withdrawn or self.is_redacted

    @property
    def serving_status(self):
        """``X-WHG-Locus-Status`` for a suppressed row: "withdrawn" (wins) or
        "redacted"; None for an active, unredacted row."""
        if self.is_withdrawn:
            return "withdrawn"
        if self.is_redacted:
            return "redacted"
        return None

    @property
    def entity_path(self) -> str:
        return f"/entity/locus:{self.pid}/"

    def save(self, *args, **kwargs):
        """Insert freely; on an existing row refuse any change to the binding."""
        if not self._state.adding:
            try:
                current = type(self).objects.get(pk=self.pk)
            except type(self).DoesNotExist:
                current = None
            if current is not None:
                for f in self.BINDING_FIELDS:
                    if f == "canonical":
                        same = bytes(current.canonical) == bytes(self.canonical)
                    elif f == "members":
                        same = list(current.members) == list(self.members)
                    else:
                        same = getattr(current, f) == getattr(self, f)
                    if not same:
                        raise RegistryImmutable(
                            f"locus registry is append-only: {f} of locus:{self.pid} cannot change (place#172)")
                if current.minted_by_id is not None and self.minted_by_id not in (None, current.minted_by_id):
                    raise RegistryImmutable(
                        f"locus registry is append-only: minted_by of locus:{self.pid} cannot change (place#172)")
                self._check_one_way(current)
        elif self.digest != self.digest_of(self.canonical):
            raise RegistryImmutable("digest must be sha256 of canonical")
        super().save(*args, **kwargs)

    def _check_one_way(self, current):
        """The Python mirror of migration 0028's one-way rules."""
        def refuse(what):
            raise RegistryImmutable(f"locus registry: {what} of locus:{self.pid} cannot change (place#325)")
        if current.status == self.STATUS_WITHDRAWN:
            if self.status != self.STATUS_WITHDRAWN:
                refuse("a withdrawal is one-way; the status")
            if self.withdrawn_at != current.withdrawn_at or self.withdrawn_reason != current.withdrawn_reason:
                refuse("the withdrawal record")
            if self.withdrawn_by_id not in (None, current.withdrawn_by_id):
                refuse("withdrawn_by")
        if current.redacted_at is not None:
            if self.redacted_at != current.redacted_at or self.redaction_reason != current.redaction_reason:
                refuse("a redaction is write-once; the redaction record")
            if self.redacted_by_id not in (None, current.redacted_by_id):
                refuse("redacted_by")
        if self.minter_public and not current.minter_public:
            refuse("minter_public may only be turned off after minting;")

    def delete(self, *args, **kwargs):
        raise RegistryImmutable(
            f"locus registry is append-only: locus:{self.pid} cannot be deleted (place#172)")

    def withdraw(self, reason: str, by=None):
        """Tombstone this locus (place#308): it keeps its PID and a page that
        says it was withdrawn, why and when; the recipe stops being re-run and
        its text stops being shown (rulings 2026-10-21). One-way: a withdrawn
        locus is never reinstated (a second call is a no-op)."""
        reason = (reason or "").strip()
        if not reason:
            raise ValueError("a withdrawal needs a stated reason")
        if self.is_withdrawn:
            return self
        self.status = self.STATUS_WITHDRAWN
        self.withdrawn_at = timezone.now()
        self.withdrawn_by = by if (by is not None and getattr(by, "pk", None)) else None
        self.withdrawn_reason = reason
        self.save(update_fields=["status", "withdrawn_at", "withdrawn_by", "withdrawn_reason"])
        return self

    def redact(self, reason: str, by=None):
        """Staff take-down (rulings 2026-10-21): the recipe text and the minter
        stop being shown anywhere; the row, its PID and its canonical bytes stay
        (the PID is their hash). Write-once: a second call keeps the first.
        ``reason`` is recorded for staff and never displayed publicly."""
        reason = (reason or "").strip()
        if not reason:
            raise ValueError("a redaction needs a stated reason")
        if self.is_redacted:
            return self
        self.redacted_at = timezone.now()
        self.redacted_by = by if (by is not None and getattr(by, "pk", None)) else None
        self.redaction_reason = reason
        self.save(update_fields=["redacted_at", "redacted_by", "redaction_reason"])
        return self

    def hide_minter(self):
        """The minter's name stops being shown (true -> false only)."""
        if self.minter_public:
            self.minter_public = False
            self.save(update_fields=["minter_public"])
        return self
