"""Minting and lookup for the locus registry (place#172, place#325 Stage 4).

Everything here is OFF unless the v4 release switch is on
(``places.v4_landing.v4_landing_enabled``: ``V4_LANDING_ENABLED``, or its
deprecated alias ``LOCUS_MINTING_ENABLED``; both absent on prod): v4 changes
nothing public until release.
Callers check :func:`minting_enabled` before doing anything that could write.

Two ways a row comes into being, and only two:

* :func:`mint_recipe`: a signed-in person confirmed the mint of a RECIPE on the
  locus page. Idempotent (the same recipe mints once and returns the same
  entry), collision-safe (a PID already bound to different bytes is refused;
  the registry is the collision authority for the 54-bit hash) and
  concurrency-safe (two simultaneous mints of one recipe race on the primary
  key; the loser re-reads and compares bytes).
* :func:`mint_legacy`: a legacy ``whg_id`` is minted as a frozen locus from
  ``LegacyUnionRecord`` on its first dereference (place#172: "everything else
  waits for a dereference from the snapshot"). The snapshot is immutable, so
  this is idempotent by construction.

Nothing here updates or deletes a binding; see ``places.models_locus``.
"""
from __future__ import annotations

import json
from datetime import timedelta
from typing import Optional

from django.db import IntegrityError, connection, transaction
from django.utils import timezone

from places.locus_recipe import (RECIPE_VERSION, RecipeError, canonical_bytes, encode_recipe_token,
                                 is_legacy_locus_id, normalise_pid, parse_recipe, pid_from_digest)
from places.models_locus import LocusRegistryEntry

# Per-user ceilings on NEW mints (idempotent re-mints of an already-minted
# recipe are free). Each mint is a permanent obligation, so the limit is
# deliberately low; a person who needs more in an hour is doing something a
# registry we have undertaken never to prune should not absorb unexamined.
MINT_LIMIT_PER_HOUR = 20
MINT_LIMIT_PER_DAY = 100
# First key of the two-key pg_advisory_xact_lock(<this>, <user pk>) that serialises one
# user's mints (count, then insert), so it cannot collide with another lock's keys.
MINT_LOCK_NAMESPACE = 325172


class LocusCollision(Exception):
    """The PID is already bound to DIFFERENT bytes. Never resolved by overwriting."""

    def __init__(self, pid: str):
        self.pid = pid
        super().__init__(
            f"locus:{pid} is already bound to a different recipe (54-bit hash collision); "
            f"this recipe cannot be minted under it. Please report it (place#172).")


class MintRateLimited(Exception):
    def __init__(self, window: str, limit: int, retry_after: int):
        self.window = window
        self.limit = limit
        self.retry_after = retry_after
        super().__init__(f"minting limit reached: {limit} new loci per {window}")


class LookupError_(Exception):
    """A PID that cannot name a locus: ``status`` is the HTTP status to answer with."""

    def __init__(self, status: int, message: str):
        self.status = status
        self.message = message
        super().__init__(message)


def minting_enabled() -> bool:
    """The v4 release switch (ruling 2026-10-21: one switch, not two);
    ``LOCUS_MINTING_ENABLED`` is honoured as its deprecated alias there."""
    from places.v4_landing import v4_landing_enabled
    return v4_landing_enabled()


# ---------------------------------------------------------------- rate limit

def mint_allowance(user) -> dict:
    """``{"hour": remaining, "day": remaining}`` new mints this user may still make."""
    now = timezone.now()
    qs = LocusRegistryEntry.objects.filter(minted_by=user, minted_via=LocusRegistryEntry.VIA_MODAL)
    hour = qs.filter(minted_at__gte=now - timedelta(hours=1)).count()
    day = qs.filter(minted_at__gte=now - timedelta(days=1)).count()
    return {"hour": max(0, MINT_LIMIT_PER_HOUR - hour), "day": max(0, MINT_LIMIT_PER_DAY - day)}


def _check_rate_limit(user):
    now = timezone.now()
    qs = LocusRegistryEntry.objects.filter(minted_by=user, minted_via=LocusRegistryEntry.VIA_MODAL)
    recent_hour = list(qs.filter(minted_at__gte=now - timedelta(hours=1))
                       .order_by("minted_at").values_list("minted_at", flat=True))
    if len(recent_hour) >= MINT_LIMIT_PER_HOUR:
        retry = int((recent_hour[0] + timedelta(hours=1) - now).total_seconds()) + 1
        raise MintRateLimited("hour", MINT_LIMIT_PER_HOUR, max(1, retry))
    recent_day = list(qs.filter(minted_at__gte=now - timedelta(days=1))
                      .order_by("minted_at").values_list("minted_at", flat=True))
    if len(recent_day) >= MINT_LIMIT_PER_DAY:
        retry = int((recent_day[0] + timedelta(days=1) - now).total_seconds()) + 1
        raise MintRateLimited("day", MINT_LIMIT_PER_DAY, max(1, retry))


# ---------------------------------------------------------------- recipes

def recipe_binding(recipe) -> dict:
    """``{"recipe", "canonical", "digest", "pid", "token"}`` for any valid recipe.
    Raises ``RecipeError`` for anything else, including pair/acceptance keys
    (place#306): the parser is the ONE gate and is reused, not re-implemented."""
    canonical = parse_recipe(recipe)
    b = canonical_bytes(canonical)
    digest = LocusRegistryEntry.digest_of(b)
    return {
        "recipe": canonical,
        "canonical": b,
        "digest": digest,
        "pid": pid_from_digest(bytes.fromhex(digest)),
        "token": encode_recipe_token(canonical),
    }


def existing_for_recipe(recipe) -> tuple[Optional[LocusRegistryEntry], dict]:
    """``(entry or None, binding)``. Raises ``LocusCollision`` when the PID is
    held by different bytes."""
    binding = recipe_binding(recipe)
    entry = LocusRegistryEntry.objects.filter(pk=binding["pid"]).first()
    if entry is not None and entry.canonical_bytes != binding["canonical"]:
        raise LocusCollision(binding["pid"])
    return entry, binding


def mint_recipe(recipe, user, via: str = LocusRegistryEntry.VIA_MODAL,
                minter_public: bool = False) -> tuple[LocusRegistryEntry, bool]:
    """Bind the recipe's PID to its canonical bytes. ``(entry, created)``.

    ``minter_public`` (the modal's checkbox, default off) is written with the
    row and only honoured when this call CREATES it: an idempotent re-mint,
    by anyone, never changes the first minter's choice. It can later only be
    turned off (``LocusRegistryEntry.hide_minter``; migration 0028).

    Raises ``RecipeError`` (invalid recipe), ``LocusCollision``,
    ``MintRateLimited``, or ``ValueError`` when ``user`` is not a signed-in
    account (a permanent public identifier has a person attached, place#172).
    """
    if user is None or not getattr(user, "is_authenticated", False) or not getattr(user, "pk", None):
        raise ValueError("minting requires a signed-in account")
    entry, binding = existing_for_recipe(recipe)
    if entry is not None:
        return entry, False          # idempotent: same bytes, same locus, nothing new owed
    try:
        with transaction.atomic():
            # One user's mints are serialised: count and insert under one
            # transaction-scoped lock (released at commit or rollback). Without it,
            # parallel POSTs each counted the same total and all minted (review #11).
            with connection.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(%s, %s)", [MINT_LOCK_NAMESPACE, int(user.pk)])
            # A parallel mint of THIS recipe may have committed while we waited:
            # that is the idempotent answer, not a rate-limit refusal.
            done = LocusRegistryEntry.objects.filter(pk=binding["pid"]).first()
            if done is not None:
                if done.canonical_bytes != binding["canonical"]:
                    raise LocusCollision(binding["pid"])
                return done, False
            _check_rate_limit(user)
            entry = LocusRegistryEntry.objects.create(
                pid=binding["pid"], kind=LocusRegistryEntry.KIND_RECIPE, version=RECIPE_VERSION,
                canonical=binding["canonical"], digest=binding["digest"], token=binding["token"],
                members=list(binding["recipe"]["members"]),
                minted_by=user, minted_via=via, minter_public=minter_public is True,
            )
            return entry, True
    except IntegrityError:
        # Lost a race on the primary key (or the digest): somebody minted it
        # between our read and our insert. Re-read and judge the bytes.
        entry = LocusRegistryEntry.objects.filter(pk=binding["pid"]).first()
        if entry is None:
            raise
        if entry.canonical_bytes != binding["canonical"]:
            raise LocusCollision(binding["pid"]) from None
        return entry, False


def overlapping(members, exclude_pid: Optional[str] = None, limit: int = 10) -> list[dict]:
    """Existing loci sharing at least one member id with ``members``, most
    shared first: the modal surfaces them so a person can reuse one instead of
    minting another (place#172, "the cheapest convergence pressure")."""
    members = [m for m in (members or []) if isinstance(m, str) and m]
    if not members:
        return []
    # A withdrawn or redacted locus is never offered: it cannot be reused, and
    # naming the members it shares would disclose part of a suppressed recipe.
    qs = (LocusRegistryEntry.objects.filter(members__overlap=members)
          .filter(status=LocusRegistryEntry.STATUS_ACTIVE, redacted_at__isnull=True))
    if exclude_pid:
        qs = qs.exclude(pk=exclude_pid)
    out = []
    want = set(members)
    for e in qs.only("pid", "kind", "status", "members", "minted_at")[: limit * 4]:
        shared = sorted(want.intersection(e.members))
        out.append({"pid": e.pid, "kind": e.kind, "status": e.status, "shared": shared,
                    "member_count": len(e.members), "minted_at": e.minted_at.isoformat(),
                    "url": e.entity_path})
    out.sort(key=lambda d: (-len(d["shared"]), d["minted_at"]))
    return out[:limit]


# ---------------------------------------------------------------- legacy

def legacy_canonical(row) -> bytes:
    """The bytes a legacy locus is bound to: the snapshot row, canonical JSON.
    ``"kind":"legacy"`` is not a recipe key, so these bytes can never parse as
    a recipe and no legacy row can ever be mistaken for a live one."""
    doc = {"kind": "legacy", "place_ids": [int(p) for p in row.place_ids],
           "titles": [str(t) for t in row.titles], "whg_id": int(row.whg_id)}
    return json.dumps(doc, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def mint_legacy(whg_id: str) -> tuple[Optional[LocusRegistryEntry], bool]:
    """Mint (on first dereference) the frozen locus for a legacy ``whg_id``.
    ``(None, False)`` when the snapshot has no such row."""
    from places.models import LegacyUnionRecord
    pid = str(int(whg_id))
    entry = LocusRegistryEntry.objects.filter(pk=pid).first()
    if entry is not None:
        return entry, False
    row = LegacyUnionRecord.objects.filter(pk=int(whg_id)).first()
    if row is None:
        return None, False
    canonical = legacy_canonical(row)
    try:
        with transaction.atomic():
            entry = LocusRegistryEntry.objects.create(
                pid=pid, kind=LocusRegistryEntry.KIND_LEGACY, version=0,
                canonical=canonical, digest=LocusRegistryEntry.digest_of(canonical), token="",
                members=[f"whg:{int(p)}" for p in row.place_ids],
                minted_by=None, minted_via=LocusRegistryEntry.VIA_DEREFERENCE,
            )
            return entry, True
    except IntegrityError:
        entry = LocusRegistryEntry.objects.filter(pk=pid).first()
        if entry is None:
            raise
        if entry.canonical_bytes != canonical:
            raise LocusCollision(pid) from None
        return entry, False


# ---------------------------------------------------------------- lookup

def lookup(pid_text: str, mint_legacy_on_dereference: bool = True) -> tuple[LocusRegistryEntry, bool]:
    """Resolve the text after ``locus:`` to a registry entry. ``(entry, created)``.

    * a recipe PID (``XXXX-XXXX-XXXX`` in any accepted spelling): 400 when it
      is not well-formed or its check digit fails (a typo is a hard error,
      never another locus); 404 when no locus has been minted under it. A
      PID is a hash: there is nothing to mint on dereference, so an unminted
      PID stays 404 until someone mints its recipe on the locus page;
    * a legacy ``1[0-9]{7}``: minted from the snapshot on first dereference,
      404 when the snapshot has no such whg_id.

    Raises :class:`LookupError_` with the status to answer.
    """
    text = (pid_text or "").strip()
    if not text:
        raise LookupError_(400, "no locus identifier was given")
    if is_legacy_locus_id(text):
        if not mint_legacy_on_dereference:
            entry = LocusRegistryEntry.objects.filter(pk=str(int(text))).first()
            if entry is None:
                raise LookupError_(404, f"no locus has been minted for legacy id {text}")
            return entry, False
        entry, created = mint_legacy(text)
        if entry is None:
            raise LookupError_(404, f"{text} is not a legacy WHG identifier held in the snapshot")
        return entry, created
    try:
        pid = normalise_pid(text)
    except ValueError as exc:
        raise LookupError_(400, f"{text!r} is not a well-formed locus PID: {exc}") from None
    entry = LocusRegistryEntry.objects.filter(pk=pid).first()
    if entry is None:
        raise LookupError_(404, f"no locus has been minted with PID {pid}")
    return entry, False


def entry_recipe(entry: LocusRegistryEntry) -> Optional[dict]:
    """The canonical recipe dict of a recipe row (None for legacy)."""
    if entry.kind != LocusRegistryEntry.KIND_RECIPE:
        return None
    try:
        return parse_recipe(json.loads(entry.canonical_bytes.decode("utf-8")))
    except (RecipeError, ValueError, UnicodeDecodeError):
        return None


def entry_legacy(entry: LocusRegistryEntry) -> Optional[dict]:
    """The frozen snapshot of a legacy row (None for recipe rows)."""
    if entry.kind != LocusRegistryEntry.KIND_LEGACY:
        return None
    try:
        return json.loads(entry.canonical_bytes.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
