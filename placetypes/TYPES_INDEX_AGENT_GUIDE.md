# `types` Index Agent Guide

This guide is for coding agents that need to query the Elasticsearch `types` index for:

1. Reverse lookup from non-AAT identifiers (GeoNames/Wikidata/OSM/OHM) to AAT
2. Retrieving `path` for a known AAT type
3. Basic hierarchy/path operations

---

## What `types` stores

The `types` index is **AAT-centric**: one document per AAT concept (typically `aat:<id>`), with optional arrays for non-AAT mappings.

### Typical identity

- `_id`: `aat:<aat_id>` (e.g., `aat:300008347`)
- `aat_id`: integer (e.g., `300008347`)

### Core fields

- `aat_id` (`int`)
- `term` (`text/keyword-style usage`)
- `term_full` (`text`)
- `note` (`text`)
- `fclasses` (`array[str]`, e.g. `A,P,S,R,L,T,H,U`)
- `path` (`str`, dot-separated AAT IDs from root to self)
- `depth` (`int`)
- `is_place_type` (`bool`)

### Mapping fields (optional)

- `gn_fcodes` (`array[str]`) e.g. `P.PPL`
- `wd_qids` (`array[str]`) e.g. `Q515`
- `osm_tags` (`array[str]`) e.g. `place=city`
- `ohm_tags` (`array[str]`) e.g. `place=city`
- `mapping_conf` (`object`) nested by source field, e.g.:
  - `mapping_conf.gn_fcodes.P.PPL = "exact"`
  - `mapping_conf.wd_qids.Q515 = "close"`

### Example document (abridged)

```json
{
  "aat_id": 300008347,
  "term": "inhabited places",
  "term_full": "inhabited places",
  "note": "...",
  "fclasses": ["P"],
  "path": "300264550.300008346.300008347",
  "depth": 2,
  "is_place_type": true,
  "gn_fcodes": ["P.PPL", "P.PPLA"],
  "wd_qids": ["Q515"],
  "osm_tags": ["place=city"],
  "ohm_tags": ["place=city"],
  "mapping_conf": {
    "gn_fcodes": {"P.PPL": "exact"},
    "wd_qids": {"Q515": "close"},
    "osm_tags": {"place=city": "exact"}
  }
}
```

---

## Reverse lookup patterns (non-AAT -> AAT)

Use **exact `term` queries** on the mapping arrays.

### GeoNames code -> AAT

```json
POST /types/_search
{
  "size": 10,
  "_source": ["aat_id", "term", "path", "mapping_conf.gn_fcodes"],
  "query": {
    "term": {
      "gn_fcodes": "P.PPL"
    }
  }
}
```

### Wikidata QID -> AAT

```json
POST /types/_search
{
  "size": 10,
  "_source": ["aat_id", "term", "path", "mapping_conf.wd_qids"],
  "query": {
    "term": {
      "wd_qids": "Q515"
    }
  }
}
```

### OSM tag -> AAT

```json
POST /types/_search
{
  "size": 10,
  "_source": ["aat_id", "term", "path", "mapping_conf.osm_tags"],
  "query": {
    "term": {
      "osm_tags": "place=city"
    }
  }
}
```

### OHM tag -> AAT

```json
POST /types/_search
{
  "size": 10,
  "_source": ["aat_id", "term", "path", "mapping_conf.ohm_tags"],
  "query": {
    "term": {
      "ohm_tags": "place=city"
    }
  }
}
```

### Unified OSM/OHM lookup (either field)

```json
POST /types/_search
{
  "size": 10,
  "_source": [
    "aat_id",
    "term",
    "path",
    "mapping_conf.osm_tags",
    "mapping_conf.ohm_tags"
  ],
  "query": {
    "bool": {
      "should": [
        {"term": {"osm_tags": "place=city"}},
        {"term": {"ohm_tags": "place=city"}}
      ],
      "minimum_should_match": 1
    }
  }
}
```

---

## Retrieve `path` for a known AAT type

### Preferred: direct by `_id`

```json
GET /types/_doc/aat:300008347
```

### Alternative: by numeric `aat_id`

```json
POST /types/_search
{
  "size": 1,
  "_source": ["aat_id", "term", "path", "depth", "is_place_type"],
  "query": {
    "term": {
      "aat_id": 300008347
    }
  }
}
```

---

## Path/hierarchy operations

Given `path = "300264550.300008346.300008347"`:

- Self id = last segment (`300008347`)
- Ancestors = all prior segments (`300264550`, `300008346`)
- Parent id = penultimate segment (`300008346`) if depth > 0

### Descendants of AAT `X`

Prefix match on `path`:

```json
POST /types/_search
{
  "size": 1000,
  "_source": ["aat_id", "term", "path", "depth"],
  "query": {
    "prefix": {
      "path": "300008347."
    }
  }
}
```

### Include self + descendants

Use `bool.should` with exact `term` on `aat_id` plus `prefix` on `path`.

```json
POST /types/_search
{
  "size": 1000,
  "_source": ["aat_id", "term", "path", "depth"],
  "query": {
    "bool": {
      "should": [
        {"term": {"aat_id": 300008347}},
        {"prefix": {"path": "300008347."}}
      ],
      "minimum_should_match": 1
    }
  }
}
```

---

## Ranking/selection guidance for reverse lookups

If multiple AAT hits exist for one non-AAT identifier:

1. Prefer confidence in `mapping_conf` (`exact` > `close` > `review`)
2. Prefer `is_place_type = true`
3. If still tied, prefer lower `depth` only if your use case wants broader terms; otherwise prefer more specific terms (higher depth)

---

## Practical caveats

- Not all docs have mapping arrays; many AAT docs are unmapped.
- `mapping_conf` can be absent or sparse; default confidence may be implied by application logic.
- Use `term` query for IDs/codes/tags (avoid `match` for exact identifiers).
- `path` is a dot-separated string of numeric IDs, not an array.
- Document `_id` convention is `aat:<id>` and is used by mapping update scripts.

---

## Minimal query checklist for agents

- Reverse lookup GeoNames: `term gn_fcodes=<code>`
- Reverse lookup Wikidata: `term wd_qids=<qid>`
- Reverse lookup OSM/OHM: `bool should(term osm_tags=<tag>, term ohm_tags=<tag>)`
- Find path by AAT id: `GET /types/_doc/aat:<id>` (or `term aat_id=<id>`)
- Traverse descendants: `prefix path="<id>."`

