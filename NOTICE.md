# NOTICE

World Historical Gazetteer
Copyright (c) 2019-2026 University of Pittsburgh

## Licensing

**WHG asserts no single blanket licence over what it holds.** It is simultaneously a
licensee and a licensor: most of what the platform serves belongs to somebody else, and
WHG cannot grant downstream what it does not hold upstream. Each item below therefore
states its own scope, and nothing here should be read as a grant over material WHG did
not create.

- **WHG software:** licensed under the [BSD 3-Clause License](LICENSE). That licence
  covers WHG's own code only.

- **WHG's curation and aggregation layer:** the linkage, reconciliation and editorial
  work WHG contributes on top of the sources it ingests is licensed
  [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). This is an *overlay*.
  It is asserted **alongside** each source's own terms and **never instead of them**, and
  it does not extend to the underlying records. It is defined once, as
  `WHG_OVERLAY_LICENSE` in `whg/settings.py`, and applied through `licensing/statements.py`.

- **WHG's own editorial content:** site pages, documentation and images created by WHG
  are likewise [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

  Both were CC BY-NC 4.0 until 30 September 2026. Material already distributed under that
  licence keeps it; everything from that date is CC BY 4.0.

- **Contributed datasets and collections:** licensed **individually by the contributor**,
  not by WHG, and not under any site-wide licence. The licence a contributor selected is
  recorded against the item, travels with the data wherever it leaves the platform, and is
  surfaced at [/licenses/](https://whgazetteer.org/licenses/), on each dataset page, and in
  API responses. **Where no licence is recorded, nothing is asserted** — that is a statement
  about WHG's records, not a claim that the item is unrestricted.

  ⚠ An earlier version of this file claimed CC BY-NC 4.0 over all contributed content. That
  was wrong in scope: it applied WHG's overlay to other people's material. It was also
  self-defeating, because CC BY 4.0 §2(a)(5)(B) forbids a recipient from imposing downstream
  restrictions, so a NonCommercial overlay on data granted to WHG under CC BY 4.0 breaches
  the licence WHG itself received. The same error was corrected across every download surface
  in place#157; this file was missed.

- **WHG research datasets published with a DOI:** licensed as stated on the published
  record, which governs. These are not covered by the overlay above. The Symphonym models,
  evaluation data and Epitran rule sets are published open access under
  [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) at
  [10.5281/zenodo.18682017](https://doi.org/10.5281/zenodo.18682017).

- **Externally hosted content:** datasets or materials linked to by WHG remain under the
  copyrights and licences specified by their original contributors. WHG's licensing does not
  apply to externally hosted content.

- **Third-party software packages:** WHG depends on a wide range of open source libraries,
  distributed under their own terms (MIT, Apache 2.0, BSD and others). The full list, with the
  licence of each, is generated from the running environment into
  [THIRD_PARTY_LICENSES](THIRD_PARTY_LICENSES) by `manage.py audit_licenses`, and is also
  published at [/licenses/software/](https://whgazetteer.org/licenses/software/). Package
  manifests (`requirements.txt`, `package.json`, Docker image definitions) remain the
  authoritative record of what is installed.

---

This NOTICE file summarises copyright and licensing for the WHG project. Where it and a
specific licence, published record or contributor agreement disagree, the specific one
governs.
