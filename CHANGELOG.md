# Changelog

All notable changes to the World Historical Gazetteer are documented in this file.

This project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [3.2] – 2025-10-29

**Feature release** introducing enhanced interoperability, scholarly authentication, and spatiotemporal filtering
capabilities. This release extends WHG's integration with external scholarly infrastructure and deepens its role as a
Linked Open Data service for historical place research.

### Added

- ORCID user authentication, allowing contributors to sign in and register via their ORCID iD for improved attribution,
  provenance tracking, and integration with scholarly research profiles
- Reconciliation Service API, enabling external applications to match their place data programmatically against the WHG
  index, supporting large-scale alignment of historical datasets and Linked Open Data interoperability
- PeriodO temporal and spatial filters, allowing users to constrain searches and visualizations by historical periods
  and regions defined in the PeriodO authority for precise chronogeographic exploration
- Sphinx-based documentation platform with markdown source files deployed via GitHub Pages, facilitating collaborative
  documentation improvements

### Changed

- User account management workflows updated to incorporate ORCID-linked profiles and enhanced contributor metadata
- API documentation expanded with examples for new Reconciliation Service endpoints
- Search and map interfaces enhanced to support combined temporal and spatial constraints from PeriodO
- General improvements to system performance, data indexing, and search response times

### Note

This release represents a major step toward full Linked Open Data integration, enabling scholarly authentication and
external reconciliation workflows for the first time. It lays the foundation for future semantic interoperability and
data enrichment through the PeriodO and ORCID ecosystems.

---

## [3.1] – 2025-06-30

**Full release** marking the conclusion of the 2023 Digital Humanities Advancement Grant from the National Endowment for
the Humanities. This release represents continuous refinements and improvements to the platform throughout the grant
period.

### Added

- Persistent identifiers (DOIs) for published dataset and collection contributions, issued via DataCite to ensure
  long-term citability and scholarly traceability
- Citation support with CSL (Citation Style Language), enabling formatted citation exports (e.g. APA, MLA, Chicago)
  directly from contribution pages
- Search engine optimisation (SEO) improvements through inclusion of schema.org structured metadata in page headers to
  enhance discoverability and indexing of WHG resources

### Changed

- Ongoing refinements and optimizations to all v3.0 features based on user feedback
- Performance improvements and bug fixes across the platform
- Documentation migrated to GitHub Pages for improved accessibility and version control
- Significant planning and preparation for future migration to University of Pittsburgh CRC infrastructure
- Development approach adapted to support future Kubernetes (K8s) deployment architecture
- Planning for transition from Elasticsearch to Vespa backend to enable vector indexing of phonetic forms of toponyms

### Note

Detailed changelog entries were not maintained during the active development phase between v3.0 beta and this release.
Much of the development effort during this period focused on strategic planning for the platform's migration to Pitt
University CRC and transition from Docker to Kubernetes orchestration. Some refinements were tailored or deferred with
this future architecture in mind, including preparation for replacing Elasticsearch with Vespa for enhanced phonetic
toponym search capabilities.

---

## [3.0 beta] – 2024-06-19

**Major beta release** funded by the 2023 Digital Humanities Advancement Grant from the National Endowment for the
Humanities. This beta release sought user feedback toward a full v3.1 release.

### Home Page

- New search option with enhanced capabilities
- Carousel of featured published datasets and collections with map extent previews
- Simplified explanation of WHG platform offerings
- News and announcements section

### Documentation

- Single navigable documentation section consolidating all guides and tutorials
- Additional help screens throughout the site
- Improved user guidance and onboarding

### Maps

- Significant upgrades to all 14 maps across the site
- Temporal controls: timespan slider and sequence player on most maps
- Faster display of large datasets and collections via WHG's new map tile server
- Consistent appearance and improved performance across all map-related functions

### Search

- New search modes: "starts with", "contains", "similar to" (fuzzy), and "exact"
- Unified search across all published records (eliminated previous "index vs database" choice)
- Spatial filter for search results
- Enhanced information in search result items

### Place Portal Pages

- Complete design overhaul
- Physical geographic context: ecoregions, watersheds, rivers
- Nearby places feature
- Preview of annotated collections that include the place

### Dataset Creation and Reconciliation

- Expanded reconciliation index to 13.6 million records
- Added 10 million GeoNames records to existing 3.6 million from Wikidata
- Improved file upload validation and error reporting

### Publication and Editorial Workflow

- Highlighted publication types: Datasets, Annotated Place Collections, and Dataset Collections (with DOIs coming soon)
- Significantly enhanced presentation of published datasets and collections
- Enhanced metadata options for all publication types
- Expanded Managing Editor role for smoother workflow
- Improved tracking of contributors and data in pipeline
- Better download options

### Annotated Place Collections

- Support for teaching and workshop scenarios
- Optional image per annotation
- Sequential place ordering with or without dates
- Enhanced display and temporal control options
- Optional gallery per class
- Site-wide student gallery

### Dataset Collections

- Link multiple datasets and their places in a single collection
- Enables creation of collaborative or individual "Historical Gazetteer of {x}" projects

### My Data Dashboard

- Simplified single-page dashboard
- New user profile page
- Streamlined interface for managing personal content

### Study Areas

- Support for non-contiguous areas (e.g., Iberian Peninsula and South America as a single area)

### API and Data Downloads

- Solidified existing endpoints
- Improved download options and formats

### Codebase

- Containerized with Docker for easier development contributions
- Upgraded versions of all major components: Django, PostgreSQL, Elasticsearch
- Refactored all map-related functions for efficiency and consistency
- Enhanced file upload validation and error reporting

---

## [2.1 beta] – 2022-04-27

### Added

- **Place Collection** feature alongside existing Dataset Collection capability
- Composition of collections from individual records or entire datasets
- Annotations and rich media support for collections

---

## [2.0.1] – 2022-02-01

### Added

- **Teaching** section with eight lesson plans (mainly secondary-school level)

---

## [2.0] – 2021-08-04

### Added

- **Collection** feature with *Dutch History* and *HGIS de las Indias* examples
- Public views for datasets, collections, and individual records
- SSL (HTTPS) support
- Formation of editorial team

### Changed

- Revamped search system: database vs index with filters for type, country, map bounds
- Switched maps to **MapLibre GL** for faster rendering
- Improved reliability and error reporting for uploads
- Rewritten documentation with new tutorials
- Redesigned home page
- Major code refactoring

---

## [1.21] – 2021-05-11

### Added

- Deferred queue for reconciliation tasks
- Review-status tracking per record and per task
- Collections feature for beta testers

### Changed

- Enhanced "Reconcile to WHG" accessioning logic

---

## [1.2] – 2021-03-05

### Added

- 3.5 million-record Wikidata index for reconciliation
- Dataset collaborator roles: *member* and *co-owner*
- Enhanced task progress tracking and status feedback

### Changed

- Improved authorization system (registration, login, password recovery)

---

## [1.1a] – 2021-01-11

### Added

- Upload/validation support for `.csv`, `.xlsx`, and `.ods` formats
- Automatic country-code computation when geometry is present

### Fixed

- Wikidata links
- Error reporting
- Temporal parsing issues

---

## [1.1] – 2020-12-10

### Added

- Queued reconciliation tasks with email notifications

### Changed

- Replaced base map with **Natural Earth** tiles

### Fixed

- Numerous minor bug fixes

---

## [1.0] – 2020-07-27

### Added

- Accessioned 28,000 records from Pleiades, Euratlas, and OWTRAD
- APIs for database and Elasticsearch access
- Expanded trace data (+70 annotation sets)
- Enhanced search filters: feature class, spatial, temporal
- Comprehensive Site Guide
- Dataset download options
- SSL enabled

### Changed

- Improved GUI with home page redesign

---

## [0.4 beta] – 2020-05-02

### Added

- Dataset downloads (raw or augmented formats)
- Draft public API (`q`, `dataset`, `ccodes` endpoints)

### Changed

- Miscellaneous UI improvements

---

## [0.3 beta] – 2020-02-25

### Added

- TGN and GeoNames records to core data
- Dataset update mechanism (diff-based upload)
- *Collaborator* role for dataset teams
- *Undo last reconciliation* action

### Changed

- Consolidated dataset-management tabs
