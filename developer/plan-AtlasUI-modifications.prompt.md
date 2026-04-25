### Atlas UI Modifications Plan

## Introduction

The prototype Atlas UI has inspired plans to extend it as a default UI for the entire site. Distinctions are no longer to be drawn between "Authorities", "Datasets", and "Collections", all of which are to be referred to as "Gazetteers".

## Implementation Plan

1. In `atlas.html` and associated CSS and JS, the "Sources" button should be reconceptualised and labelled "Gazetteers". The "Data Sources offcanvas" section should be renamed "Gazetteers" and references to "sources" in its constituent element `id`s should be altered to maintain clarity.
2. Because we will soon be implementing dynamic clustering (as outlined in developer/plan-dynamicClusteringUI.prompt.md), the "Clustering" section should be relocated from the "Sources"/"Gazetteers" offcanvas to appear at the top of the Results panel when it is shown.
3. The "Toponyms" button should be relabelled as "Places".
4. The renamed "Gazetteers" offcanvas will need to be populated from an extended `/suggest` API which will provide lists of available gazetteers (including what are elsewhere in the codebase known as Authorities and also the individual WHG Datasets -- which are now being indexed as separately-namespaced Authorities -- and Collections).
5. The Gazetteers list will serve two distinct functions, the selection of which will need to be clearly articulated, perhaps by tabbing(Filter|Explore, or similar) which switches between checkboxes and radio buttons, and changes the labelling of an action button:
    - When more that one is selected, they serve as a filter for subsequent searches.
    - When only one is selected, the Atlas UI becomes a "Gazetteer Explorer", taking over the role currently served by separate pages for Dataset Browse and two types of Collection Browse.
6. A toggle filter on the Gazetteers list should allow logged-in users to show (and subsequently explore) only their own gazetteers.
7. The main site navigation bar can then be rationalised:
    - The "Search" option should be removed (in its current `dev`-server form it was a prototype which led to development of the Atlas UI).
    - The "Workbench" will remain for now, although the contribution pipeline will probably need to be restructured in subsequent development. It could be moved entirely to the Documentation site where it would be more easily edited and maintained.
    - The "Teaching" option will remain but will incorporate new material from OME once the integration is active. References to "Place Collection" and "Collection Groups" will need reframing as "Gazetteers". Some of the content could be moved to the Documentation site. The link might better be served as a dropdown rather than a single page.
    - The "Data" option should be removed as much of its functionality will be provided by the Gazetteers list (i.e. "My Data", "Published Datasets", and "Published Collections"). Functions that will need to move to other options include "Admin Dashboard", "API", and "Volunteering".
8. The content of the `.atlas-welcome-title` element could absorb the function of the current main site landing page, making that redundant.