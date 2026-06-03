# leads/services/gapvalue.py
"""
WHG gap-value automation.

Estimates how thinly a lead's region is covered in the live `places` index
(via settings.CRC_GATEWAY_URL) and returns a 0-100 score where higher = thinner
= more valuable to ingest. Advisory only: a human still sets priority_score.

NOTE: full implementation lands in build-order step 5. Stub raises until then.
"""


def gap_value_for(lead):
    """Return a 0-100 gap-value estimate for a DatasetLead. Higher = thinner coverage."""
    raise NotImplementedError(
        "gap_value_for() is not implemented yet (build-order step 5)."
    )
