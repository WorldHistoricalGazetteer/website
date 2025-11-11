# datasets/task_utils.py
"""
Task-specific utilities.
Imports from core_utils to avoid duplication while preventing circular import issues.
"""
import logging

# Import from core_utils (no Django dependencies - safe)
from .core_utils import (
    makeNow,
    HitRecord,
    parse_wkt,
    elapsed,
    flatten
)

logger = logging.getLogger(__name__)


def bestParent(qobj, flag=False):
    """
    Get best parent hierarchy for TGN.
    Applicable for TGN only.

    Args:
        qobj: Query object with countries and parents
        flag: Optional flag (unused)

    Returns:
        list: Parent labels
    """
    # Lazy import to avoid circular dependency
    from datasets.static.hashes.parents import ccodes as parents_ccodes

    best = []

    # Merge parent country/ies & parents
    if len(qobj['countries']) > 0 and qobj['countries'][0] != '':
        for c in qobj['countries']:
            if c.upper() in parents_ccodes[0]:
                best.append(parents_ccodes[0][c.upper()]['tgnlabel'])

    if len(qobj['parents']) > 0:
        for p in qobj['parents']:
            best.append(p)

    if len(best) == 0:
        best = ['World']

    return best