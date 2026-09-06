#!/usr/bin/env python3
"""Set ``DOCKER_IMAGE_TAG`` for one site in the DO server's ``env_template.py``.

**Why this exists.** A dependency change has to be baked into a new image
(``build_docker.py``), and then the deployed site has to be pointed at that new
tag — which lives in ``/home/whgadmin/sites/env_template.py``, a file that is
deliberately NOT in the repository because it holds credentials. Those two steps
used to be a build on one machine and a remembered hand-edit on another, and the
remembering is where it went wrong: an image gets pushed, the tag never moves,
and the site keeps running the old image while its ``requirements.txt`` says
otherwise. Nothing reports that; it shows up later as an ImportError.

``deploy.sh --image=<tag>`` calls this immediately before regenerating config, so
the bump happens in the same command as the deploy, on the machine that holds the
file. See ``developer/build-image.md``.

The edit is deliberately narrow: exactly one ``'DOCKER_IMAGE_TAG': '…'`` line,
inside exactly one site's block, matched by indentation. Anything else — a
missing site, a missing key, more than one match — is an error rather than a
guess, because the alternative to a clear failure here is a silently
mis-tagged production stack.
"""

import argparse
import re
import shutil
import sys
from pathlib import Path

DEFAULT_TEMPLATE = Path('/home/whgadmin/sites/env_template.py')
# Site blocks are nested inside ENV_VARS['sites'], so they sit at eight spaces
# and their contents at twelve. Matching on that indentation is what keeps the
# edit inside one site instead of running on into the next.
SITE_RE = "        '{site}': {{"
SITE_BODY_INDENT = ' ' * 12
TAG_RE = re.compile(r"^(\s*'DOCKER_IMAGE_TAG':\s*')([^']*)('.*)$")


def set_tag(path, site, tag):
    """Returns ``(old_tag, changed)``. Raises ValueError if it cannot be certain."""
    lines = path.read_text(encoding='utf-8').splitlines(keepends=True)

    start = next((i for i, line in enumerate(lines)
                  if line.rstrip('\n') == SITE_RE.format(site=site)), None)
    if start is None:
        raise ValueError(f"no site block for {site!r} in {path}")

    # The block ends at the next line indented by exactly four spaces that is not
    # a continuation — i.e. the next site's opening line, or the closing brace.
    end = len(lines)
    for i in range(start + 1, len(lines)):
        stripped = lines[i].rstrip('\n')
        if stripped and not stripped.startswith(SITE_BODY_INDENT):
            end = i
            break

    matches = [i for i in range(start, end) if TAG_RE.match(lines[i])]
    if len(matches) != 1:
        raise ValueError(f"expected exactly one DOCKER_IMAGE_TAG in {site!r}, "
                         f"found {len(matches)}")

    index = matches[0]
    match = TAG_RE.match(lines[index])
    old = match.group(2)
    if old == tag:
        return old, False
    lines[index] = f'{match.group(1)}{tag}{match.group(3)}\n'
    shutil.copy2(path, path.with_suffix('.py.bak'))
    path.write_text(''.join(lines), encoding='utf-8')
    return old, True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--site', required=True,
                        help="Site key, e.g. dev-whgazetteer-org")
    parser.add_argument('--tag', required=True, help="Image tag, e.g. 1.0.19")
    parser.add_argument('--template', default=str(DEFAULT_TEMPLATE))
    args = parser.parse_args()

    path = Path(args.template)
    if not path.is_file():
        sys.exit(f"env template not found: {path}")
    try:
        old, changed = set_tag(path, args.site, args.tag)
    except ValueError as exc:
        sys.exit(f"refusing to edit {path}: {exc}")
    if changed:
        print(f"{args.site}: DOCKER_IMAGE_TAG {old} → {args.tag} (backup: {path}.bak)")
    else:
        print(f"{args.site}: DOCKER_IMAGE_TAG already {args.tag}")


if __name__ == '__main__':
    main()
