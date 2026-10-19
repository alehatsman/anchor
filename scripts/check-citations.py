#!/usr/bin/env python3
"""Checks that every intra-repo doc citation resolves.

Twenty-five citations in `src/` and `tests/` once pointed at headings that
do not exist, and three at teleport filenames absent from this repo (#17).
Nothing rejected them, because nothing read them. This does.

It checks two things about every `docs/NN-name.md[#fragment]` reference:
the file exists, and the fragment matches a heading in it. Cross-repo
references (teleport's own docs) are reported as skipped rather than
checked -- teleport is not present in CI, and a checker that silently
ignored them would be claiming more coverage than it has.

The suite must not be able to pass by not running (docs/08-testing.md), so
finding no references at all is a failure. A path typo in this script would
otherwise read as a clean bill of health.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Where citations are allowed to live. A file outside this set is not
# checked, so widening it is how coverage grows.
SOURCES = [
    *ROOT.glob("src/**/*.rs"),
    *ROOT.glob("tests/**/*.rs"),
    *ROOT.glob("docs/*.md"),
    ROOT / "README.md",
    ROOT / "ROADMAP.md",
]

# `docs/04-protocol.md#heading`, or bare `04-protocol.md#heading` as written
# from inside docs/. The trailing character class keeps a fragment from
# swallowing the prose that follows it.
CITATION = re.compile(r"(?:docs/)?(\d\d-[a-z0-9-]+\.md)(#[a-zA-Z0-9_-]+)?")

# A citation reached through one of these is talking about teleport's copy of
# a similarly-named file, not ours. It has to cover the three spellings in
# use: `teleport/docs/x.md`, teleport's `docs/x.md`, and a github.com URL
# with the repo in its path.
CROSS_REPO = re.compile(r"(?:teleport\S*|\.\.)[\s/`'\"]*(?:docs/)?$")

HEADING = re.compile(r"^#{1,6}\s+(.*?)\s*$", re.MULTILINE)


def slugify(heading: str) -> str:
    """GitHub's heading-anchor rules, as far as this repo exercises them."""
    text = heading.strip().lower()
    text = re.sub(r"[`*_\[\]()]", "", text)  # inline markup is not in the slug
    text = re.sub(r"[^\w\s-]", "", text)  # punctuation, em dashes included
    return re.sub(r"\s", "-", text)


def anchors_of(path: Path) -> set[str]:
    return {slugify(h) for h in HEADING.findall(path.read_text(encoding="utf-8"))}


def main() -> int:
    anchors: dict[Path, set[str]] = {}
    problems: list[str] = []
    checked = 0
    skipped = 0

    for source in sorted(set(SOURCES)):
        if not source.exists():
            problems.append(f"{source.relative_to(ROOT)}: listed as a source but missing")
            continue
        for lineno, line in enumerate(
            source.read_text(encoding="utf-8").splitlines(), start=1
        ):
            for match in CITATION.finditer(line):
                where = f"{source.relative_to(ROOT)}:{lineno}"
                if CROSS_REPO.search(line[: match.start()]):
                    skipped += 1
                    continue

                checked += 1
                target = ROOT / "docs" / match.group(1)
                if not target.exists():
                    problems.append(f"{where} -> {match.group(1)} (no such file)")
                    continue

                fragment = match.group(2)
                if fragment is None:
                    continue
                if target not in anchors:
                    anchors[target] = anchors_of(target)
                if fragment[1:] not in anchors[target]:
                    problems.append(
                        f"{where} -> {match.group(1)}{fragment} (no such heading)"
                    )

    # The guard that stops this passing by not running. A broken glob or a
    # moved directory lands here rather than printing a clean result.
    if checked == 0:
        print("FAIL: no citations found at all -- this checker is not reading anything")
        return 1

    for problem in problems:
        print(f"FAIL: {problem}")
    print(
        f"{checked} intra-repo citations checked, "
        f"{skipped} cross-repo skipped, {len(problems)} broken"
    )
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
