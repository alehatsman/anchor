#!/usr/bin/env python3
"""Checks that every intra-repo doc citation resolves.

Twenty-five citations in `src/` and `tests/` once pointed at headings that
do not exist, and three at teleport filenames absent from this repo (#17).
Nothing rejected them, because nothing read them. This does.

It checks two things about every `docs/NN-name.md[#fragment]` reference:
the file exists, and the fragment matches a heading in it.

It also checks every reference to one of this repo's own source files:
the path exists, and it carries no `:123` line number. Line numbers were
allowed once and eight of twenty-eight had drifted onto the wrong line --
including three naming a file or a line that no longer existed. A reference
that reads as precise and is wrong is worse than none. Cross-repo
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

# A citation naming one of this repo's own source files, with an optional
# `:123` or `:12-20` suffix. Twenty-eight of these existed and eight pointed
# at the wrong line, because nothing read them either.
#
# The leading lookbehind is load-bearing: without it `daemon/src/pty.rs` --
# teleport's file, named all over docs/09-windows.md -- matches as this
# repo's `src/pty.rs` and is reported missing or line-numbered.
SOURCE_CITATION = re.compile(
    r"(?<![\w/-])((?:src|tests|scripts)/[\w./-]+\.(?:rs|py))(:[\d,-]*\d)?"
)


def source_problems(line: str, where: str) -> tuple[int, list[str]]:
    """Source citations must name a file that exists, and never a line number.

    A line number cannot be kept true: it is correct when written and silently
    wrong after the next edit above it, which is worse than no reference at
    all, because it reads as precision. Name the symbol instead -- that
    survives an edit and is what a reader was going to search for anyway.
    """
    checked, found = 0, []
    for match in SOURCE_CITATION.finditer(line):
        # teleport has files at the same paths; those are its business.
        if CROSS_REPO.search(line[: match.start()]):
            continue
        checked += 1
        path, lineno = match.group(1), match.group(2)
        if not (ROOT / path).exists():
            found.append(f"{where} -> {path} (no such file)")
        elif lineno:
            found.append(
                f"{where} -> {path}{lineno} "
                f"(line numbers rot silently; name the symbol instead)"
            )
    return checked, found


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
            where = f"{source.relative_to(ROOT)}:{lineno}"
            source_checked, found = source_problems(line, where)
            checked += source_checked
            problems.extend(found)

            for match in CITATION.finditer(line):
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
