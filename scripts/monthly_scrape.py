#!/usr/bin/env python3
"""Monthly scraper to add missing company career links to readme.md."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
README_PATH = ROOT / "readme.md"
SEEDS_PATH = ROOT / "data" / "monthly_seed_companies.json"
EXTERNAL_PORTUGAL_LIST = (
    "https://raw.githubusercontent.com/marmelo/tech-companies-in-portugal/master/README.md"
)

UA = "Mozilla/5.0 (compatible; monthly-company-scraper/1.0)"

SECTION_MAP = {
    "Automotive": "Automotive",
    "Consultancy": "Consultancy",
    "Developer Tools": "Developer Tools",
    "E-commerce": "E-Commerce",
    "Education": "Education",
    "Enterprise Software": "Enterprise Software",
    "FinTech": "FinTech",
    "Gaming": "Gaming",
    "Healthcare": "Healthcare",
    "Industry": "Industry",
    "Mobility": "Mobility",
    "Multimedia": "Multimedia",
    "Sea": "Sea",
    "Security": "Security",
    "Social": "Social",
    "Sport": "Sport",
    "Telcos": "Telcos",
    "Travel": "Travel",
}


@dataclass(frozen=True)
class Candidate:
    section: str
    name: str
    url: str
    description: str
    source: str

    def bullet(self) -> str:
        desc = self.description.strip()
        if not desc.endswith("."):
            desc += "."
        return f"- [{self.name}]({self.url}) - {desc}"


def fetch_text(url: str, timeout: int = 30) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def http_status(url: str, timeout: int = 8) -> int:
    req = urllib.request.Request(url, headers={"User-Agent": UA}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return int(getattr(resp, "status", 0) or 0)
    except urllib.error.HTTPError as exc:
        return int(exc.code or 0)
    except Exception:
        return 0


def load_seed_candidates() -> list[Candidate]:
    data = json.loads(SEEDS_PATH.read_text(encoding="utf-8"))
    out: list[Candidate] = []
    for item in data:
        out.append(
            Candidate(
                section=item["section"].strip(),
                name=item["name"].strip(),
                url=item["url"].strip(),
                description=item["description"].strip(),
                source="seed",
            )
        )
    return out


def scrape_portugal_candidates() -> list[Candidate]:
    text = fetch_text(EXTERNAL_PORTUGAL_LIST)
    lines = text.splitlines()

    current = ""
    out: list[Candidate] = []

    header_re = re.compile(r"^##\s+([A-Za-z\- ]+)")
    row_re = re.compile(
        r"^\|\s+\[([^\]]+)\]\([^)]+\)\s+\[:rocket:\]\((https?://[^)]+)\)\s+\|\s+([^|]+?)\s+\|"
    )

    for line in lines:
        hm = header_re.match(line)
        if hm:
            current = hm.group(1).strip()
            if "Consultancy" in current:
                current = "Consultancy"
            continue

        rm = row_re.match(line)
        if not rm:
            continue

        section = SECTION_MAP.get(current)
        if not section:
            continue

        name, url, desc = rm.group(1).strip(), rm.group(2).strip(), rm.group(3).strip()
        if "linkedin.com" in url.lower():
            continue
        out.append(
            Candidate(
                section=section,
                name=name,
                url=url,
                description=desc,
                source="tech-companies-in-portugal",
            )
        )
    return out


def parse_existing_names(lines: Iterable[str]) -> set[str]:
    names: set[str] = set()
    bullet_re = re.compile(r"^- \[([^\]]+)\]\(")
    for line in lines:
        m = bullet_re.match(line)
        if m:
            names.add(m.group(1).strip().casefold())
    return names


def find_section_bounds(lines: list[str], section: str) -> tuple[int, int] | None:
    head = f"### {section}"
    start = -1
    for i, line in enumerate(lines):
        if line.strip() == head:
            start = i
            break
    if start < 0:
        return None
    end = len(lines)
    for j in range(start + 1, len(lines)):
        if lines[j].startswith("### ") or lines[j].startswith("## "):
            end = j
            break
    return (start, end)


def insert_candidates(readme_text: str, candidates: list[Candidate]) -> tuple[str, list[Candidate]]:
    lines = readme_text.splitlines()
    existing = parse_existing_names(lines)
    accepted: list[Candidate] = []

    per_section: dict[str, list[Candidate]] = {}
    for c in candidates:
        if c.name.casefold() in existing:
            continue
        per_section.setdefault(c.section, []).append(c)
        existing.add(c.name.casefold())

    for section, section_candidates in per_section.items():
        bounds = find_section_bounds(lines, section)
        if not bounds:
            continue
        _start, end = bounds
        insert_at = end
        while insert_at > 0 and lines[insert_at - 1].strip() == "":
            insert_at -= 1

        new_lines = [c.bullet() for c in sorted(section_candidates, key=lambda x: x.name.casefold())]
        lines[insert_at:insert_at] = new_lines + [""]
        accepted.extend(section_candidates)

    return ("\n".join(lines) + "\n", accepted)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Write changes to readme.md")
    parser.add_argument("--report", default="monthly-scrape-report.md", help="Output markdown report")
    parser.add_argument("--max-added", type=int, default=80, help="Limit number of additions")
    args = parser.parse_args()

    readme = README_PATH.read_text(encoding="utf-8")

    seeds = load_seed_candidates()
    scraped = scrape_portugal_candidates()
    all_candidates = seeds + scraped

    existing_names = parse_existing_names(readme.splitlines())

    unique: dict[tuple[str, str], Candidate] = {}
    for c in all_candidates:
        key = (c.section.casefold(), c.name.casefold())
        if key not in unique:
            unique[key] = c

    to_validate = [
        c for c in unique.values() if c.name.casefold() not in existing_names
    ]

    vetted: list[Candidate] = []
    skipped: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        status_map = {
            pool.submit(http_status, c.url): c for c in to_validate
        }
        for fut in concurrent.futures.as_completed(status_map):
            c = status_map[fut]
            status = fut.result()
            if 200 <= status < 400:
                vetted.append(c)
            else:
                skipped.append(f"- `{c.name}` ({c.url}) skipped with HTTP {status}")

    updated, added = insert_candidates(readme, vetted[: args.max_added])

    if args.apply and updated != readme:
        README_PATH.write_text(updated, encoding="utf-8")

    report_lines = [
        "# Monthly Company Scrape Report",
        "",
        f"- Seed candidates: {len(seeds)}",
        f"- Scraped candidates: {len(scraped)}",
        f"- Vetted candidates (HTTP 2xx/3xx): {len(vetted)}",
        f"- Added to readme: {len(added)}",
        "",
        "## Added Companies",
    ]
    if added:
        for c in sorted(added, key=lambda x: (x.section, x.name.casefold())):
            report_lines.append(f"- [{c.name}]({c.url}) -> `{c.section}` (source: `{c.source}`)")
    else:
        report_lines.append("- No new companies were added.")

    report_lines.append("")
    report_lines.append("## Skipped Candidates")
    if skipped:
        report_lines.extend(skipped[:200])
    else:
        report_lines.append("- None")

    report_path = ROOT / args.report
    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    print(f"Added companies: {len(added)}")
    print(f"Report: {report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
