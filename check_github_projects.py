#!/usr/bin/env python3
"""
Check whether repositories listed in a CSV column still exist on GitHub.

Reads unique values from the `gh_project_name` column of the input CSV and
uses `github_api_client.GitHubAPIClient` to query the GitHub API. Results are
written to a small CSV with columns: gh_project_name, status, error.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd
import yaml

from github_api_client import GitHubAPIClient

logger = logging.getLogger(__name__)


def _tokens_from_yaml(path: Path) -> List[str]:
    """Load GitHub tokens from a YAML file (supports top-level list or github.tokens)."""
    if not path.exists():
        return []
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        logger.warning("Failed to read token config %s: %s", path, exc)
        return []

    if isinstance(raw, dict):
        tokens_raw = raw.get("github", {}).get("tokens") or raw.get("tokens")
    else:
        tokens_raw = raw

    tokens: List[str] = []
    if isinstance(tokens_raw, str):
        tokens.extend(token.strip() for token in tokens_raw.split(",") if token.strip())
    elif isinstance(tokens_raw, Sequence):
        tokens.extend(str(token).strip() for token in tokens_raw if str(token).strip())
    return [t for t in dict.fromkeys(tokens) if t]


def resolve_tokens(cli_tokens: Optional[Sequence[str]], config_path: Optional[Path]) -> List[str]:
    """
    Collect tokens from CLI, YAML config, and environment variables.

    Priority: CLI tokens > YAML file > GITHUB_TOKENS (comma-separated) > GITHUB_TOKEN.
    """
    tokens: List[str] = []
    if cli_tokens:
        tokens.extend([token.strip() for token in cli_tokens if token.strip()])

    if config_path:
        tokens.extend(_tokens_from_yaml(config_path))

    env_tokens = os.getenv("GITHUB_TOKENS")
    if env_tokens:
        tokens.extend([token.strip() for token in env_tokens.split(",") if token.strip()])

    single_token = os.getenv("GITHUB_TOKEN")
    if single_token:
        tokens.append(single_token.strip())

    # Preserve order but drop duplicates
    deduped: List[str] = []
    for token in tokens:
        if token and token not in deduped:
            deduped.append(token)
    return deduped


def load_unique_projects(input_path: Path, chunk_size: int = 50000) -> List[str]:
    """Read CSV in chunks to collect unique gh_project_name values."""
    uniques: Set[str] = set()
    for chunk in pd.read_csv(input_path, usecols=["gh_project_name"], chunksize=chunk_size):
        for value in chunk["gh_project_name"]:
            if pd.isna(value):
                continue
            slug = str(value).strip()
            if slug:
                uniques.add(slug)
    return sorted(uniques)


def check_repository(slug: str, tokens: Sequence[str]) -> Tuple[str, str]:
    """
    Check if a repository exists on GitHub.

    Returns:
        status: "exists", "missing", or "error"
        error: error message (empty if none)
    """
    if "/" not in slug:
        return "invalid", "gh_project_name is not in owner/repo form"

    owner, repo = slug.split("/", 1)
    client = GitHubAPIClient(owner, repo, tokens=tokens)
    url = f"https://api.github.com/repos/{owner}/{repo}"

    try:
        client.request(url)
        return "exists", ""
    except RuntimeError as exc:
        message = str(exc)
        if "404" in message:
            return "missing", ""
        return "error", message
    except Exception as exc:  # Catch-all for network or parsing issues
        return "error", str(exc)


def write_results(output_path: Path, rows: Iterable[dict]) -> None:
    """Write repository existence results to CSV."""
    fieldnames = ["gh_project_name", "status", "error"]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check if repositories in gh_project_name still exist on GitHub."
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Path to CSV file containing gh_project_name column.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Where to write the report CSV. Defaults to <input>.repo-status.csv",
    )
    parser.add_argument(
        "--tokens",
        nargs="*",
        default=None,
        help="Optional GitHub tokens (space separated). Falls back to GITHUB_TOKENS/GITHUB_TOKEN.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).with_name("github_tokens.yml"),
        help="YAML file containing GitHub tokens (default: github_tokens.yml next to this script).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=50000,
        help="CSV chunk size when reading unique gh_project_name values.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional cap on number of unique projects to check (for quick tests).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable INFO-level logging.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    input_path = args.input
    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    output_path = args.output or input_path.with_name(f"{input_path.name}.repo-status.csv")
    tokens = resolve_tokens(args.tokens, args.config)

    if tokens:
        logger.info("Using %s GitHub tokens (rotated automatically).", len(tokens))
    else:
        logger.warning("No GitHub tokens provided; unauthenticated requests have low rate limits.")

    logger.info("Collecting unique gh_project_name entries from %s", input_path)
    slugs = load_unique_projects(input_path, chunk_size=max(args.chunk_size, 1))
    if args.limit is not None:
        slugs = slugs[: args.limit]

    total = len(slugs)
    results = []
    for index, slug in enumerate(slugs, start=1):
        status, error = check_repository(slug, tokens)
        if index % 25 == 0 or args.verbose:
            logger.info("Checked %s/%s: %s -> %s", index, total, slug, status)

        results.append(
            {
                "gh_project_name": slug,
                "status": status,
                "error": error,
            }
        )

    write_results(output_path, results)
    logger.info("Wrote %s rows to %s", len(results), output_path)


if __name__ == "__main__":
    main()
