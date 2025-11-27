#!/usr/bin/env python3
"""
Identify commits from filtered_travistorrent.csv that no longer exist in their repositories.

For each unique (gh_project_name, git_trigger_commit) pair the script:
1. Clones/fetches the repository into a local cache directory (bare clone).
2. Fetches branches, tags, and pull request refs (refs/pull/*/{head,merge}).
3. Marks a commit as "missing" if `git cat-file -e <sha>^{commit}` fails after the fetch.
4. Removes the local bare repo after processing each project (use --keep-repos to skip).
5. Streams results directly to disk to keep RAM usage low.

The output is a small CSV with two columns: gh_project_name, git_trigger_commit.
"""

from __future__ import annotations

import argparse
import csv
import logging
import shutil
import subprocess
import sys
import threading
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find commits that cannot be checked out in their repository (likely deleted PR heads)."
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to CSV containing gh_project_name and git_trigger_commit (e.g., 19314170/filtered_travistorrent.csv).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Where to write the CSV of missing commits. Default: <input>.missing-commits.csv",
    )
    parser.add_argument(
        "--repos-dir",
        type=Path,
        default=Path(__file__).with_name("repos"),
        help="Directory to cache bare clones (default: ./repos next to this script).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=250_000,
        help="Rows per chunk when reading the input CSV.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional cap on number of input rows to scan (useful for quick tests).",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Number of repositories to fetch in parallel.",
    )
    parser.add_argument(
        "--fetch-depth",
        type=int,
        help="Optional --depth for git fetch; omit for full history.",
    )
    parser.add_argument(
        "--only-project",
        type=str,
        help="If set, process only this gh_project_name (owner/repo).",
    )
    parser.add_argument(
        "--keep-repos",
        action="store_true",
        help="Keep fetched bare repos on disk. Default behavior cleans them after each project.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from existing output/checkpoint files to avoid re-processing repos.",
    )
    parser.add_argument(
        "--checkpoint",
        type=Path,
        help="Optional path to a checkpoint file (tracks processed repos). Default: <output>.checkpoint",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable INFO-level logging.",
    )
    return parser.parse_args()


def run_git(args: Sequence[str], cwd: Path) -> subprocess.CompletedProcess:
    """Run a git command and return the completed process."""
    cmd = ["git", "-C", str(cwd), *args]
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def ensure_repo(slug: str, repos_dir: Path) -> Optional[Path]:
    """
    Prepare a bare clone for the given repo slug (owner/repo).

    Returns the repo path or None if initialization failed.
    """
    if "/" not in slug:
        logger.warning("Invalid gh_project_name (missing /): %s", slug)
        return None

    owner, repo = slug.split("/", 1)
    repo_dir = repos_dir / owner / repo
    repo_url = f"https://github.com/{slug}.git"

    if not repo_dir.exists():
        repo_dir.parent.mkdir(parents=True, exist_ok=True)
        init_proc = subprocess.run(
            ["git", "init", "--bare", str(repo_dir)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        if init_proc.returncode != 0:
            logger.warning("Failed to init %s: %s", slug, init_proc.stdout.strip())
            return None

    # Ensure the path is a git repo
    check_proc = run_git(["rev-parse", "--git-dir"], repo_dir)
    if check_proc.returncode != 0:
        logger.warning("Path is not a git repo, skipping %s: %s", slug, check_proc.stdout.strip())
        return None

    # Ensure origin remote exists
    remote_proc = run_git(["remote", "get-url", "origin"], repo_dir)
    if remote_proc.returncode != 0:
        add_proc = run_git(["remote", "add", "origin", repo_url], repo_dir)
        if add_proc.returncode != 0:
            logger.warning("Failed to add origin for %s: %s", slug, add_proc.stdout.strip())
            return None

    return repo_dir


def fetch_repo(slug: str, repo_dir: Path, fetch_depth: Optional[int]) -> bool:
    """Fetch branches, tags, and PR refs into the bare repo."""
    refspecs = [
        "+refs/heads/*:refs/remotes/origin/*",
        "+refs/tags/*:refs/tags/*",
        "+refs/pull/*/head:refs/pull/*/head",
        "+refs/pull/*/merge:refs/pull/*/merge",
    ]

    cmd: List[str] = ["fetch", "--force", "--prune", "--tags"]
    if fetch_depth:
        cmd.append(f"--depth={fetch_depth}")
    cmd.append("origin")
    cmd.extend(refspecs)

    proc = run_git(cmd, repo_dir)
    if proc.returncode != 0:
        logger.warning("Fetch failed for %s: %s", slug, proc.stdout.strip())
        return False
    return True


def commit_exists(repo_dir: Path, sha: str) -> bool:
    """Return True if the commit object exists locally."""
    proc = run_git(["cat-file", "-e", f"{sha}^{{commit}}"], repo_dir)
    return proc.returncode == 0


def cleanup_repo(repo_dir: Path) -> None:
    """Remove the bare repo directory to save space."""
    try:
        shutil.rmtree(repo_dir)
        parent = repo_dir.parent
        if parent.exists() and not any(parent.iterdir()):
            parent.rmdir()
    except Exception:
        logger.debug("Cleanup failed for %s", repo_dir, exc_info=True)


def load_checkpoint(path: Path) -> Set[str]:
    """Load processed repository slugs from a checkpoint file."""
    processed: Set[str] = set()
    if not path.exists():
        return processed
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            slug = line.split(",", 1)[0].strip()
            if slug:
                processed.add(slug)
    except Exception:
        logger.warning("Could not read checkpoint file %s", path)
    return processed


def load_commit_map(
    input_path: Path, chunk_size: int, limit: Optional[int], only_project: Optional[str]
) -> Tuple[Dict[str, Set[str]], int]:
    """
    Read gh_project_name/git_trigger_commit pairs into a dict of repo -> set(commits).
    Returns (commit_map, rows_read).
    """
    commit_map: Dict[str, Set[str]] = defaultdict(set)
    rows_read = 0

    for chunk in pd.read_csv(
        input_path,
        usecols=["gh_project_name", "git_trigger_commit"],
        chunksize=chunk_size,
        dtype=str,
    ):
        for slug, sha in zip(chunk["gh_project_name"], chunk["git_trigger_commit"]):
            if pd.isna(slug) or pd.isna(sha):
                continue
            slug = str(slug).strip()
            sha = str(sha).strip()
            if not slug or not sha:
                continue
            if only_project and slug != only_project:
                continue
            commit_map[slug].add(sha)
            rows_read += 1
            if limit and rows_read >= limit:
                logger.info("Reached limit=%s rows; stopping input read.", limit)
                return commit_map, rows_read
    return commit_map, rows_read


def process_repo(slug: str, commits: Set[str], args: argparse.Namespace) -> Tuple[str, List[str], str]:
    """
    Ensure the repo is fetched and return commits that are missing locally.
    If fetching fails, the repo is skipped to avoid false positives. Status indicates success/failure reason.
    """
    repo_dir = ensure_repo(slug, args.repos_dir)
    if repo_dir is None:
        return slug, [], "init_failed"

    missing: List[str] = []
    status = "ok"
    try:
        fetched = fetch_repo(slug, repo_dir, args.fetch_depth)
        if not fetched:
            status = "fetch_failed"
        else:
            missing = [sha for sha in commits if not commit_exists(repo_dir, sha)]
    finally:
        if not args.keep_repos:
            cleanup_repo(repo_dir)

    return slug, missing, status


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if not args.input.exists():
        raise SystemExit(f"Input file not found: {args.input}")

    output_path = args.output or args.input.with_name(f"{args.input.name}.missing-commits.csv")
    checkpoint_path = args.checkpoint or output_path.with_suffix(f"{output_path.suffix}.checkpoint")

    processed_repos = load_checkpoint(checkpoint_path) if args.resume else set()

    logger.info("Reading %s", args.input)
    commit_map, rows_read = load_commit_map(args.input, args.chunk_size, args.limit, args.only_project)
    if processed_repos:
        logger.info("Checkpoint loaded: %s repos already processed.", len(processed_repos))
    logger.info("Collected %s unique repositories from %s rows", len(commit_map), rows_read)

    if not commit_map:
        logger.warning("No repositories to process. Check filters or input columns.")
        return

    if processed_repos:
        # Filter out repos already processed
        commit_map = {slug: commits for slug, commits in commit_map.items() if slug not in processed_repos}

    if not commit_map:
        logger.info("All repositories were already processed; nothing to do.")
        return

    total_repos = len(commit_map)
    processed = len(processed_repos)
    missing_count = 0
    failed_repos: List[Tuple[str, str]] = []  # (slug, reason)

    # Prepare output writer (streaming to avoid large memory use)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    output_exists = output_path.exists()
    mode = "a" if (args.resume and output_exists) else "w"
    # If resuming and appending, compute current missing_count from file (lines - header)
    if args.resume and output_exists:
        try:
            with output_path.open("r", encoding="utf-8") as f:
                missing_count = max(0, sum(1 for _ in f) - 1)
            logger.info("Resuming with %s missing commits already recorded.", missing_count)
        except Exception:
            logger.warning("Could not count existing output rows; continuing without initial count.")

    cp_mode = "a" if (args.resume and checkpoint_path.exists()) else "w"

    output_lock = threading.Lock()
    checkpoint_lock = threading.Lock()

    with output_path.open(mode, newline="", encoding="utf-8") as handle, checkpoint_path.open(
        cp_mode, encoding="utf-8"
    ) as cp_handle:
        writer = csv.writer(handle)
        if mode == "w":
            writer.writerow(["gh_project_name", "git_trigger_commit"])

        # Process repos in a bounded thread pool; tune max_workers to avoid overwhelming GitHub.
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as executor:
            future_to_slug = {
                executor.submit(process_repo, slug, commits, args): slug
                for slug, commits in commit_map.items()
            }
            for future in as_completed(future_to_slug):
                slug = future_to_slug[future]
                try:
                    repo_slug, missing, status = future.result()
                except Exception as exc:  # Defensive: keep going on a single repo failure
                    logger.warning("Error while processing %s: %s", slug, exc)
                    missing = []
                    repo_slug = slug
                    status = "exception"

                processed += 1

                if status != "ok":
                    failed_repos.append((repo_slug, status))
                    logger.warning("Skipping %s due to %s", repo_slug, status)

                if missing:
                    with output_lock:
                        for sha in sorted(missing):
                            writer.writerow([repo_slug, sha])
                            missing_count += 1
                        handle.flush()

                with checkpoint_lock:
                    cp_handle.write(f"{repo_slug},{status}\n")
                    cp_handle.flush()

                if processed % 10 == 0 or processed == total_repos:
                    logger.info(
                        "Processed %s/%s repos (missing commits so far: %s)",
                        processed,
                        total_repos,
                        missing_count,
                    )

    if failed_repos:
        logger.warning("Failed to clone/fetch %s repos: %s", len(failed_repos), failed_repos)
    logger.info("Finished. Missing commits written: %s -> %s", missing_count, output_path)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
        sys.exit(1)
