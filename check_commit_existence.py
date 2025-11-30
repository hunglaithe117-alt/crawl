#!/usr/bin/env python3
"""
Check whether commits from a CSV still exist on GitHub and filter rows accordingly.

The script streams the input CSV to keep RAM usage low, checks unique
(gh_project_name, git_trigger_commit) pairs via the GitHub REST API in a
thread pool, and writes:
  1) A CSV containing only rows whose commit is confirmed to exist.
  2) A log of commits that could not be found (with a reason/status).

By default, GitHub tokens are loaded from tokens.yml next to this script
and from the GITHUB_TOKEN environment variable. TokenManager handles
rate limits across worker threads.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import threading
from collections import OrderedDict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, TypeVar

import pandas as pd
import pyarrow as pa
import duckdb
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed

from github_api_client import GitHubAPIClient
from token_pool import TokenManager

logger = logging.getLogger(__name__)


# ------------------------------ Config helpers ------------------------------ #
def load_yaml(path: Path) -> Dict:
    """Load a YAML file, returning an empty dict on failure."""
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle) or {}
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Could not read %s: %s", path, exc)
        return {}


def load_tokens(tokens_path: Path) -> List[str]:
    """Load GitHub tokens from a YAML file and env var."""
    tokens: List[str] = []
    env_token = os.environ.get("GITHUB_TOKEN")
    if env_token:
        tokens.append(env_token.strip())

    config = load_yaml(tokens_path)
    file_tokens = config.get("github_tokens") or []
    tokens.extend([str(t).strip() for t in file_tokens if str(t).strip()])

    # Deduplicate while preserving order
    seen = set()
    unique_tokens = []
    for token in tokens:
        if token and token not in seen:
            seen.add(token)
            unique_tokens.append(token)
    return unique_tokens


# ------------------------------ GitHub helpers ------------------------------ #
class ResultCache:
    """Bounded LRU cache for commit existence checks."""

    def __init__(self, max_size: int) -> None:
        self._data: "OrderedDict[Tuple[str, str], Tuple[bool, str]]" = OrderedDict()
        self._lock = threading.Lock()
        self._max_size = max_size

    def get(self, key: Tuple[str, str]) -> Optional[Tuple[bool, str]]:
        with self._lock:
            value = self._data.get(key)
            if value is not None:
                self._data.move_to_end(key)
            return value

    def set(self, key: Tuple[str, str], value: Tuple[bool, str]) -> None:
        with self._lock:
            self._data[key] = value
            self._data.move_to_end(key)
            if len(self._data) > self._max_size:
                self._data.popitem(last=False)

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)


class RepoCache:
    """Bounded LRU cache for repository existence checks."""

    def __init__(self, max_size: int = 10_000) -> None:
        self._data: OrderedDict[str, Tuple[bool, str]] = OrderedDict()
        self._lock = threading.Lock()
        self._max_size = max_size

    def get(self, slug: str) -> Optional[Tuple[bool, str]]:
        with self._lock:
            value = self._data.get(slug)
            if value is not None:
                self._data.move_to_end(slug)
            return value

    def set(self, slug: str, value: Tuple[bool, str]) -> None:
        with self._lock:
            self._data[slug] = value
            self._data.move_to_end(slug)
            if len(self._data) > self._max_size:
                self._data.popitem(last=False)

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)


class ClientPool:
    """
    Lightweight per-thread client cache so we don't recreate GitHubAPIClient
    instances for the same repo repeatedly.
    """

    def __init__(
        self,
        token_manager: TokenManager,
        retry_count: int,
        retry_delay: float,
        cache_dir: Optional[Path],
        max_clients_per_thread: int,
    ) -> None:
        self._token_manager = token_manager
        self._retry_count = retry_count
        self._retry_delay = retry_delay
        self._cache_dir = str(cache_dir) if cache_dir else None
        self._max_clients_per_thread = max_clients_per_thread
        self._local = threading.local()

    def _get_local_cache(self) -> OrderedDict:
        cache = getattr(self._local, "clients", None)
        if cache is None:
            cache = OrderedDict()
            self._local.clients = cache
        return cache

    def get_client(self, slug: str) -> GitHubAPIClient:
        cache = self._get_local_cache()
        client = cache.get(slug)
        if client:
            cache.move_to_end(slug)
            return client

        if "/" not in slug:
            raise ValueError(f"Invalid repo slug: {slug}")
        owner, repo = slug.split("/", 1)
        client = GitHubAPIClient(
            owner=owner,
            repo=repo,
            token_manager=self._token_manager,
            retry_count=self._retry_count,
            retry_delay=self._retry_delay,
            cache_dir=self._cache_dir,
        )

        cache[slug] = client
        cache.move_to_end(slug)
        if len(cache) > self._max_clients_per_thread:
            cache.popitem(last=False)
        return client


def check_commit_exists(
    slug: str,
    sha: str,
    client_pool: ClientPool,
    timeout: float,
) -> Tuple[bool, str]:
    """
    Return (exists, detail) for a commit in a repository.
    detail contains a short status string for logging.
    """
    if not slug or not sha:
        return False, "missing_data"
    if "/" not in slug:
        return False, "invalid_repo"

    try:
        client = client_pool.get_client(slug)
    except Exception as exc:  # pragma: no cover - defensive
        return False, f"client_error:{exc}"

    owner, repo = slug.split("/", 1)
    url = f"https://api.github.com/repos/{owner}/{repo}/commits/{sha}"

    try:
        resp = client.request(url, timeout=timeout)
    except Exception as exc:
        logger.warning("Request failed for %s@%s: %s", slug, sha, exc)
        return False, f"request_error:{exc}"

    status = resp.status_code
    if status == 200:
        return True, "ok"
    if status in (404, 410, 422):
        return False, "not_found"
    if status == 409:
        return False, "repo_conflict"
    if status == 451:
        return False, "unavailable"

    detail = f"status_{status}"
    try:
        msg = resp.json().get("message")
        if msg:
            detail = f"{detail}:{msg}"
    except Exception:
        pass
    return False, detail


def check_repo_exists(
    slug: str,
    client_pool: ClientPool,
    timeout: float,
) -> Tuple[bool, str]:
    """Return (exists, status) for a repository to avoid useless commit calls."""
    if not slug:
        return False, "missing_repo"
    if "/" not in slug:
        return False, "invalid_repo"

    owner, repo = slug.split("/", 1)
    url = f"https://api.github.com/repos/{owner}/{repo}"

    try:
        client = client_pool.get_client(slug)
    except Exception as exc:  # pragma: no cover - defensive
        return False, f"client_error:{exc}"

    try:
        resp = client.request(url, timeout=timeout)
    except Exception as exc:
        logger.warning("Repo check failed for %s: %s", slug, exc)
        return False, f"request_error:{exc}"

    status = resp.status_code
    if status == 200:
        return True, "repo_ok"
    if status in (404, 410):
        return False, "repo_not_found"
    if status == 451:
        return False, "repo_unavailable"

    detail = f"repo_status_{status}"
    try:
        msg = resp.json().get("message")
        if msg:
            detail = f"{detail}:{msg}"
    except Exception:
        pass
    return False, detail


# ------------------------------ Main pipeline ------------------------------ #
def parse_args() -> argparse.Namespace:
    base_dir = Path(__file__).resolve().parent
    default_tokens = base_dir / "tokens.yml"
    default_config = base_dir / "crawler_config.yml"

    # Prefer the known dataset location if it exists; otherwise require --input.
    default_input = base_dir.parent / "19314170" / "final-2017-01-25.csv"
    input_required = not default_input.exists()

    parser = argparse.ArgumentParser(
        description="Filter a CSV by checking whether git_trigger_commit exists on GitHub.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=input_required,
        default=default_input if default_input.exists() else None,
        help="Path to input CSV (must contain gh_project_name and git_trigger_commit).",
    )
    parser.add_argument(
        "--existing-output",
        type=Path,
        help="Where to write rows whose commit exists. Defaults to <input>.existing-commits.csv",
    )
    parser.add_argument(
        "--missing-output",
        type=Path,
        help="Where to log commits that could not be found. Defaults to <input>.missing-commits.csv",
    )
    parser.add_argument(
        "--tokens",
        type=Path,
        default=default_tokens,
        help="YAML file with github_tokens.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=default_config,
        help="Optional crawler_config.yml to read defaults (max_worker, retry settings).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=5000,
        help="Number of rows to process per chunk.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        help="Thread pool size for GitHub API calls.",
    )
    parser.add_argument(
        "--cache-size",
        type=int,
        default=100_000,
        help="Max commit results to retain in-memory (LRU).",
    )
    parser.add_argument(
        "--client-cache",
        type=int,
        default=256,
        help="Max GitHubAPIClient objects to keep per worker thread.",
    )
    parser.add_argument(
        "--api-timeout",
        type=float,
        default=20.0,
        help="Timeout (seconds) per GitHub API request.",
    )
    parser.add_argument(
        "--retry-count",
        type=int,
        help="Override github_api_retry_count.",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        help="Override github_api_retry_delay.",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        help="Optional cache directory for GitHub ETag responses.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Process at most this many rows (useful for quick tests).",
    )
    parser.add_argument(
        "--checkpoint",
        type=Path,
        help="Path to checkpoint file storing processed row offset.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint; appends to outputs instead of overwriting.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    return parser.parse_args()


def resolve_settings(args: argparse.Namespace) -> argparse.Namespace:
    """Fill unspecified args from config defaults."""
    cfg = load_yaml(args.config)
    args.max_workers = args.max_workers or cfg.get("max_worker") or 8
    args.retry_count = args.retry_count or cfg.get("github_api_retry_count") or 5
    args.retry_delay = args.retry_delay or cfg.get("github_api_retry_delay") or 1.0
    if args.cache_dir is None:
        cache_dir_cfg = cfg.get("github_api_cache_dir")
        args.cache_dir = Path(cache_dir_cfg) if cache_dir_cfg else None
    return args


def ensure_outputs(
    input_path: Path,
    existing_out: Optional[Path],
    missing_out: Optional[Path],
) -> Tuple[Path, Path]:
    existing_path = (
        existing_out
        if existing_out
        else input_path.with_name(f"{input_path.stem}.existing-commits.csv")
    )
    missing_path = (
        missing_out
        if missing_out
        else input_path.with_name(f"{input_path.stem}.missing-commits.csv")
    )
    existing_path.parent.mkdir(parents=True, exist_ok=True)
    missing_path.parent.mkdir(parents=True, exist_ok=True)
    return existing_path, missing_path


T = TypeVar("T")


def iter_unique(items: Iterable[T]) -> List[T]:
    """Return unique items preserving order."""
    seen = set()
    unique: List[T] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        unique.append(item)
    return unique


def load_checkpoint(path: Path) -> int:
    """Read processed row offset from checkpoint file."""
    if not path.exists():
        return 0
    try:
        return int(path.read_text().strip() or 0)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Could not read checkpoint %s: %s", path, exc)
        return 0


def write_checkpoint(path: Path, processed_rows: int) -> None:
    """Persist processed row offset."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(int(processed_rows)))
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to write checkpoint %s: %s", path, exc)


def main() -> None:
    args = parse_args()
    args = resolve_settings(args)

    log_level = getattr(logging, str(args.log_level).upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    logging.getLogger().setLevel(log_level)
    logger.info("Logging level set to %s", logging.getLevelName(log_level))

    if not args.input:
        raise SystemExit("Please provide --input path to the CSV.")
    if not args.input.exists():
        raise SystemExit(f"Input file not found: {args.input}")

    tokens = load_tokens(args.tokens)
    if not tokens:
        raise SystemExit(
            f"No GitHub tokens found. Add github_tokens to {args.tokens} or set GITHUB_TOKEN."
        )

    token_manager = TokenManager(tokens)
    client_pool = ClientPool(
        token_manager=token_manager,
        retry_count=int(args.retry_count),
        retry_delay=float(args.retry_delay),
        cache_dir=args.cache_dir,
        max_clients_per_thread=int(args.client_cache),
    )
    cache = ResultCache(max_size=int(args.cache_size))
    repo_cache = RepoCache()

    existing_path, missing_path = ensure_outputs(
        args.input, args.existing_output, args.missing_output
    )
    checkpoint_path = args.checkpoint or existing_path.with_suffix(
        f"{existing_path.suffix}.checkpoint"
    )

    start_row = load_checkpoint(checkpoint_path) if args.resume else 0
    if start_row:
        logger.info("Resuming from checkpoint at row %s", start_row)

    existing_mode = "a" if (args.resume and existing_path.exists()) else "w"
    missing_mode = "a" if (args.resume and missing_path.exists()) else "w"

    header_written = (
        existing_mode == "a"
        and existing_path.exists()
        and existing_path.stat().st_size > 0
    )
    missing_header_written = (
        missing_mode == "a"
        and missing_path.exists()
        and missing_path.stat().st_size > 0
    )

    logger.info("Writing existing rows to %s", existing_path)
    logger.info("Logging missing commits to %s", missing_path)

    total_rows = start_row
    existing_rows = 0
    missing_pairs_logged: set[Tuple[str, str]] = set()

    with existing_path.open(existing_mode, newline="", encoding="utf-8") as existing_handle, missing_path.open(
        missing_mode, newline="", encoding="utf-8"
    ) as missing_handle:
        missing_writer = csv.writer(missing_handle)
        if not missing_header_written:
            missing_writer.writerow(["gh_project_name", "git_trigger_commit", "status"])

        con = duckdb.connect(database=":memory:")
        offset = start_row

        with ThreadPoolExecutor(max_workers=int(args.max_workers)) as executor:
            while True:
                if args.limit and total_rows >= args.limit:
                    break

                # Respect remaining rows when limit is set.
                batch_size = int(args.chunk_size)
                if args.limit:
                    remaining = args.limit - total_rows
                    if remaining <= 0:
                        break
                    batch_size = min(batch_size, remaining)

                table: pa.Table = con.execute(
                    "SELECT * FROM read_csv_auto(?, ALL_VARCHAR=TRUE) LIMIT ? OFFSET ?",
                    [str(args.input), batch_size, offset],
                ).fetch_arrow_table()

                if table.num_rows == 0:
                    break

                chunk = table.to_pandas(strings_to_categorical=False)
                offset += len(chunk)

                slugs = chunk["gh_project_name"].astype(str).str.strip()
                shas = chunk["git_trigger_commit"].astype(str).str.strip()
                pairs = list(zip(slugs, shas))

                unique_slugs = iter_unique(slugs)
                repo_results: Dict[str, Tuple[bool, str]] = {}
                pending_repos: List[str] = []
                for slug in unique_slugs:
                    cached_repo = repo_cache.get(slug)
                    if cached_repo is not None:
                        repo_results[slug] = cached_repo
                    else:
                        pending_repos.append(slug)

                if pending_repos:
                    repo_futures = {
                        executor.submit(
                            check_repo_exists, slug, client_pool, args.api_timeout
                        ): slug
                        for slug in pending_repos
                    }
                    for future in as_completed(repo_futures):
                        slug = repo_futures[future]
                        try:
                            repo_status = future.result()
                        except Exception as exc:  # pragma: no cover - defensive
                            logger.warning("Repo worker failed for %s: %s", slug, exc)
                            repo_status = (False, f"repo_worker_error:{exc}")
                        repo_results[slug] = repo_status
                        repo_cache.set(slug, repo_status)

                unique_pairs = iter_unique(pairs)
                results: Dict[Tuple[str, str], Tuple[bool, str]] = {}
                pending: List[Tuple[str, str]] = []
                for pair in unique_pairs:
                    slug, _ = pair
                    repo_exists, repo_status = repo_results.get(
                        slug, (False, "repo_not_checked")
                    )
                    if not repo_exists:
                        results[pair] = (False, repo_status)
                        continue

                    cached = cache.get(pair)
                    if cached is not None:
                        results[pair] = cached
                    else:
                        pending.append(pair)

                if pending:
                    future_map = {
                        executor.submit(
                            check_commit_exists, slug, sha, client_pool, args.api_timeout
                        ): (slug, sha)
                        for slug, sha in pending
                    }
                    for future in as_completed(future_map):
                        pair = future_map[future]
                        try:
                            result = future.result()
                        except Exception as exc:  # pragma: no cover - defensive
                            logger.warning(
                                "Worker failed for %s@%s: %s", pair[0], pair[1], exc
                            )
                            result = (False, f"worker_error:{exc}")
                        results[pair] = result
                        cache.set(pair, result)

                mask: List[bool] = []
                for slug, sha in pairs:
                    exists, status = results[(slug, sha)]
                    mask.append(bool(exists))
                    if not exists and (slug, sha) not in missing_pairs_logged:
                        missing_pairs_logged.add((slug, sha))
                        missing_writer.writerow([slug, sha, status])

                if not header_written:
                    # Write header once to keep the output schema even if no rows pass.
                    chunk.head(0).to_csv(existing_handle, index=False, header=True)
                    header_written = True

                existing_chunk = chunk[mask]
                if not existing_chunk.empty:
                    existing_chunk.to_csv(
                        existing_handle, index=False, header=False
                    )

                total_rows += len(chunk)
                existing_rows += len(existing_chunk)
                write_checkpoint(checkpoint_path, total_rows)

                if total_rows % (args.chunk_size * 5) == 0 or (
                    args.limit and total_rows >= args.limit
                ):
                    logger.info(
                        "Processed %s rows (kept %s). Cache size=%s",
                        total_rows,
                        existing_rows,
                        len(cache),
                    )

    logger.info(
        "Done. Processed %s rows. Existing rows: %s. Missing unique pairs: %s",
        total_rows,
        existing_rows,
        len(missing_pairs_logged),
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
