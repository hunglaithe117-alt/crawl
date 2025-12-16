#!/usr/bin/env python3
"""Scan GitHub repositories for build logs (GitHub Actions / Travis CI)."""

from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from config import ScannerConfig, load_config
from store import ScanStore

from token_pool import Token, TokenManager, MongoTokenManager
from github_api_client import GitHubAPIClient

LOGGER = logging.getLogger(__name__)
LOG_REQUESTS = False


def _init_token_manager(
    tokens: Optional[List[str]],
    cfg: ScannerConfig,
    *,
    collection_name: str,
    token_type: str,
    allow_empty: bool = False,
) -> Optional[TokenManager]:
    """
    Initialize a TokenManager or MongoTokenManager based on configuration.

    If Mongo is available, tokens are seeded into the specified collection.
    When allow_empty is True and no tokens are provided and Mongo init fails,
    returns None instead of raising.
    """
    cleaned_tokens = [str(t).strip() for t in tokens or [] if str(t).strip()]

    try:
        manager = MongoTokenManager(cfg.mongo_uri, cfg.db_name, collection_name)
        for token in cleaned_tokens:
            manager.add_token(token_type, token)
        return manager
    except Exception as exc:
        LOGGER.warning(
            "Failed to init MongoTokenManager for %s (%s). Falling back to memory.",
            collection_name,
            exc,
        )

    if not cleaned_tokens:
        if allow_empty:
            return None
        raise RuntimeError(
            f"No tokens configured for {token_type} and MongoDB is unavailable."
        )

    return TokenManager(cleaned_tokens)


def _is_human_actor(actor: Optional[Dict[str, Any]]) -> bool:
    """Return True if the actor appears to be a human user."""
    if not actor or not isinstance(actor, dict):
        return False
    login = str(actor.get("login") or "").lower()
    actor_type = (actor.get("type") or "").lower()
    if actor_type == "user":
        return not (
            login.endswith("[bot]") or login.endswith("-bot") or login.endswith("bot")
        )
    if actor_type == "bot":
        return False
    # Fallback heuristic on login
    return not (
        login.endswith("[bot]") or login.endswith("-bot") or login.endswith("bot")
    )


def _github_headers(token: Optional[str]) -> Dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "ci-build-log-scanner",
    }
    if token:
        headers["Authorization"] = f"token {token}"
    return headers


def _travis_headers(
    token: Optional[str], accept: str = "application/json"
) -> Dict[str, str]:
    headers = {
        "Travis-API-Version": "3",
        "User-Agent": "ci-build-log-scanner",
        "Accept": accept,
    }
    if token:
        headers["Authorization"] = f"token {token}"
    return headers


def _create_github_client(
    cfg: ScannerConfig,
    token_manager: Optional[TokenManager],
    owner: str = "placeholder",
    repo: str = "placeholder",
) -> GitHubAPIClient:
    """Create a GitHubAPIClient with the shared token manager."""
    return GitHubAPIClient(
        owner=owner,
        repo=repo,
        token_manager=token_manager,
        retry_count=cfg.retry_count,
        retry_delay=cfg.retry_base_delay_seconds,
    )


def travis_request_with_manager(
    url: str,
    token_manager: Optional[TokenManager],
    cfg: ScannerConfig,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    allow_redirects: bool = True,
    stream: bool = False,
) -> Tuple[requests.Response, Optional[str]]:
    """Travis-only GET with token manager and retries."""
    attempts = 0
    while True:
        token_obj: Optional[Token] = None
        if token_manager:
            try:
                token_obj = token_manager.get_best_token()
            except Exception as exc:
                LOGGER.error("Failed to acquire Travis token: %s", exc)
                raise

        hdrs = {
            **_travis_headers(
                token_obj.key if token_obj else None,
                (headers or {}).get("Accept", "application/json"),
            ),
            **(headers or {}),
        }
        try:
            response = requests.get(
                url,
                headers=hdrs,
                params=params,
                allow_redirects=allow_redirects,
                timeout=cfg.request_timeout,
                stream=stream,
            )
            if LOG_REQUESTS:
                LOGGER.info(
                    "HTTP GET %s params=%s -> %s",
                    url,
                    params or {},
                    response.status_code,
                )
        except requests.RequestException as exc:
            attempts += 1
            if attempts > cfg.retry_count:
                raise
            backoff = cfg.retry_base_delay_seconds * (2 ** (attempts - 1))
            LOGGER.warning(
                "Travis request error (%s). Retry %s/%s in %.1fs",
                exc,
                attempts,
                cfg.retry_count,
                backoff,
            )
            time.sleep(backoff)
            continue

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            wait_seconds = cfg.sleep_on_429_seconds
            if retry_after:
                try:
                    wait_seconds = max(float(retry_after), 1.0)
                except ValueError:
                    pass
            if token_manager and token_obj:
                token_manager.handle_rate_limit(token_obj)
            LOGGER.warning(
                "429 from %s; token on cool-down for %.1fs", url, wait_seconds
            )
            time.sleep(wait_seconds)
            continue

        if 500 <= response.status_code < 600:
            attempts += 1
            if attempts > cfg.retry_count:
                return response, token_obj.key if token_obj else None
            backoff = cfg.retry_base_delay_seconds * (2 ** (attempts - 1))
            LOGGER.warning(
                "Travis server error %s from %s. Retry %s/%s in %.1fs",
                response.status_code,
                url,
                attempts,
                cfg.retry_count,
                backoff,
            )
            time.sleep(backoff)
            continue

        if token_manager and token_obj:
            try:
                token_manager.update_token(token_obj, response.headers)
            except Exception:
                pass

        time.sleep(cfg.request_delay_seconds)
        return response, token_obj.key if token_obj else None


def search_repositories(
    cfg: ScannerConfig, token_manager: Optional[TokenManager]
) -> Iterable[Dict[str, Any]]:
    """Yield repositories that match the configured filters, per-language (no OR)."""
    base = f"{cfg.github_api_url.rstrip('/')}/search/repositories"
    seen: set[str] = set()
    languages = cfg.languages or [""]
    segments = cfg.search_segments or [None]

    for seg in segments:
        seg_from = seg.get("from") if seg else None
        seg_to = seg.get("to") if seg else None
        for lang in languages:
            LOGGER.info(
                "Searching repositories for language=%s segment=%s..%s",
                lang or "<any>",
                seg_from or cfg.updated_since or "<none>",
                seg_to or "",
            )
            query_parts: List[str] = []
            if lang:
                query_parts.append(f"language:{lang}")
            if seg_from and seg_to:
                query_parts.append(f"created:{seg_from}..{seg_to}")
            elif cfg.updated_since:
                query_parts.append(f"pushed:>={cfg.updated_since}")
            if cfg.min_stars:
                query_parts.append(f"stars:>={cfg.min_stars}")
            query = " ".join(query_parts).strip()

            for page in range(1, cfg.search_pages + 1):
                params = {
                    "q": query,
                    "sort": cfg.search_sort,
                    "order": cfg.search_order,
                    "per_page": cfg.search_per_page,
                    "page": page,
                }
                client = _create_github_client(cfg, token_manager)
                try:
                    response = client.request(
                        base, params=params, timeout=cfg.request_timeout
                    )
                except Exception as e:
                    LOGGER.warning("Search request failed: %s", e)
                    break
                if response.status_code == 422:
                    LOGGER.warning(
                        "Search failed 422 for language=%s page=%s segment=%s..%s: %s",
                        lang or "<any>",
                        page,
                        seg_from or "",
                        seg_to or "",
                        response.text[:200],
                    )
                    break
                if response.status_code != 200:
                    LOGGER.warning(
                        "Search failed (%s) for language=%s page=%s segment=%s..%s: %s",
                        response.status_code,
                        lang or "<any>",
                        page,
                        seg_from or "",
                        seg_to or "",
                        response.text[:200],
                    )
                    break
                payload = response.json()
                LOGGER.info(
                    "Search page %s for language=%s segment=%s..%s returned %s items",
                    page,
                    lang or "<any>",
                    seg_from or "",
                    seg_to or "",
                    len(payload.get("items", [])),
                )
                for item in payload.get("items", []):
                    slug = item.get("full_name")
                    if slug and slug in seen:
                        continue
                    if slug:
                        seen.add(slug)
                    item["_segment_from"] = seg_from
                    item["_segment_to"] = seg_to
                    yield item


def _fetch_content_exists(
    owner: str,
    repo: str,
    path: str,
    cfg: ScannerConfig,
    token_manager: Optional[TokenManager],
) -> bool:
    url = f"{cfg.github_api_url.rstrip('/')}/repos/{owner}/{repo}/contents/{path.lstrip('/')}"
    client = _create_github_client(cfg, token_manager, owner, repo)
    try:
        response = client.request(
            url, allow_redirects=False, timeout=cfg.request_timeout
        )
    except Exception:
        return False
    return response.status_code == 200


def detect_ci(
    owner: str, repo: str, cfg: ScannerConfig, token_manager: Optional[TokenManager]
) -> List[str]:
    """Detect whether the repo uses GitHub Actions and/or Travis CI."""
    providers: List[str] = []

    runs_url = f"{cfg.github_api_url.rstrip('/')}/repos/{owner}/{repo}/actions/runs"
    client = _create_github_client(cfg, token_manager, owner, repo)
    try:
        resp = client.request(
            runs_url, params={"per_page": 1}, timeout=cfg.request_timeout
        )
    except Exception:
        # If request fails, assume no Actions or error
        resp = None

    if resp and resp.status_code == 200 and resp.json().get("total_count", 0) > 0:
        providers.append("github_actions")

    if _fetch_content_exists(owner, repo, ".travis.yml", cfg, token_manager):
        providers.append("travis_ci")

    LOGGER.info(
        "CI detection for %s/%s: %s", owner, repo, providers if providers else "none"
    )
    return providers


def fetch_github_log_with_rules(
    url: str, cfg: ScannerConfig, token_manager: Optional[TokenManager]
) -> str:
    """
    Fetch a GitHub Actions log respecting 302/404/401/403/5xx/429 rules.

    Returns: ok|gone|auth|error
    """
    accept_header = "application/vnd.github+json"
    client = _create_github_client(cfg, token_manager)
    try:
        resp = client.request(
            url,
            headers={"Accept": accept_header},
            allow_redirects=False,
            timeout=cfg.request_timeout,
        )
    except Exception as e:
        LOGGER.warning("Failed to fetch GitHub log initial: %s", e)
        return "error"
    if resp.status_code == 302:
        location = resp.headers.get("Location")
        if not location:
            return "error"
        try:
            follow = client.request(
                location,
                headers={"Accept": accept_header},
                allow_redirects=True,
                stream=False,
                timeout=cfg.request_timeout,
            )
        except Exception as e:
            LOGGER.warning("Failed to follow GitHub log redirect: %s", e)
            return "error"
        return "ok" if follow.status_code == 200 else "error"
    if resp.status_code == 200:
        return "ok"
    message_text = ""
    try:
        body = resp.json()
        message_text = str(body.get("message") or body)
    except Exception:
        message_text = resp.text
    if "unsupported 'accept' header" in message_text.lower():
        LOGGER.warning(
            "GitHub log fetch rejected Accept header (%s): %s",
            resp.status_code,
            message_text,
        )
        return "auth"
    if "admin rights" in message_text.lower():
        LOGGER.warning(
            "GitHub log fetch requires admin rights (%s): %s",
            resp.status_code,
            message_text,
        )
        return "auth"
    if resp.status_code == 404:
        return "gone"
    if resp.status_code in (401, 403, 415):
        LOGGER.warning(
            "GitHub log fetch failed (%s): %s",
            resp.status_code,
            message_text[:200],
        )
        return "auth"
    if 500 <= resp.status_code < 600:
        return "error"
    return "error"


def fetch_travis_log_with_rules(
    url: str, cfg: ScannerConfig, token_manager: Optional[TokenManager]
) -> str:
    """
    Fetch a Travis log respecting 302/404/401/403/5xx/429 rules.

    Returns: ok|gone|auth|error
    """
    resp, _ = travis_request_with_manager(
        url,
        token_manager,
        cfg,
        headers={"Accept": "text/plain"},
        allow_redirects=False,
    )
    if resp.status_code == 302:
        location = resp.headers.get("Location")
        if not location:
            return "error"
        follow, _ = travis_request_with_manager(
            location,
            token_manager,
            cfg,
            headers={"Accept": "text/plain"},
            allow_redirects=True,
            stream=False,
        )
        return "ok" if follow.status_code == 200 else "error"
    if resp.status_code == 200:
        return "ok"
    if resp.status_code == 404:
        return "gone"
    if resp.status_code in (401, 403):
        return "auth"
    if 500 <= resp.status_code < 600:
        return "error"
    return "error"


def evaluate_github_actions(
    owner: str, repo: str, cfg: ScannerConfig, token_manager: Optional[TokenManager]
) -> Tuple[str, Dict[str, Any]]:
    """Collect GitHub Actions runs until min_builds or until logs disappear."""
    builds_ok = 0
    page = 1
    consecutive_errors = 0
    MAX_CONSECUTIVE_ERRORS = 10

    while builds_ok < cfg.min_builds:
        url = f"https://api.github.com/repos/{owner}/{repo}/actions/runs"
        client = _create_github_client(cfg, token_manager, owner, repo)
        try:
            resp = client.request(
                url,
                params={"per_page": 50, "page": page, "status": "completed"},
                timeout=30,
            )
        except Exception as e:
            LOGGER.warning("GitHub Actions list failed: %s", e)
            return "error", {
                "builds_found": builds_ok,
                "message": f"Failed to list runs: {e}",
            }

        if resp.status_code == 404:
            return "missing_actions", {
                "builds_found": builds_ok,
                "message": "Actions disabled or repo missing.",
            }
        if resp.status_code != 200:
            return "error", {
                "builds_found": builds_ok,
                "message": f"Failed to list runs: {resp.status_code}",
            }

        data = resp.json()
        runs = data.get("workflow_runs", [])
        if not runs:
            if page == 1:
                return "missing_actions", {
                    "builds_found": builds_ok,
                    "message": "Actions disabled or no runs found.",
                }
            break

        LOGGER.info(
            "GitHub Actions page %s for %s/%s returned %s runs (total_ok=%s)",
            page,
            owner,
            repo,
            len(runs),
            builds_ok,
        )
        for run in runs:
            log_url = run.get("logs_url")
            if not log_url:
                continue
            actor = run.get("triggering_actor") or run.get("actor")
            if not _is_human_actor(actor):
                LOGGER.info(
                    "Skipping non-human actor for %s/%s run %s (%s)",
                    owner,
                    repo,
                    run.get("id"),
                    (actor or {}).get("login"),
                )
                continue
            status = fetch_github_log_with_rules(log_url, cfg, token_manager)
            if status == "ok":
                builds_ok += 1
                consecutive_errors = 0  # Reset on success
                LOGGER.info(
                    "GitHub Actions log OK for %s/%s run %s (total_ok=%s)",
                    owner,
                    repo,
                    run.get("id"),
                    builds_ok,
                )
            elif status == "gone":
                consecutive_errors += 1
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    LOGGER.warning(
                        "Too many consecutive 410/Gone errors (%d) for %s/%s. Skipping repo.",
                        consecutive_errors,
                        owner,
                        repo,
                    )
                    return "logs_gone", {
                        "builds_found": builds_ok,
                        "message": f"Logs removed (404) consecutively {consecutive_errors} times, stop scanning.",
                    }
            elif status == "auth":
                return "auth_failed", {
                    "builds_found": builds_ok,
                    "message": "401/403 fetching logs.",
                }
            elif status == "error":
                consecutive_errors += 1
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    LOGGER.warning(
                        "Too many consecutive errors (%d) for %s/%s. Skipping repo.",
                        consecutive_errors,
                        owner,
                        repo,
                    )
                    return "error", {
                        "builds_found": builds_ok,
                        "message": f"Too many consecutive errors {consecutive_errors}, stop scanning.",
                    }

            if builds_ok >= cfg.min_builds:
                break

        page += 1

    if builds_ok >= cfg.min_builds:
        return "ready", {
            "builds_found": builds_ok,
            "message": "Has enough downloadable GitHub Actions logs.",
        }
    return "insufficient", {
        "builds_found": builds_ok,
        "message": "Not enough Actions runs with logs.",
    }


def _travis_builds(
    owner: str, repo: str, cfg: ScannerConfig, token_manager: Optional[TokenManager]
) -> List[Dict[str, Any]]:
    slug = f"{owner}%2F{repo}"
    url = f"{cfg.travis_api_url.rstrip('/')}/repo/{slug}/builds"
    resp, _ = travis_request_with_manager(
        url,
        token_manager,
        cfg,
        params={"limit": 50, "sort_by": "started_at:desc", "include": "build.jobs"},
    )
    if resp.status_code != 200:
        LOGGER.warning(
            "Travis build list failed %s: %s", resp.status_code, resp.text[:200]
        )
        return []
    data = resp.json()
    return data.get("builds", [])


def _travis_job_ids(build: Dict[str, Any]) -> List[int]:
    job_ids: List[int] = []
    jobs = build.get("jobs") or []
    for job in jobs:
        if isinstance(job, dict) and "id" in job:
            job_ids.append(int(job["id"]))
    return job_ids


def evaluate_travis(
    owner: str, repo: str, cfg: ScannerConfig, token_manager: Optional[TokenManager]
) -> Tuple[str, Dict[str, Any]]:
    """Collect Travis job logs until min_builds or until logs disappear."""
    builds = _travis_builds(owner, repo, cfg, token_manager)
    if not builds:
        return "missing_travis", {
            "builds_found": 0,
            "message": "No Travis builds listed.",
        }

    builds_ok = 0
    for build in builds:
        for job_id in _travis_job_ids(build):
            log_url = f"{cfg.travis_api_url.rstrip('/')}/job/{job_id}/log"
            status = fetch_travis_log_with_rules(log_url, cfg, token_manager)
            if status == "ok":
                builds_ok += 1
                LOGGER.info(
                    "Travis log OK for %s/%s job %s (total_ok=%s)",
                    owner,
                    repo,
                    job_id,
                    builds_ok,
                )
            elif status == "gone":
                return "logs_gone", {
                    "builds_found": builds_ok,
                    "message": "Logs removed (404), stop scanning.",
                }
            elif status == "auth":
                return "auth_failed", {
                    "builds_found": builds_ok,
                    "message": "401/403 fetching Travis logs.",
                }
            if builds_ok >= cfg.min_builds:
                break
        if builds_ok >= cfg.min_builds:
            break

    if builds_ok >= cfg.min_builds:
        return "ready", {
            "builds_found": builds_ok,
            "message": "Has enough downloadable Travis logs.",
        }
    return "insufficient", {
        "builds_found": builds_ok,
        "message": "Not enough Travis job logs.",
    }


def scan_repository(
    repo: Dict[str, Any],
    cfg: ScannerConfig,
    github_tokens: Optional[TokenManager],
    travis_tokens: Optional[TokenManager],
    store: ScanStore,
) -> None:
    owner, name = repo["full_name"].split("/", 1)
    LOGGER.info("Scanning repo %s", repo["full_name"])
    if store.seen_any(repo["full_name"]):
        LOGGER.info("Skipping %s (already processed)", repo["full_name"])
        return
    providers = detect_ci(owner, name, cfg, github_tokens)
    LOGGER.info(
        "Detected CI providers for %s: %s", repo["full_name"], providers or "<none>"
    )
    if not providers:
        store.upsert(
            repo["full_name"], "unknown", "skipped", {"message": "No CI detected."}
        )
        return

    repo_details = {
        "stars": repo.get("stargazers_count", 0),
        "pushed_at": repo.get("pushed_at"),
        "languages_hint": repo.get("language"),
        "checked_at": datetime.now(timezone.utc),
        "segment_from": repo.get("_segment_from"),
        "segment_to": repo.get("_segment_to"),
    }

    for provider in providers:
        if store.seen(repo["full_name"], provider):
            continue

        if provider == "github_actions":
            status, details = evaluate_github_actions(
                owner, name, cfg, github_tokens
            )
        else:
            status, details = evaluate_travis(owner, name, cfg, travis_tokens)

        payload = {**repo_details, **details, "provider": provider}
        store.upsert(repo["full_name"], provider, status, payload)
        LOGGER.info(
            "[%s] %s -> %s (%s builds)",
            provider,
            repo["full_name"],
            status,
            payload.get("builds_found", 0),
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan GitHub repos for build logs (Actions/Travis)."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("crawler_config.yml"),
        help="Path to YAML config.",
    )
    parser.add_argument(
        "--limit", type=int, help="Optional max number of repos to scan."
    )
    parser.add_argument(
        "--min-builds",
        type=int,
        help="Override minimum builds/logs required (default from config).",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Continuously scan (infinite loop) until interrupted.",
    )
    parser.add_argument(
        "--loop-sleep",
        type=int,
        default=300,
        help="Seconds to sleep between scan iterations when --loop is set (default: 300).",
    )
    parser.add_argument(
        "--add-github-token",
        action="append",
        dest="add_github_tokens",
        help="Add a GitHub token (seeds Mongo if configured). Can be repeated.",
    )
    parser.add_argument(
        "--add-travis-token",
        action="append",
        dest="add_travis_tokens",
        help="Add a Travis token (seeds Mongo if configured). Can be repeated.",
    )
    parser.add_argument(
        "--remove-github-token",
        action="append",
        dest="remove_github_tokens",
        help="Remove a GitHub token (Mongo only). Can be repeated.",
    )
    parser.add_argument(
        "--remove-travis-token",
        action="append",
        dest="remove_travis_tokens",
        help="Remove a Travis token (Mongo only). Can be repeated.",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable INFO-level logging."
    )
    parser.add_argument(
        "--log-requests",
        action="store_true",
        help="Log each HTTP request (method, URL, status). Implies --verbose logging.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO if (args.verbose or args.log_requests) else logging.WARNING,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    global LOG_REQUESTS
    LOG_REQUESTS = bool(args.log_requests)

    cfg = load_config(args.config)
    if args.min_builds is not None:
        cfg.min_builds = max(1, args.min_builds)

    github_tokens = list(cfg.github_tokens or [])
    travis_tokens = list(cfg.travis_tokens or [])

    # Apply CLI token modifications to local lists before manager creation
    github_tokens.extend(args.add_github_tokens or [])
    travis_tokens.extend(args.add_travis_tokens or [])
    if args.remove_github_tokens:
        to_remove = set(args.remove_github_tokens)
        github_tokens = [t for t in github_tokens if t not in to_remove]
    if args.remove_travis_tokens:
        to_remove = set(args.remove_travis_tokens)
        travis_tokens = [t for t in travis_tokens if t not in to_remove]

    maintenance_performed = bool(
        (args.add_github_tokens or args.add_travis_tokens)
        or (args.remove_github_tokens or args.remove_travis_tokens)
    )

    try:
        github_manager = _init_token_manager(
            github_tokens,
            cfg,
            collection_name="github_tokens",
            token_type="github",
        )
    except RuntimeError as exc:
        LOGGER.error(exc)
        return

    travis_manager = _init_token_manager(
        travis_tokens,
        cfg,
        collection_name="travis_tokens",
        token_type="travis",
        allow_empty=True,
    )

    # Apply removals to Mongo collections if requested
    if isinstance(github_manager, MongoTokenManager):
        for token_str in args.remove_github_tokens or []:
            try:
                github_manager.remove_token(Token(token_str))
            except Exception as exc:
                LOGGER.warning("Failed to remove GitHub token %s: %s", token_str, exc)
    if isinstance(travis_manager, MongoTokenManager):
        for token_str in args.remove_travis_tokens or []:
            try:
                travis_manager.remove_token(Token(token_str))
            except Exception as exc:
                LOGGER.warning("Failed to remove Travis token %s: %s", token_str, exc)

    if maintenance_performed and args.limit is None and args.min_builds is None:
        LOGGER.info("Token maintenance complete; skipping scan.")
        return

    store = ScanStore(cfg.mongo_uri, cfg.db_name)

    def run_once(limit: Optional[int]) -> int:
        count_local = 0
        for repo in search_repositories(cfg, github_manager):
            if limit is not None and count_local >= limit:
                break
            if store.seen_any(repo.get("full_name", "")):
                LOGGER.info("Skipping %s (already in DB)", repo.get("full_name"))
                continue
            scan_repository(repo, cfg, github_manager, travis_manager, store)
            count_local += 1
        return count_local

    total_processed = 0
    if args.loop:
        iteration = 0
        while True:
            iteration += 1
            LOGGER.info("Starting scan iteration %s", iteration)
            processed = run_once(args.limit)
            total_processed += processed
            LOGGER.info(
                "Iteration %s complete. Processed %s repositories (total=%s). Sleeping %ss.",
                iteration,
                processed,
                total_processed,
                args.loop_sleep,
            )
            time.sleep(max(args.loop_sleep, 1))
    else:
        processed = run_once(args.limit)
        total_processed += processed
        LOGGER.info("Scanning complete. Processed %s repositories.", total_processed)


if __name__ == "__main__":
    main()
