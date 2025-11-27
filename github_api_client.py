"""GitHub API client with rate limiting and token rotation."""

from __future__ import annotations

import logging
import time
import threading
import hashlib
import json
import os
from typing import Any, Dict, List, Optional, Sequence
import requests
from requests.models import Response

# Try relative import first (for package usage), then absolute
try:
    from .token_pool import TokenManager
    from .gce_rotator import GCERotator
except ImportError:
    from token_pool import TokenManager
    from gce_rotator import GCERotator

logger = logging.getLogger(__name__)

# --- GLOBAL CONTROLS FOR IP ROTATION ---
# Shared across all GitHubAPIClient instances
network_ready_event = threading.Event()
network_ready_event.set()  # Initially ready
rotation_lock = threading.Lock()
ip_rotator = GCERotator()


def check_and_rotate_ip() -> None:
    # Try to acquire lock non-blocking. If locked, rotation is already in progress.
    if rotation_lock.acquire(blocking=False):
        try:
            logger.warning("🚨 DETECTED BLOCK (429). INITIATING IP ROTATION...")

            # 1. Pause all workers
            network_ready_event.clear()

            # 2. Perform rotation
            success = ip_rotator.rotate_ip()

            if success:
                logger.info("✅ IP Rotated successfully. Resuming crawlers...")
            else:
                logger.error("❌ IP Rotation Failed. Sleeping 60s instead.")
                time.sleep(60)

        except Exception as e:
            logger.error(f"💥 Error during IP rotation: {e}")
        finally:
            # 3. Resume workers
            network_ready_event.set()
            rotation_lock.release()


class GitHubAPIClient:
    """
    GitHub API client with automatic rate limiting and token rotation.
    Uses Centralized Token Manager for smart waiting and load balancing.
    """

    def __init__(
        self,
        owner: str,
        repo: str,
        tokens: Optional[Sequence[str]] = None,
        token_manager: Optional[TokenManager] = None,
        retry_count: int = 5,
        retry_delay: float = 1.0,
        cache_dir: Optional[str] = None,
    ) -> None:
        """
        Initialize GitHub API client.

        Args:
            owner: Repository owner
            repo: Repository name
            tokens: List of GitHub personal access tokens
            token_manager: Optional external TokenManager instance
            retry_count: Number of retries for failed requests
            retry_delay: Base delay for exponential backoff
            cache_dir: Directory to store ETag caches
        """
        self.owner = owner
        self.repo = repo

        if token_manager:
            self._token_manager = token_manager
        elif tokens:
            self._token_manager = TokenManager(list(tokens))
        else:
            self._token_manager = TokenManager([])

        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.cache_dir = cache_dir
        if self.cache_dir:
            os.makedirs(self.cache_dir, exist_ok=True)

    @property
    def repo_slug(self) -> str:
        """Get the repository slug (owner/repo)."""
        return f"{self.owner}/{self.repo}"

    def _github_headers(self, token_key: str) -> Dict[str, str]:
        """
        Build headers for GitHub API request.
        """
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "travistorrent-tools-py",
        }
        if token_key:
            if token_key.startswith("github_pat_"):
                headers["Authorization"] = f"Bearer {token_key}"
            else:
                headers["Authorization"] = f"token {token_key}"
        return headers

    def _get_cache_path(self, url: str, params: Optional[Dict] = None) -> str:
        if not self.cache_dir:
            return ""
        key = f"{url}:{json.dumps(params, sort_keys=True)}"
        hashed = hashlib.md5(key.encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{hashed}.json")

    def request(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET",
        json_data: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> requests.Response:
        """
        Make a GitHub API request with automatic rate limit handling.
        Supports ETag caching if cache_dir is configured.
        """
        retries = 0
        max_retries = self.retry_count

        # Check cache for ETag (only for GET)
        etag = None
        cache_path = ""
        if self.cache_dir and method == "GET":
            cache_path = self._get_cache_path(url, params)
            if os.path.exists(cache_path):
                try:
                    with open(cache_path, "r") as f:
                        cached = json.load(f)
                        etag = cached.get("etag")
                except Exception:
                    pass

        while True:
            # 0. Check Network Ready Flag (Global Pause)
            # If IP rotation is happening, this will block until it's done.
            network_ready_event.wait()

            # 1. Acquire Token (Blocking)
            try:
                token = self._token_manager.get_best_token()
            except RuntimeError as e:
                logger.error(f"Token acquisition failed: {e}")
                raise

            try:
                # Prepare headers
                request_headers = self._github_headers(token.key)
                if "headers" in kwargs:
                    request_headers.update(kwargs.pop("headers"))

                if etag:
                    request_headers["If-None-Match"] = etag

                logger.info(f"🚀 Using token {token.key} for {method} {url}")

                response = requests.request(
                    method,
                    url,
                    headers=request_headers,
                    params=params,
                    json=json_data,
                    timeout=kwargs.get("timeout", 30),
                    **{k: v for k, v in kwargs.items() if k != "timeout"},
                )

                # 2. Update Token immediately
                self._token_manager.update_token(token, response.headers)

                # Handle 304 Not Modified
                if response.status_code == 304 and self.cache_dir and method == "GET":
                    logger.info(f"⚡ 304 Not Modified for {url}. Using cache.")
                    try:
                        with open(cache_path, "r") as f:
                            cached_data = json.load(f)

                        # Construct response from cache
                        cached_resp = Response()
                        cached_resp.status_code = 200
                        cached_resp._content = json.dumps(cached_data["data"]).encode(
                            "utf-8"
                        )
                        cached_resp.headers = response.headers
                        cached_resp.url = url
                        return cached_resp
                    except Exception as e:
                        logger.warning(f"Failed to load cache for {url}: {e}")
                        # If cache load fails, we might want to retry without ETag?
                        # For now, just continue and maybe it will fail or we can force retry
                        etag = None  # Disable ETag for next retry if we loop
                        # But we are inside loop. If we continue, we retry request.
                        # Let's just force a retry without ETag
                        continue

                # Handle Invalid Token (401)
                if response.status_code == 401:
                    logger.error(
                        f"🚫 Token {token.key} is INVALID (401). Removing from pool."
                    )
                    self._token_manager.remove_token(token)
                    continue

                # Handle Rate Limits (403/429)
                if response.status_code == 429:
                    logger.warning(
                        f"❌ 429 Too Many Requests. Token {token.key} exhausted."
                    )
                    self._token_manager.handle_rate_limit(token)

                    # Trigger IP Rotation Logic
                    # We spawn a thread to handle rotation so this worker can loop back and wait
                    threading.Thread(target=check_and_rotate_ip).start()
                    continue

                if response.status_code == 403:
                    remaining = response.headers.get("X-RateLimit-Remaining")
                    if remaining == "0":
                        logger.warning(
                            f"❌ 403 Rate Limit Exceeded. Token {token.key} exhausted."
                        )
                        self._token_manager.handle_rate_limit(token)
                        continue

                if 400 < response.status_code < 500:
                    # Check for spammy
                    if "spammy" in response.text.lower():
                        logger.warning(
                            f"🚫 Token {token.key} flagged as spammy."
                        )
                        self._token_manager.disable_token(token)
                        continue

                # Handle Server Errors (5xx) - Retry logic
                if response.status_code >= 500:
                    if retries < max_retries:
                        wait_time = self.retry_delay * (2**retries)
                        logger.warning(
                            f"Server error {response.status_code}. Retrying in {wait_time:.1f}s..."
                        )
                        time.sleep(wait_time)
                        retries += 1
                        continue

                # Success (200) - Save to cache if enabled
                if response.status_code == 200 and self.cache_dir and method == "GET":
                    etag_new = response.headers.get("ETag")
                    if etag_new:
                        try:
                            with open(cache_path, "w") as f:
                                json.dump(
                                    {"etag": etag_new, "data": response.json()}, f
                                )
                        except Exception as e:
                            logger.warning(f"Failed to write cache: {e}")

                # Success or other client errors (404, etc)
                return response

            except requests.exceptions.ConnectionError:
                # This happens if network is cut during IP rotation
                logger.warning(
                    "⚠️ Network Error (Likely due to IP Rotation). Retrying later..."
                )
                time.sleep(5)
                continue

            except requests.exceptions.RequestException as e:
                if retries < max_retries:
                    wait_time = self.retry_delay * (2**retries)
                    logger.warning(
                        f"Request failed: {e}. Retrying in {wait_time:.1f}s..."
                    )
                    time.sleep(wait_time)
                    retries += 1
                    continue
                raise RuntimeError(
                    f"GitHub API request failed after {max_retries} retries: {e}"
                )

    def get(
        self,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        paginate: bool = False,
    ) -> Any:
        """
        GET request to GitHub API.

        Args:
            path: API path (e.g., "/repos/owner/repo")
            params: Query parameters
            paginate: Whether to automatically paginate results

        Returns:
            JSON response (list if paginated, otherwise dict or list)
        """
        params = params or {}
        base_url = f"https://api.github.com{path}"

        results: Any
        if paginate:
            aggregated: List[Any] = []
            url = base_url
            query_params = params
            while url:
                response = self.request(url, query_params)
                page = response.json()
                if isinstance(page, list):
                    aggregated.extend(page)
                else:
                    aggregated.append(page)
                if "next" in response.links:
                    url = response.links["next"]["url"]
                    query_params = None
                else:
                    url = None
            results = aggregated
        else:
            response = self.request(base_url, params)
            results = response.json()

        return results

    def get_commit(self, sha: str) -> Optional[Dict[str, Any]]:
        """
        Fetch commit data from GitHub API.

        Args:
            sha: Commit SHA

        Returns:
            Commit data, or None if not found
        """
        try:
            logger.debug("GET /repos/%s/%s/commits/%s", self.owner, self.repo, sha)
            commit_data = self.get(f"/repos/{self.owner}/{self.repo}/commits/{sha}")
            return commit_data if isinstance(commit_data, dict) else None
        except Exception as exc:
            logger.warning("Failed to fetch commit %s from GitHub API: %s", sha, exc)
            return None

    def get_commits(
        self, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch a list of commits from GitHub API.

        Args:
            params: Query parameters (e.g., {'author': 'username', 'per_page': 1})

        Returns:
            List of commit objects
        """
        try:
            logger.debug("GET /repos/%s/%s/commits", self.owner, self.repo)
            commits = self.get(
                f"/repos/{self.owner}/{self.repo}/commits", params=params
            )
            return commits if isinstance(commits, list) else []
        except Exception as exc:
            logger.warning("Failed to fetch commits list: %s", exc)
            return []

    def get_repository(self) -> Optional[Dict[str, Any]]:
        """
        Fetch repository metadata from GitHub API.

        Returns:
            Repository data, or None if not found
        """
        try:
            logger.debug("GET /repos/%s/%s", self.owner, self.repo)
            repo_data = self.get(f"/repos/{self.owner}/{self.repo}")
            return repo_data if isinstance(repo_data, dict) else None
        except Exception as exc:
            logger.warning(
                "Failed to fetch repository %s/%s from GitHub API: %s",
                self.owner,
                self.repo,
                exc,
            )
            return None

    def get_languages(self) -> Optional[Dict[str, int]]:
        """
        Fetch repository languages from GitHub API.

        Returns:
            Dictionary of languages and their byte counts
        """
        try:
            logger.debug("GET /repos/%s/%s/languages", self.owner, self.repo)
            languages = self.get(f"/repos/{self.owner}/{self.repo}/languages")
            return languages if isinstance(languages, dict) else None
        except Exception as exc:
            logger.warning(
                "Failed to fetch languages for %s/%s from GitHub API: %s",
                self.owner,
                self.repo,
                exc,
            )
            return None

    def get_pull_request(self, pull_number: int) -> Optional[Dict[str, Any]]:
        """
        Fetch pull request data from GitHub API.

        Args:
            pull_number: Pull request number

        Returns:
            Pull request data, or None if not found
        """
        try:
            logger.debug(
                "GET /repos/%s/%s/pulls/%s", self.owner, self.repo, pull_number
            )
            pr_data = self.get(f"/repos/{self.owner}/{self.repo}/pulls/{pull_number}")
            return pr_data if isinstance(pr_data, dict) else None
        except Exception as exc:
            logger.warning(
                "Failed to fetch pull request #%s from GitHub API: %s", pull_number, exc
            )
            return None

    def get_commit_comments(self, sha: str) -> List[Dict[str, Any]]:
        """
        Fetch comments for a commit from GitHub API.

        Args:
            sha: Commit SHA

        Returns:
            List of comment objects
        """
        try:
            logger.debug(
                "GET /repos/%s/%s/commits/%s/comments", self.owner, self.repo, sha
            )
            comments = self.get(
                f"/repos/{self.owner}/{self.repo}/commits/{sha}/comments", paginate=True
            )
            return comments if isinstance(comments, list) else []
        except Exception as exc:
            logger.warning("Failed to fetch comments for commit %s: %s", sha, exc)
            return []

    def get_pull_commits(self, pull_number: int) -> List[Dict[str, Any]]:
        """
        Fetch commits for a pull request from GitHub API.

        Args:
            pull_number: Pull request number

        Returns:
            List of commit objects
        """
        try:
            logger.debug(
                "GET /repos/%s/%s/pulls/%s/commits", self.owner, self.repo, pull_number
            )
            commits = self.get(
                f"/repos/{self.owner}/{self.repo}/pulls/{pull_number}/commits",
                paginate=True,
            )
            return commits if isinstance(commits, list) else []
        except Exception as exc:
            logger.warning(
                "Failed to fetch commits for pull request #%s: %s", pull_number, exc
            )
            return []

    def get_pull_comments(self, pull_number: int) -> List[Dict[str, Any]]:
        """
        Fetch review comments for a pull request from GitHub API.

        Args:
            pull_number: Pull request number

        Returns:
            List of review comment objects
        """
        try:
            logger.debug(
                "GET /repos/%s/%s/pulls/%s/comments", self.owner, self.repo, pull_number
            )
            comments = self.get(
                f"/repos/{self.owner}/{self.repo}/pulls/{pull_number}/comments",
                paginate=True,
            )
            return comments if isinstance(comments, list) else []
        except Exception as exc:
            logger.warning(
                "Failed to fetch review comments for pull request #%s: %s",
                pull_number,
                exc,
            )
            return []

    def get_issue_comments(self, issue_number: int) -> List[Dict[str, Any]]:
        """
        Fetch issue/PR comments from GitHub API.

        Args:
            issue_number: Issue or pull request number

        Returns:
            List of comment objects
        """
        try:
            logger.debug(
                "GET /repos/%s/%s/issues/%s/comments",
                self.owner,
                self.repo,
                issue_number,
            )
            comments = self.get(
                f"/repos/{self.owner}/{self.repo}/issues/{issue_number}/comments",
                paginate=True,
            )
            return comments if isinstance(comments, list) else []
        except Exception as exc:
            logger.warning(
                "Failed to fetch issue comments for #%s: %s", issue_number, exc
            )
            return []

    def get_pull_reviews(self, pull_number: int) -> List[Dict[str, Any]]:
        """
        Fetch reviews for a pull request from GitHub API.

        Args:
            pull_number: Pull request number

        Returns:
            List of review objects
        """
        try:
            logger.debug(
                "GET /repos/%s/%s/pulls/%s/reviews", self.owner, self.repo, pull_number
            )
            reviews = self.get(
                f"/repos/{self.owner}/{self.repo}/pulls/{pull_number}/reviews",
                paginate=True,
            )
            return reviews if isinstance(reviews, list) else []
        except Exception as exc:
            logger.warning(
                "Failed to fetch reviews for pull request #%s: %s", pull_number, exc
            )
            return []

    def get_user(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Fetch user data from GitHub API.

        Args:
            username: GitHub username

        Returns:
            User data, or None if not found
        """
        try:
            logger.debug("GET /users/%s", username)
            # Note: User data is not repo-specific, but we use the same client for convenience
            user_data = self.get(f"/users/{username}")
            return user_data if isinstance(user_data, dict) else None
        except Exception as exc:
            logger.warning("Failed to fetch user %s: %s", username, exc)
            return None

    def graphql(
        self, query: str, variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute a GraphQL query.
        """
        url = "https://api.github.com/graphql"
        response = self.request(
            url, method="POST", json_data={"query": query, "variables": variables or {}}
        )

        if response.status_code == 200:
            data = response.json()
            if "errors" in data:
                logger.error(f"GraphQL Errors: {data['errors']}")
            return data
        else:
            logger.error(
                f"GraphQL Request Failed: {response.status_code} {response.text}"
            )
            return {}


__all__ = ["GitHubAPIClient"]
