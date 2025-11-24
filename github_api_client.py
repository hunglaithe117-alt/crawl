"""GitHub API client with rate limiting and token rotation."""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
import requests

logger = logging.getLogger(__name__)


class RateLimitExceeded(Exception):
    """Raised when GitHub API rate limit is exceeded."""

    pass


class GitHubTokenPool:
    """Rotate GitHub tokens to mitigate API rate limits."""

    def __init__(self, tokens: Sequence[str]) -> None:
        self._lock = threading.Lock()
        unique_tokens: List[str] = []
        seen: Set[str] = set()
        for token in tokens:
            token = token.strip()
            if not token or token in seen:
                continue
            seen.add(token)
            unique_tokens.append(token)
        self._tokens: List[str] = unique_tokens
        self._next_available: Dict[str, float] = {token: 0.0 for token in self._tokens}
        self._anonymous_next_available: float = 0.0
        self._index: int = 0

    @property
    def tokens(self) -> List[str]:
        """Get the list of tokens."""
        with self._lock:
            return list(self._tokens)

    def add_token(self, token: str) -> bool:
        """
        Add a new token to the pool.

        Args:
            token: The token to add

        Returns:
            True if the token was added, False if it already exists
        """
        token = token.strip()
        if not token:
            return False

        with self._lock:
            if token in self._next_available:
                return False
            self._tokens.append(token)
            self._next_available[token] = 0.0
            logger.info(f"Added new token to pool. Total tokens: {len(self._tokens)}")
            return True

    def has_tokens(self) -> bool:
        """Check if there are any tokens available."""
        return bool(self._tokens)

    def acquire(self) -> Tuple[Optional[str], float]:
        """
        Return a token ready for use and optional wait time in seconds.

        Returns:
            Tuple of (token, wait_time_seconds)
        """
        now = time.time()
        with self._lock:
            if not self._tokens:
                wait = max(self._anonymous_next_available - now, 0.0)
                return None, wait

            for _ in range(len(self._tokens)):
                token = self._tokens[self._index]
                if self._next_available[token] <= now:
                    return token, 0.0
                self._advance()

            token = min(
                self._tokens,
                key=lambda item: self._next_available.get(item, float("inf")),
            )
            wait = max(self._next_available.get(token, now) - now, 0.0)
            self._index = self._tokens.index(token)
            return token, wait

    def mark_rate_limited(
        self, token: Optional[str], reset_epoch: Optional[int]
    ) -> None:
        """
        Mark a token as rate limited until the given reset time.

        Args:
            token: The token that hit rate limit (or None for anonymous)
            reset_epoch: Unix timestamp when the rate limit resets
        """
        wait_until = float(reset_epoch) if reset_epoch else time.time() + 60.0
        with self._lock:
            if token is None:
                self._anonymous_next_available = wait_until
                return
            if token in self._next_available:
                self._next_available[token] = wait_until
            self._advance()

    def disable(self, token: Optional[str]) -> None:
        """
        Permanently disable a token (e.g., due to authentication failure).

        Args:
            token: The token to disable (or None for anonymous)
        """
        with self._lock:
            if token is None:
                self._anonymous_next_available = time.time() + 3600.0
                return
            if token not in self._next_available:
                return
            del self._next_available[token]
            self._tokens = [t for t in self._tokens if t != token]
            if not self._tokens:
                self._index = 0
            else:
                self._index %= len(self._tokens)

    def _advance(self) -> None:
        """Move to the next token in rotation."""
        if not self._tokens:
            return
        self._index = (self._index + 1) % len(self._tokens)


class GitHubAPIClient:
    """
    GitHub API client with automatic rate limiting and token rotation.

    This class handles:
    - Token rotation to maximize API quota
    - Automatic rate limit detection and waiting
    - Request retries with exponential backoff
    - Error handling for authentication failures
    """

    def __init__(
        self,
        owner: str,
        repo: str,
        tokens: Optional[Sequence[str]] = None,
        token_pool: Any = None,
        retry_count: int = 5,
        retry_delay: float = 1.0,
    ) -> None:
        """
        Initialize GitHub API client.

        Args:
            owner: Repository owner
            repo: Repository name
            tokens: List of GitHub personal access tokens
            token_pool: Optional external token pool instance
            retry_count: Number of retries for failed requests
            retry_delay: Base delay for exponential backoff
        """
        self.owner = owner
        self.repo = repo
        if token_pool:
            self._token_pool = token_pool
        else:
            self._token_pool = GitHubTokenPool(tokens or [])
        self._github_remaining: Optional[int] = None
        self._github_reset: Optional[int] = None
        self.retry_count = retry_count
        self.retry_delay = retry_delay

    @property
    def repo_slug(self) -> str:
        """Get the repository slug (owner/repo)."""
        return f"{self.owner}/{self.repo}"

    def _github_headers(self, token: Optional[str]) -> Dict[str, str]:
        """
        Build headers for GitHub API request.

        Args:
            token: GitHub access token (or None for anonymous)

        Returns:
            Dictionary of HTTP headers
        """
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "travistorrent-tools-py",
        }
        if token:
            headers["Authorization"] = f"token {token}"
        return headers

    def request(
        self, url: str, params: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """
        Make a GitHub API request with automatic rate limit handling.

        Args:
            url: Full URL to request
            params: Query parameters

        Returns:
            Response object

        Raises:
            RuntimeError: On authentication failure or other errors
        """
        retries = 0
        max_retries = self.retry_count

        while True:
            token, wait_for = self._token_pool.acquire()
            if wait_for > 0:
                logger.info(
                    "All GitHub tokens exhausted; sleeping for %.1f seconds",
                    wait_for,
                )
                time.sleep(wait_for)

            try:
                response = requests.get(
                    url,
                    headers=self._github_headers(token),
                    params=params,
                    timeout=30,
                )
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

            self._github_remaining = int(
                response.headers.get("X-RateLimit-Remaining", "0") or 0
            )
            reset_ts = response.headers.get("X-RateLimit-Reset")
            self._github_reset = int(reset_ts) if reset_ts else None

            # Handle 429 Too Many Requests
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait_seconds = 60.0
                if retry_after and retry_after.isdigit():
                    wait_seconds = max(float(retry_after), 1.0)

                logger.warning(
                    "GitHub 429 (Too Many Requests); token on cool-down for %.1fs",
                    wait_seconds,
                )
                # Use mark_rate_limited to simulate cooloff
                self._token_pool.mark_rate_limited(
                    token, int(time.time() + wait_seconds)
                )
                continue

            if response.status_code >= 500:
                if retries < max_retries:
                    wait_time = self.retry_delay * (2**retries)
                    logger.warning(
                        f"Server error {response.status_code}. Retrying in {wait_time:.1f}s..."
                    )
                    time.sleep(wait_time)
                    retries += 1
                    continue

            # Handle 4xx errors
            if 400 <= response.status_code < 500:
                body_text = response.text.lower()

                # Check for "spammy" detection
                if "spammy" in body_text:
                    logger.warning(
                        f"GitHub token {token} flagged as spammy; disabling and rotating."
                    )
                    self._token_pool.disable(token)
                    continue

                if (
                    response.status_code == 403
                    and self._github_remaining == 0
                    and self._github_reset
                ):
                    logger.warning(
                        "GitHub rate limit reached; rotating token (reset at %s)",
                        (
                            datetime.fromtimestamp(self._github_reset, tz=timezone.utc)
                            if self._github_reset
                            else "unknown"
                        ),
                    )
                    self._token_pool.mark_rate_limited(token, self._github_reset)
                    retries = 0  # Reset retries on token rotation
                    continue

                if response.status_code == 401:
                    self._token_pool.disable(token)
                    raise RuntimeError(
                        f"GitHub API authentication failed for {url}: {response.text[:200]}"
                    )

                raise RuntimeError(
                    f"GitHub API error {response.status_code} for {url}: {response.text[:200]}"
                )

            if self._github_remaining == 0 and self._github_reset:
                self._token_pool.mark_rate_limited(token, self._github_reset)

            return response

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


__all__ = ["GitHubAPIClient", "GitHubTokenPool", "RateLimitExceeded"]
