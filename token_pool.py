"""Token pool management with smart rate limiting and load balancing."""

from __future__ import annotations

import logging
import threading
import time
from typing import List, Optional, Dict, Any

LOGGER = logging.getLogger(__name__)


class Token:
    """Represents a GitHub API token with its rate limit state."""

    def __init__(self, token_str: str):
        self.key = token_str.strip()
        self.remaining = 5000  # Assume max initially
        self.reset_time = 0.0  # Unix timestamp
        self.is_disabled = False

    def __repr__(self) -> str:
        return f"<Token {self.key[:4]}... Rem:{self.remaining}>"


class TokenManager:
    """
    Centralized Token Manager (Singleton-like behavior).
    Manages a pool of tokens, handles smart waiting, and load balancing.
    """

    def __init__(self, tokens: List[str]):
        # Deduplicate and create Token objects
        unique_tokens = set(t.strip() for t in tokens if t.strip())
        self.tokens = [Token(t) for t in unique_tokens]
        self.pool_lock = threading.Lock()

    def get_best_token(self) -> Token:
        """
        Blocking method: Waits until a usable token is available.
        Implements smart sleeping if all tokens are exhausted.
        """
        while True:
            with self.pool_lock:
                current_time = time.time()
                available_tokens = []
                valid_tokens_exist = False

                for t in self.tokens:
                    if t.is_disabled:
                        continue

                    valid_tokens_exist = True

                    # If reset time has passed, optimistically reset remaining
                    # This handles cases where we waited locally or just started up
                    if current_time > t.reset_time and t.remaining == 0:
                        t.remaining = 5000

                    if t.remaining > 0:
                        available_tokens.append(t)

                if not valid_tokens_exist:
                    raise RuntimeError("All tokens have been disabled/flagged as spam.")

                # Strategy: Load Balancing (Pick token with most remaining requests)
                if available_tokens:
                    selected_token = max(available_tokens, key=lambda x: x.remaining)
                    return selected_token

                # No tokens available -> Smart Wait
                # Find the earliest reset time among all non-disabled tokens
                active_tokens = [t for t in self.tokens if not t.is_disabled]
                if not active_tokens:
                    raise RuntimeError("All tokens disabled.")

                soonest_reset = min(t.reset_time for t in active_tokens)
                wait_seconds = soonest_reset - current_time + 1.0  # +1s buffer

                if wait_seconds < 0:
                    wait_seconds = 1.0

                LOGGER.warning(
                    f"⚠️ ALL TOKENS EXHAUSTED. System sleeping for {wait_seconds:.2f}s..."
                )

            # Sleep outside the lock to allow other threads to reach this point
            # (though they will also likely sleep or wait for lock)
            time.sleep(wait_seconds)

    def update_token(self, token: Token, headers: Any) -> None:
        """Update token state from GitHub API response headers."""
        with self.pool_lock:
            try:
                # headers keys are case-insensitive in requests, but let's be safe
                # requests.structures.CaseInsensitiveDict handles this usually
                remaining = headers.get("X-RateLimit-Remaining")
                reset = headers.get("X-RateLimit-Reset")

                if remaining is not None:
                    token.remaining = int(remaining)
                if reset is not None:
                    token.reset_time = float(reset)

                LOGGER.debug(
                    f"🔄 Updated {token.key[:4]}... | Rem: {token.remaining} | Resets in: {int(token.reset_time - time.time())}s"
                )
            except Exception as e:
                LOGGER.error(f"Error updating token {token.key[:4]}...: {e}")

    def handle_rate_limit(self, token: Token) -> None:
        """Manually mark token as exhausted (e.g. on 403/429)."""
        with self.pool_lock:
            token.remaining = 0
            # We don't know the exact reset time without headers,
            # but usually update_token is called with headers before this if available.
            # If this is called, it means we want to force rotation.
            # If reset_time is in the past, bump it slightly to force wait/check?
            # Actually, just setting remaining=0 is enough for get_best_token to skip it
            # until reset_time passes.
            if token.reset_time < time.time():
                token.reset_time = time.time() + 60  # Default cooloff if unknown

    def disable_token(self, token: Token) -> None:
        """Permanently disable a token (e.g. bad credentials, spammy)."""
        with self.pool_lock:
            token.is_disabled = True
            LOGGER.error(f"🚫 Token {token.key[:4]}... disabled permanently.")

    def remove_token(self, token: Token) -> None:
        """Remove a token from the pool (e.g. invalid)."""
        with self.pool_lock:
            try:
                self.tokens = [t for t in self.tokens if t.key != token.key]
                LOGGER.info(f"🗑️ Token {token.key[:4]}... removed from pool.")
            except Exception as e:
                LOGGER.error(f"Failed to remove token {token.key[:4]}...: {e}")


class MongoTokenManager(TokenManager):
    """
    Token Manager backed by MongoDB.
    Directly queries MongoDB for token operations (Stateless in-memory).
    """

    def __init__(
        self, mongo_uri: str, db_name: str, collection_name: str = "github_tokens"
    ):
        try:
            from pymongo import MongoClient, ASCENDING
        except ImportError:
            raise ImportError(
                "pymongo is required for MongoTokenManager. Install it with `pip install pymongo`."
            )

        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self._ensure_indexes()

        # Initialize parent with empty list as we don't use in-memory tokens list
        super().__init__([])
        self.pool_lock = threading.Lock()
        LOGGER.info(
            f"Initialized MongoTokenManager (Direct DB Mode) connected to {db_name}.{collection_name}"
        )

    def _ensure_indexes(self):
        from pymongo import ASCENDING

        self.collection.create_index([("token", ASCENDING)], unique=True)
        self.collection.create_index("type")
        self.collection.create_index("disabled")

    def add_token(self, token_type: str, token_str: str, enable: bool = True) -> bool:
        """Add a new token to MongoDB."""
        try:
            self.collection.update_one(
                {"token": token_str},
                {
                    "$set": {
                        "type": token_type,
                        "disabled": not enable,
                        "added_at": time.time(),
                    }
                },
                upsert=True,
            )
            return True
        except Exception as e:
            LOGGER.error(f"Failed to add token to Mongo: {e}")
            return False

    def get_best_token(self) -> Token:
        """
        Get the best available token from MongoDB.
        """
        while True:
            with self.pool_lock:
                current_time = time.time()

                # Fetch all non-disabled tokens
                cursor = self.collection.find({"disabled": {"$ne": True}})
                docs = list(cursor)

                if not docs:
                    raise RuntimeError("No active tokens found in MongoDB.")

                available_candidates = []
                min_reset_time = float("inf")

                for doc in docs:
                    remaining = doc.get("remaining", 5000)
                    reset_time = doc.get("reset_time", 0.0)

                    # Track min reset time for sleeping
                    if reset_time < min_reset_time:
                        min_reset_time = reset_time

                    # Check if token is usable
                    # 1. Remaining > 0
                    # 2. OR Reset time has passed (treat as full)
                    if current_time > reset_time:
                        remaining = 5000

                    if remaining > 0:
                        available_candidates.append((doc, remaining))

                if available_candidates:
                    # Load balancing: pick one with max remaining
                    best_doc, _ = max(available_candidates, key=lambda x: x[1])

                    # Construct Token object
                    t = Token(best_doc["token"])
                    t.remaining = best_doc.get("remaining", 5000)
                    t.reset_time = best_doc.get("reset_time", 0.0)

                    # If we assumed it reset, let's update the object state
                    if current_time > t.reset_time:
                        t.remaining = 5000

                    return t

                # If no tokens available, sleep
                wait_seconds = min_reset_time - current_time + 1.0
                if wait_seconds < 0:
                    wait_seconds = 1.0

                LOGGER.warning(
                    f"⚠️ ALL TOKENS EXHAUSTED (Mongo). Sleeping {wait_seconds:.2f}s..."
                )

            time.sleep(wait_seconds)

    def update_token(self, token: Token, headers: Any) -> None:
        """Update token status in MongoDB."""
        try:
            remaining = headers.get("X-RateLimit-Remaining")
            reset = headers.get("X-RateLimit-Reset")

            updates = {}
            if remaining is not None:
                updates["remaining"] = int(remaining)
            if reset is not None:
                updates["reset_time"] = float(reset)

            updates["last_used"] = time.time()

            if updates:
                self.collection.update_one({"token": token.key}, {"$set": updates})
        except Exception as e:
            LOGGER.error(f"Failed to update token {token.key[:4]}... in Mongo: {e}")

    def handle_rate_limit(self, token: Token) -> None:
        """Handle 403/429 by setting remaining to 0 and backing off in Mongo."""
        try:
            # Force a cool-off period
            new_reset = time.time() + 60
            self.collection.update_one(
                {"token": token.key},
                {"$set": {"remaining": 0, "reset_time": new_reset}},
            )
            LOGGER.warning(
                f"Token {token.key[:4]}... rate limited. Cooldown set in Mongo."
            )
        except Exception as e:
            LOGGER.error(
                f"Failed to handle rate limit for {token.key[:4]}... in Mongo: {e}"
            )

    def disable_token(self, token: Token) -> None:
        """Disable token in MongoDB."""
        try:
            self.collection.update_one(
                {"token": token.key},
                {"$set": {"disabled": True, "disabled_at": time.time()}},
            )
            LOGGER.info(f"🚫 Token {token.key[:4]}... disabled in MongoDB.")
        except Exception as e:
            LOGGER.error(f"Failed to disable token in Mongo: {e}")

    def remove_token(self, token: Token) -> None:
        """Permanently remove token from MongoDB."""
        try:
            result = self.collection.delete_one({"token": token.key})
            if result.deleted_count > 0:
                LOGGER.info(f"🗑️ Token {token.key[:4]}... removed from MongoDB.")
            else:
                LOGGER.warning(
                    f"Token {token.key[:4]}... not found in MongoDB to remove."
                )
        except Exception as e:
            LOGGER.error(f"Failed to remove token from Mongo: {e}")


# Alias for backward compatibility with manage_tokens.py
MongoTokenPool = MongoTokenManager
