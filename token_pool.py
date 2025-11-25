"""Mongo-backed token pool with round-robin selection and cooldowns."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Iterable, Tuple, Protocol, Dict, List, Optional, Any
import threading
import time
import os

from pymongo import MongoClient, ReturnDocument
from pymongo.collection import Collection
from pymongo.errors import OperationFailure

LOGGER = logging.getLogger(__name__)


class TokenPool(Protocol):
    def acquire(self, token_type: str) -> Tuple[Optional[str], float]: ...
    def cooloff(self, token_type: str, token: str, seconds: float) -> None: ...
    def disable_token(self, token_type: str, token: str) -> None: ...
    def mark_rate_limited(
        self, token_type: str, token: Optional[str], reset_epoch: int
    ) -> None: ...
    def has_tokens(self, token_type: str) -> bool: ...
    def seed_tokens(self, token_type: str, tokens: Iterable[str]) -> None: ...
    def add_token(self, token_type: str, token: str, enable: bool = True) -> bool: ...
    def remove_token(self, token_type: str, token: str) -> bool: ...


class GitHubTokenPoolAdapter:
    """Adapter to make TokenPool compatible with GitHubAPIClient."""

    def __init__(self, pool: TokenPool, token_type: str = "github"):
        self.pool = pool
        self.token_type = token_type

    def acquire(self) -> Tuple[Optional[str], float]:
        return self.pool.acquire(self.token_type)

    def mark_rate_limited(self, token: Optional[str], reset_epoch: int) -> None:
        self.pool.mark_rate_limited(self.token_type, token, reset_epoch)

    def disable(self, token: Optional[str]) -> None:
        if token is None:
            # Anonymous auth failed or rate limited?
            # GitHubTokenPool disables anonymous for 1 hour on disable(None)
            reset_epoch = int(time.time() + 3600)
            self.pool.mark_rate_limited(self.token_type, None, reset_epoch)
        else:
            self.pool.disable_token(self.token_type, token)


class MongoTokenPool:
    """
    Token rotation backed by MongoDB.

    Tokens are stored with fields:
    - type: "github" or "travis"
    - token: secret value
    - disabled: bool
    - cooldown_until: datetime when token becomes available
    - last_used: datetime for round-robin ordering
    """

    def __init__(
        self, mongo_uri: str, db_name: str, collection: str = "tokens"
    ) -> None:
        self.client = MongoClient(mongo_uri)
        self.collection: Collection = self.client[db_name][collection]
        try:
            self._ensure_indexes()
        except OperationFailure as exc:
            LOGGER.error(
                "Cannot ensure Mongo indexes for token pool (auth/permission issue): %s",
                exc,
            )
            raise
        except Exception as exc:
            LOGGER.error("Cannot ensure Mongo indexes for token pool: %s", exc)
            raise

    def _ensure_indexes(self) -> None:
        self.collection.create_index([("type", 1), ("token", 1)], unique=True)
        self.collection.create_index(
            [("type", 1), ("disabled", 1), ("cooldown_until", 1)]
        )

    def seed_tokens(self, token_type: str, tokens: Iterable[str]) -> None:
        """Idempotently add a list of tokens into the pool."""
        for token in tokens:
            self.add_token(token_type, token, enable=True)

    def add_token(self, token_type: str, token: str, enable: bool = True) -> bool:
        """Insert or re-enable a token. Returns True if inserted/updated."""
        token = (token or "").strip()
        if not token:
            return False

        now = datetime.now(timezone.utc)
        try:
            result = self.collection.update_one(
                {"type": token_type, "token": token},
                {
                    "$setOnInsert": {
                        "last_used": datetime.fromtimestamp(0, tz=timezone.utc)
                    },
                    "$set": {
                        "disabled": not enable,
                        "cooldown_until": datetime.fromtimestamp(0, tz=timezone.utc),
                        "updated_at": now,
                    },
                },
                upsert=True,
            )
            return result.upserted_id is not None or result.modified_count > 0
        except OperationFailure as exc:
            LOGGER.error("Mongo auth/permission failed when adding token: %s", exc)
            raise

    def disable_token(self, token_type: str, token: str) -> None:
        """Disable a token permanently (e.g., auth failure)."""
        self.collection.update_one(
            {"type": token_type, "token": token},
            {"$set": {"disabled": True, "updated_at": datetime.now(timezone.utc)}},
        )

    def remove_token(self, token_type: str, token: str) -> bool:
        """Remove a token from the pool entirely."""
        token = (token or "").strip()
        if not token:
            return False
        result = self.collection.delete_one({"type": token_type, "token": token})
        return result.deleted_count > 0

    def mark_rate_limited(
        self, token_type: str, token: Optional[str], reset_epoch: int
    ) -> None:
        """Mark token as unavailable until the given epoch seconds."""
        query = {"type": token_type, "token": token}
        update: Dict[str, Any] = {
            "$set": {
                "cooldown_until": datetime.fromtimestamp(reset_epoch, tz=timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            }
        }

        if token is None:
            # Upsert for anonymous token
            update["$setOnInsert"] = {
                "disabled": False,
                "last_used": datetime.fromtimestamp(0, tz=timezone.utc),
            }
            self.collection.update_one(query, update, upsert=True)
        else:
            self.collection.update_one(query, update)

    def cooloff(self, token_type: str, token: str, seconds: float) -> None:
        """Mark token unavailable for a short duration."""
        until = datetime.now(timezone.utc) + timedelta(seconds=seconds)
        self.collection.update_one(
            {"type": token_type, "token": token},
            {
                "$set": {
                    "cooldown_until": until,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )

    def acquire(self, token_type: str) -> Tuple[str | None, float]:
        """
        Acquire an available token and return (token, wait_seconds).

        If no token is immediately available, returns (None, wait_seconds).
        """
        now = datetime.now(timezone.utc)
        doc = self.collection.find_one_and_update(
            {
                "type": token_type,
                "disabled": False,
                "$or": [
                    {"cooldown_until": {"$exists": False}},
                    {"cooldown_until": {"$lte": now}},
                ],
            },
            {"$set": {"last_used": now}},
            sort=[("last_used", 1), ("_id", 1)],
            return_document=ReturnDocument.AFTER,
        )
        if doc:
            return doc.get("token"), 0.0

        waiting = self.collection.find_one(
            {"type": token_type, "disabled": False},
            sort=[("cooldown_until", 1)],
        )
        if waiting and waiting.get("cooldown_until"):
            cooldown = waiting["cooldown_until"]
            # Ensure cooldown is offset-aware (UTC) if it comes back naive from Mongo
            if cooldown.tzinfo is None:
                cooldown = cooldown.replace(tzinfo=timezone.utc)
            wait_seconds = max((cooldown - now).total_seconds(), 0.0)
            return None, wait_seconds
        return None, 0.0

    def has_tokens(self, token_type: str) -> bool:
        return (
            self.collection.count_documents({"type": token_type, "disabled": False}) > 0
        )

    def next_ready_in(self, token_type: str) -> float:
        now = datetime.now(timezone.utc)
        waiting = self.collection.find_one(
            {"type": token_type, "disabled": False},
            sort=[("cooldown_until", 1)],
        )
        if waiting and waiting.get("cooldown_until"):
            return max((waiting["cooldown_until"] - now).total_seconds(), 0.0)
        return 0.0


class InMemoryTokenPool:
    """
    In-memory token rotation (non-persistent).
    """

    def __init__(self, token_file: Optional[str] = None) -> None:
        # {token_type: [{token: str, disabled: bool, cooldown_until: float, last_used: float}]}
        self._store: Dict[str, List[Dict[str, Any]]] = {}
        self._anonymous_state: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self.token_file = token_file
        if self.token_file:
            self.reload_from_file()

    def reload_from_file(self) -> None:
        """Reload tokens from the configured token file."""
        if not self.token_file or not os.path.exists(self.token_file):
            return

        try:
            with open(self.token_file, "r") as f:
                lines = f.readlines()

            # Simple format: token_string (assumes github type for now, or format type:token)
            # Let's assume just tokens for github for simplicity as per requirement
            for line in lines:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue

                # Check if type is specified "type:token"
                if ":" in line:
                    parts = line.split(":", 1)
                    t_type, t_token = parts[0], parts[1]
                else:
                    t_type, t_token = "github", line

                self.add_token(t_type, t_token)

            LOGGER.info(f"Reloaded tokens from {self.token_file}")
        except Exception as e:
            LOGGER.error(f"Failed to reload tokens from file: {e}")

    def seed_tokens(self, token_type: str, tokens: Iterable[str]) -> None:
        for token in tokens:
            self.add_token(token_type, token)

    def add_token(self, token_type: str, token: str, enable: bool = True) -> bool:
        token = (token or "").strip()
        if not token:
            return False
        with self._lock:
            if token_type not in self._store:
                self._store[token_type] = []

            # Check if exists
            for item in self._store[token_type]:
                if item["token"] == token:
                    item["disabled"] = not enable
                    item["cooldown_until"] = 0.0
                    return False

            self._store[token_type].append(
                {
                    "token": token,
                    "disabled": not enable,
                    "cooldown_until": 0.0,
                    "last_used": 0.0,
                }
            )
            return True

    def disable_token(self, token_type: str, token: str) -> None:
        with self._lock:
            items = self._store.get(token_type, [])
            for item in items:
                if item["token"] == token:
                    item["disabled"] = True
                    break

    def remove_token(self, token_type: str, token: str) -> bool:
        with self._lock:
            if token_type not in self._store:
                return False
            initial_len = len(self._store[token_type])
            self._store[token_type] = [
                t for t in self._store[token_type] if t["token"] != token
            ]
            return len(self._store[token_type]) < initial_len

    def mark_rate_limited(
        self, token_type: str, token: Optional[str], reset_epoch: int
    ) -> None:
        with self._lock:
            if token is None:
                if token_type not in self._anonymous_state:
                    self._anonymous_state[token_type] = {}
                self._anonymous_state[token_type]["cooldown_until"] = float(reset_epoch)
                return

            items = self._store.get(token_type, [])
            for item in items:
                if item["token"] == token:
                    item["cooldown_until"] = float(reset_epoch)
                    break

    def cooloff(self, token_type: str, token: str, seconds: float) -> None:
        until = time.time() + seconds
        with self._lock:
            items = self._store.get(token_type, [])
            for item in items:
                if item["token"] == token:
                    item["cooldown_until"] = until
                    break

    def acquire(self, token_type: str) -> Tuple[Optional[str], float]:
        now = time.time()
        with self._lock:
            items = self._store.get(token_type, [])
            # Filter usable
            candidates = [
                t for t in items if not t["disabled"] and t["cooldown_until"] <= now
            ]
            if candidates:
                # Sort by last_used to round-robin
                candidates.sort(key=lambda x: x["last_used"])
                chosen = candidates[0]
                chosen["last_used"] = now
                return chosen["token"], 0.0

            # If we have tokens but none are ready
            if items:
                waiting = [
                    t for t in items if not t["disabled"] and t["cooldown_until"] > now
                ]
                if waiting:
                    min_wait = min(t["cooldown_until"] for t in waiting) - now
                    return None, max(min_wait, 0.0)
                return None, 0.0

            # No tokens configured, check anonymous
            anon_state = self._anonymous_state.get(token_type, {})
            anon_cooldown = anon_state.get("cooldown_until", 0.0)
            if anon_cooldown > now:
                return None, anon_cooldown - now

            return None, 0.0

    def has_tokens(self, token_type: str) -> bool:
        with self._lock:
            items = self._store.get(token_type, [])
            return any(not t["disabled"] for t in items)

    def next_ready_in(self, token_type: str) -> float:
        now = time.time()
        with self._lock:
            items = self._store.get(token_type, [])
            waiting = [
                t for t in items if not t["disabled"] and t["cooldown_until"] > now
            ]
            if waiting:
                return max(min(t["cooldown_until"] for t in waiting) - now, 0.0)
            return 0.0


__all__ = ["MongoTokenPool", "InMemoryTokenPool", "GitHubTokenPoolAdapter"]
