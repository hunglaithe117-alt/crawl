"""Mongo helpers for storing scan progress and results."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pymongo import MongoClient
from pymongo.collection import Collection


class ScanStore:
    """Persist repository scan metadata to MongoDB."""

    def __init__(self, mongo_uri: str, db_name: str) -> None:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        self.repos: Collection = db["repositories"]
        self.repos.create_index([("slug", 1), ("provider", 1)], unique=True)

    def seen(self, slug: str, provider: str) -> bool:
        return (
            self.repos.count_documents(
                {"slug": slug, "provider": provider, "status": {"$ne": "retry"}}, limit=1
            )
            > 0
        )

    def seen_any(self, slug: str) -> bool:
        """Check whether the repo has been processed for any provider."""
        return (
            self.repos.count_documents(
                {"slug": slug, "status": {"$ne": "retry"}},
                limit=1,
            )
            > 0
        )

    def upsert(
        self,
        slug: str,
        provider: str,
        status: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        payload: Dict[str, Any] = {
            "slug": slug,
            "provider": provider,
            "status": status,
            "checked_at": datetime.now(timezone.utc),
        }
        if details:
            payload.update(details)
        self.repos.update_one(
            {"slug": slug, "provider": provider},
            {"$set": payload},
            upsert=True,
        )


__all__ = ["ScanStore"]
