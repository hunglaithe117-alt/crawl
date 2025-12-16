"""Configuration loading for the build log scanner."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Sequence

import yaml


@dataclass
class ScannerConfig:
    """Runtime configuration for scanning repositories and build logs."""

    mongo_uri: str = "mongodb://localhost:27017"
    db_name: str = "ci_crawler"
    languages: List[str] = field(default_factory=lambda: ["Python", "Ruby", "Java"])
    github_tokens: List[str] = field(default_factory=list)
    travis_tokens: List[str] = field(default_factory=list)
    min_builds: int = 30
    search_sort: str = "stars"
    search_order: str = "desc"
    search_per_page: int = 50
    search_pages: int = 1
    min_stars: int = 50
    search_segments: List[dict] = field(
        default_factory=list
    )  # list of {"from": "...", "to": "..."} date strings
    segment_start: str = ""  # optional YYYY-MM-DD
    segment_end: str = ""  # optional YYYY-MM-DD
    updated_since: str = "2025-11-01"  # YYYY-MM-DD
    request_timeout: int = 30
    request_delay_seconds: float = 0.1
    retry_count: int = 3
    retry_base_delay_seconds: float = 0.25
    github_api_url: str = "https://api.github.com"
    travis_api_url: str = "https://api.travis-ci.com"
    sleep_on_429_seconds: int = 300

    @property
    def language_query(self) -> str:
        """Build the GitHub Search API language clause using OR semantics."""
        if not self.languages:
            return ""
        return " OR ".join(f"language:{lang}" for lang in self.languages)


def _read_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def load_config(path: Path) -> ScannerConfig:
    """
    Load configuration from YAML, falling back to sensible defaults.

    Only keys present in `ScannerConfig` are applied.
    """
    raw = _read_yaml(path)

    # Load tokens from tokens.yml if it exists in the same directory or parent
    tokens_path = path.parent / "tokens.yml"
    if not tokens_path.exists():
        tokens_path = path.parent.parent / "tokens.yml"

    if tokens_path.exists():
        tokens_data = _read_yaml(tokens_path)
        if "github_tokens" in tokens_data:
            raw.setdefault("github_tokens", []).extend(tokens_data["github_tokens"])
        if "travis_tokens" in tokens_data:
            raw.setdefault("travis_tokens", []).extend(tokens_data["travis_tokens"])
    cfg = ScannerConfig()
    for key, value in raw.items():
        if not hasattr(cfg, key):
            continue
        if key == "search_segments" and isinstance(value, Sequence):
            segments: List[dict] = []
            for seg in value:
                start = end = ""
                if isinstance(seg, dict):
                    start = str(
                        seg.get("from") or seg.get("start") or seg.get("from_") or ""
                    ).strip()
                    end = str(
                        seg.get("to") or seg.get("end") or seg.get("to_") or ""
                    ).strip()
                elif isinstance(seg, Sequence) and len(seg) >= 2:
                    start = str(seg[0]).strip()
                    end = str(seg[1]).strip()
                if start and end:
                    segments.append({"from": start, "to": end})
            setattr(cfg, key, segments)
        elif isinstance(getattr(cfg, key), list) and isinstance(value, Sequence):
            setattr(cfg, key, [str(item) for item in value])
        else:
            setattr(cfg, key, value)

    # Auto-build weekly search segments if none provided but start/end exist.
    if not cfg.search_segments and cfg.segment_start and cfg.segment_end:
        try:
            start_dt = datetime.fromisoformat(str(cfg.segment_start).strip())
            end_dt = datetime.fromisoformat(str(cfg.segment_end).strip())
            segments: List[dict] = []
            cur = start_dt
            while cur <= end_dt:
                nxt = min(cur + timedelta(days=6), end_dt)
                segments.append(
                    {"from": cur.date().isoformat(), "to": nxt.date().isoformat()}
                )
                cur = nxt + timedelta(days=1)
            cfg.search_segments = segments
        except Exception:
            # Fall back silently if dates are invalid
            pass
    return cfg


__all__ = ["ScannerConfig", "load_config"]
