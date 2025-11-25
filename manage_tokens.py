#!/usr/bin/env python3
"""
Script to manage GitHub tokens in the MongoDB token pool.
Allows adding, removing, and listing tokens while the crawler is running.
"""

import argparse
import logging
import os
import sys
import yaml
from datetime import datetime, timezone

# Add parent directory to path to import token_pool
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from token_pool import MongoTokenPool

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_config(config_path):
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except Exception:
        return {}


def main():
    parser = argparse.ArgumentParser(description="Manage GitHub Tokens in MongoDB Pool")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Add command
    add_parser = subparsers.add_parser(
        "add", help="Add a new token or re-enable existing"
    )
    add_parser.add_argument("token", help="The GitHub token string")
    add_parser.add_argument(
        "--type", default="github", help="Token type (default: github)"
    )

    # Remove command
    remove_parser = subparsers.add_parser("remove", help="Remove a token permanently")
    remove_parser.add_argument("token", help="The GitHub token string")
    remove_parser.add_argument(
        "--type", default="github", help="Token type (default: github)"
    )

    # List command
    list_parser = subparsers.add_parser("list", help="List all tokens and their status")
    list_parser.add_argument(
        "--type", default="github", help="Token type (default: github)"
    )

    # Common arguments
    parser.add_argument(
        "--file", help="Path to token file (for InMemoryTokenPool mode)"
    )

    args = parser.parse_args()

    # Config setup (similar to github_enrichment.py)
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(BASE_DIR, "crawler_config.yml")
    config = load_config(CONFIG_PATH)

    # Check for NO_MONGO env var or --file arg
    NO_MONGO = os.environ.get("NO_MONGO", "false").lower() in ("true", "1", "yes")
    token_file = args.file or os.environ.get("TOKEN_FILE", "tokens.txt")

    if args.file or NO_MONGO:
        logger.info(f"Using file mode: {token_file}")

        if args.command == "add":
            # Append to file if not exists
            try:
                existing = []
                if os.path.exists(token_file):
                    with open(token_file, "r") as f:
                        existing = [l.strip() for l in f.readlines()]

                if args.token in existing:
                    logger.info(f"Token already exists in {token_file}")
                else:
                    with open(token_file, "a") as f:
                        f.write(f"{args.token}\n")
                    logger.info(f"Added token to {token_file}")
            except Exception as e:
                logger.error(f"Failed to write to {token_file}: {e}")

        elif args.command == "remove":
            try:
                if not os.path.exists(token_file):
                    logger.warning(f"File {token_file} does not exist.")
                else:
                    with open(token_file, "r") as f:
                        lines = f.readlines()

                    new_lines = [l for l in lines if l.strip() != args.token]

                    if len(new_lines) < len(lines):
                        with open(token_file, "w") as f:
                            f.writelines(new_lines)
                        logger.info(f"Removed token from {token_file}")
                    else:
                        logger.warning(f"Token not found in {token_file}")
            except Exception as e:
                logger.error(f"Failed to update {token_file}: {e}")

        elif args.command == "list":
            if not os.path.exists(token_file):
                logger.info(f"File {token_file} does not exist.")
            else:
                with open(token_file, "r") as f:
                    lines = f.readlines()
                print(f"Tokens in {token_file}:")
                for line in lines:
                    if line.strip() and not line.startswith("#"):
                        print(f"- {line.strip()}")
        return

    mongo_uri = os.environ.get(
        "MONGO_URI", config.get("mongo_uri", "mongodb://localhost:27017")
    )
    db_name = os.environ.get("DB_NAME", config.get("db_name", "ci_crawler"))

    logger.info(f"Connecting to MongoDB at {mongo_uri} (DB: {db_name})")
    try:
        pool = MongoTokenPool(mongo_uri, db_name)
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        sys.exit(1)

    if args.command == "add":
        if pool.add_token(args.type, args.token, enable=True):
            logger.info(f"Successfully added/enabled token: {args.token[:4]}...")
        else:
            logger.warning(f"Failed to add token (might be empty): {args.token[:4]}...")

    elif args.command == "remove":
        if pool.remove_token(args.type, args.token):
            logger.info(f"Successfully removed token: {args.token[:4]}...")
        else:
            logger.warning(f"Token not found or failed to remove: {args.token[:4]}...")

    elif args.command == "list":
        # MongoTokenPool doesn't expose list directly, so we access collection
        tokens = list(pool.collection.find({"type": args.type}))
        if not tokens:
            logger.info(f"No tokens found for type '{args.type}'.")
        else:
            logger.info(f"Found {len(tokens)} tokens for type '{args.type}':")
            print(
                f"{'Token':<20} | {'Disabled':<10} | {'Cooldown Until':<30} | {'Last Used':<30}"
            )
            print("-" * 100)
            for t in tokens:
                token_masked = (
                    f"{t['token'][:4]}...{t['token'][-4:]}"
                    if len(t["token"]) > 8
                    else t["token"]
                )
                disabled = str(t.get("disabled", False))
                cooldown = t.get("cooldown_until")
                last_used = t.get("last_used")

                # Format dates
                cooldown_str = cooldown.isoformat() if cooldown else "Ready"
                if cooldown and cooldown <= datetime.now(timezone.utc):
                    cooldown_str = "Ready"

                last_used_str = last_used.isoformat() if last_used else "Never"

                print(
                    f"{token_masked:<20} | {disabled:<10} | {cooldown_str:<30} | {last_used_str:<30}"
                )


if __name__ == "__main__":
    main()
