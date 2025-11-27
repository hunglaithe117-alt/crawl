import os
import sys
import logging
import pandas as pd
import duckdb
import yaml
import shutil
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Add parent directory to path to import github_api_client and token_pool
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from github_api_client import GitHubAPIClient
    from token_pool import TokenManager
except ImportError:
    GitHubAPIClient = None
    TokenManager = None

from risk_features_enrichment import (
    get_build_time,
    check_is_new_contributor,
    clone_repo,
    load_config,
)


# Setup logging
class TqdmLoggingHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[TqdmLoggingHandler()],
    force=True,
)
logger = logging.getLogger(__name__)


def process_row(row, repo_dir, client):
    features = {}
    commit_sha = row["git_trigger_commit"]

    # 1. Build Time & Risk
    build_time = get_build_time(row, repo_dir, commit_sha, client)

    # 2. Is New Contributor
    is_new_contributor, error_reason = check_is_new_contributor(
        repo_dir, commit_sha, build_time, client
    )
    features["is_new_contributor"] = is_new_contributor

    return features


def process_project_group(
    project_name, group, repos_dir, config, token_manager, cleanup=True
):
    logger.info(f"[{project_name}] Processing {len(group)} rows")

    repo_url = f"https://github.com/{project_name}.git"
    owner, repo = project_name.split("/")
    repo_dir = os.path.join(repos_dir, f"{owner}_{repo}")

    # Clone
    has_repo = clone_repo(repo_url, repo_dir)
    if not has_repo:
        logger.warning(f"[{project_name}] Failed to clone, will use API fallback")
        repo_dir = None

    # Initialize Client
    client = None
    if GitHubAPIClient and token_manager:
        client = GitHubAPIClient(
            owner,
            repo,
            token_manager=token_manager,
            retry_count=config.get("github_api_retry_count", 3),
            retry_delay=config.get("github_api_retry_delay", 1.0),
        )

    results = []
    for idx, (index, row) in enumerate(group.iterrows()):
        feats = process_row(row, repo_dir, client)
        for k, v in feats.items():
            row[k] = v
        results.append(row)

    if cleanup and repo_dir and os.path.exists(repo_dir):
        try:
            shutil.rmtree(repo_dir)
        except Exception:
            pass

    return pd.DataFrame(results)


def main():
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    CONFIG_PATH = os.path.join(BASE_DIR, "crawler_config.yml")

    parser = argparse.ArgumentParser(description="Recalculate Specific Risk Features")
    parser.add_argument("--input", required=True, help="Input CSV file")
    parser.add_argument("--output", required=True, help="Output CSV file")
    parser.add_argument(
        "--repos-dir", default="/tmp/repos_recalc", help="Temp dir for repos"
    )
    parser.add_argument("--batch-size", type=int, default=1000)

    args = parser.parse_args()

    os.makedirs(args.repos_dir, exist_ok=True)
    config = load_config(CONFIG_PATH)

    tokens = config.get("github_tokens", [])
    token_manager = TokenManager(tokens) if TokenManager else None

    logger.info(f"Reading {args.input}...")
    try:
        df = duckdb.read_csv(args.input).df()
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        sys.exit(1)

    # Ensure columns exist
    cols_to_update = ["is_new_contributor"]
    for col in cols_to_update:
        if col not in df.columns:
            df[col] = None

    # Group by project
    grouped = df.groupby("gh_project_name")
    project_groups = [group for _, group in grouped]

    results = []

    # Sequential or Parallel? Let's do parallel
    max_workers = config.get("max_workers", 5)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_project = {
            executor.submit(
                process_project_group,
                group["gh_project_name"].iloc[0],
                group,
                args.repos_dir,
                config,
                token_manager,
            ): i
            for i, group in enumerate(project_groups)
        }

        for future in tqdm(
            as_completed(future_to_project), total=len(project_groups), desc="Projects"
        ):
            try:
                res_df = future.result()
                results.append(res_df)
            except Exception as e:
                logger.error(f"Project failed: {e}")

    if results:
        final_df = pd.concat(results)
        logger.info(f"Saving to {args.output}")
        final_df.to_csv(args.output, index=False)
    else:
        logger.warning("No results generated")


if __name__ == "__main__":
    main()
