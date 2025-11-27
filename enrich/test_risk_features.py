import os
import sys
import shutil
import pandas as pd
import logging
import yaml

# Add current directory to path to import risk_features_enrichment
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# Add parent directory to path to import github_api_client and token_pool
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from risk_features_enrichment import (
    get_build_time,
    calculate_change_entropy_feature,
    clone_repo,
    check_is_new_contributor
)
from github_api_client import GitHubAPIClient
from token_pool import TokenManager

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_client():
    tokens_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tokens.yml")
    tokens = []
    if os.path.exists(tokens_path):
        with open(tokens_path, "r") as f:
            cfg = yaml.safe_load(f)
            tokens = cfg.get("github_tokens", [])
    
    if not tokens:
        print("No tokens found in tokens.yml")
        return None

    token_manager = TokenManager(tokens)
    return GitHubAPIClient("AlchemyCMS", "alchemy_cms", token_manager=token_manager)

def test_features():
    repo_url = "https://github.com/AlchemyCMS/alchemy_cms.git"
    repo_dir = "/tmp/test_alchemy_cms_features"
    
    print(f"Cloning {repo_url} to {repo_dir}...")
    if os.path.exists(repo_dir):
        shutil.rmtree(repo_dir)
    
    clone_success = clone_repo(repo_url, repo_dir)
    if not clone_success:
        print("Failed to clone repo")
        return

    commit_sha = "dfcbe784a598382625a2da337613da04b73785d5"
    # Mock row data
    row = {
        "gh_build_started_at": "2011-10-12 08:40:16",
        "git_all_built_commits": commit_sha, # Simple case
        "gh_project_name": "AlchemyCMS/alchemy_cms",
        "tr_build_id": "12345"
    }
    
    client = get_client()

    print("-" * 30)
    print("Testing get_build_time...")
    build_time = get_build_time(row, repo_dir, commit_sha, client)
    print(f"Build Time: {build_time}")

    print("-" * 30)
    print("Testing calculate_change_entropy_feature...")
    entropy, error = calculate_change_entropy_feature(row, repo_dir, commit_sha, client, "AlchemyCMS/alchemy_cms", "12345")
    print(f"Entropy: {entropy}, Error: {error}")

    print("-" * 30)
    print("Testing check_is_new_contributor (re-verification)...")
    if build_time:
        is_new, error_new = check_is_new_contributor(repo_dir, commit_sha, build_time, client)
        print(f"Is New Contributor: {is_new}, Error: {error_new}")
    else:
        print("Skipping is_new_contributor because build_time is None")

    # Cleanup
    if os.path.exists(repo_dir):
        shutil.rmtree(repo_dir)
    print("Done.")

if __name__ == "__main__":
    test_features()
