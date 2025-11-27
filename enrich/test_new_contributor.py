import os
import sys
import shutil
import pandas as pd
from datetime import datetime
import logging
import yaml

# Add current directory to path to import risk_features_enrichment
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# Add parent directory to path to import github_api_client and token_pool
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from risk_features_enrichment import check_is_new_contributor, clone_repo
from github_api_client import GitHubAPIClient
from token_pool import TokenManager

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("Starting test script...")

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
    # Owner/Repo will be set per request or just dummy here
    return GitHubAPIClient("AlchemyCMS", "alchemy_cms", token_manager=token_manager)

def test_check_is_new_contributor():
    repo_url = "https://github.com/AlchemyCMS/alchemy_cms.git"
    repo_dir = "/tmp/test_alchemy_cms"
    
    # 1. Clone Repo
    print(f"Cloning {repo_url} to {repo_dir}...")
    if os.path.exists(repo_dir):
        shutil.rmtree(repo_dir)
    
    clone_success = clone_repo(repo_url, repo_dir)
    if not clone_success:
        print("Failed to clone repo")
        return
    print("Clone successful.")

    # 2. Test Case 1
    commit_sha = "dfcbe784a598382625a2da337613da04b73785d5"
    build_time_str = "2011-10-12 08:40:16"
    build_time = pd.to_datetime(build_time_str)
    
    print(f"Testing commit {commit_sha} at {build_time} (GIT MODE)")
    
    # Test with Git only (client=None)
    is_new, error = check_is_new_contributor(repo_dir, commit_sha, build_time, client=None)
    print(f"Result for {commit_sha}: is_new_contributor={is_new}, error={error}")

    # 3. Test Case 2 (Another commit)
    commit_sha_2 = "83ca85f58495cad524ec70198f9d422ff95ab3b4"
    build_time_str_2 = "2011-10-12 08:50:41"
    build_time_2 = pd.to_datetime(build_time_str_2)

    print(f"Testing commit {commit_sha_2} at {build_time_2} (GIT MODE)")
    is_new_2, error_2 = check_is_new_contributor(repo_dir, commit_sha_2, build_time_2, client=None)
    print(f"Result for {commit_sha_2}: is_new_contributor={is_new_2}, error={error_2}")

    # 4. Test API Fallback
    print("-" * 30)
    print("Testing API Fallback (repo_dir=None)")
    client = get_client()
    if client:
        print(f"Testing commit {commit_sha} at {build_time} (API MODE)")
        # Pass repo_dir=None to force API usage
        is_new_api, error_api = check_is_new_contributor(None, commit_sha, build_time, client=client)
        print(f"Result for {commit_sha} (API): is_new_contributor={is_new_api}, error={error_api}")
    else:
        print("Skipping API test (no client)")

    # Cleanup
    # if os.path.exists(repo_dir):
    #     shutil.rmtree(repo_dir)
    print("Done.")

if __name__ == "__main__":
    test_check_is_new_contributor()
