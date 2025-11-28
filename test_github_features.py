import os
import sys
import logging
import shutil
import subprocess
import pandas as pd
import yaml
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from github_api_client import GitHubAPIClient
    from token_pool import TokenManager
except ImportError:
    # Fallback if running from different location
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from github_api_client import GitHubAPIClient
    from token_pool import TokenManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_config_local(config_path):
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f) or {}
    
    # Load tokens from tokens.yml if it exists and merge
    base_dir = os.path.dirname(config_path)
    tokens_path = os.path.join(base_dir, "tokens.yml")
    if os.path.exists(tokens_path):
        try:
            with open(tokens_path, "r") as f_tokens:
                tokens_config = yaml.safe_load(f_tokens) or {}
                if "github_tokens" in tokens_config:
                    cfg.setdefault("github_tokens", []).extend(
                        tokens_config["github_tokens"]
                    )
        except Exception as e:
            logger.warning(f"Failed to load tokens from {tokens_path}: {e}")
    return cfg


def clone_repo(repo_url, clone_dir):
    if os.path.exists(clone_dir):
        logger.info(f"Repo already exists at {clone_dir}")
        return True
    try:
        logger.info(f"Cloning {repo_url}...")
        subprocess.check_call(["git", "clone", repo_url, clone_dir])
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Clone failed: {e}")
        return False


# ---------------------------------------------------------
# Logic adapted from github_enrichment.py for GIT ONLY
# ---------------------------------------------------------
def get_commit_features_git(repo_dir, commit_sha, git_all_built_commits, row):
    print(f"[GIT MODE] Processing commit {commit_sha}")
    features = {}

    # Check if commit exists
    try:
        subprocess.check_call(
            ["git", "cat-file", "-e", commit_sha],
            cwd=repo_dir,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        print(f"[GIT] Commit {commit_sha} not found in repo.")
        return None

    # 1. Commit Date
    try:
        cmd = ["git", "show", "-s", "--format=%ct", commit_sha]
        ts = subprocess.check_output(cmd, cwd=repo_dir, text=True).strip()
        commit_ts = int(ts)
        commit_date = datetime.fromtimestamp(commit_ts)
        print(f"[GIT] Commit date: {commit_date}")
    except Exception as e:
        print(f"[GIT] Failed to get commit date: {e}")
        return None

    # 2. Touched Files & Authors
    touched_files = set()
    build_authors_name = set()
    
    print(f"[GIT] Processing {len(git_all_built_commits)} built commits...")
    for sha in git_all_built_commits:
        try:
            # Added -m to handle merge commits correctly
            cmd = ["git", "show", "-m", "--name-only", "--format=%an", sha]
            output = subprocess.check_output(cmd, cwd=repo_dir, text=True).splitlines()
            if len(output) >= 1:
                author_name = output[0]
                build_authors_name.add(author_name)
                for f in output[1:]:
                    if f.strip():
                        touched_files.add(f.strip())
        except Exception as e:
            print(f"[GIT] Failed to get touched files for {sha}: {e}")

    print(f"[GIT] Touched files: {len(touched_files)}")
    print(f"[GIT] Build authors: {build_authors_name}")

    if not touched_files:
        return features

    # 3. Author Ownership
    total_commits_scanned = 0
    owned_commits_count = 0
    target_authors = build_authors_name

    since_date = commit_date - timedelta(days=90)

    for filename in touched_files:
        full_path = os.path.join(repo_dir, filename)
        if os.path.exists(full_path):
            try:
                cmd = [
                    "git",
                    "log",
                    f"--since={since_date.isoformat()}",
                    f"--until={commit_date.isoformat()}",
                    "--pretty=format:%an",
                    filename,
                ]
                output = subprocess.check_output(cmd, cwd=repo_dir, text=True)
                lines = output.splitlines()
                file_commits = len(lines)
                file_owned = 0
                for author in lines:
                    if author in target_authors:
                        file_owned += 1

                total_commits_scanned += file_commits
                owned_commits_count += file_owned
                # print(f"  File {filename}: {file_owned}/{file_commits} owned")
            except Exception as e:
                print(f"[GIT] Error log for {filename}: {e}")

    # Denominator logic
    commits_on_files = row.get("gh_num_commits_on_files_touched", 0)
    if pd.isna(commits_on_files):
        commits_on_files = 0

    denominator = commits_on_files if commits_on_files > 0 else total_commits_scanned

    print(
        f"[GIT] Total Scanned: {total_commits_scanned}, Owned: {owned_commits_count}, Denom: {denominator}"
    )

    if denominator > 0:
        features["author_ownership"] = owned_commits_count / denominator
    else:
        features["author_ownership"] = 0.0

    features["file_change_frequency"] = 0
    if len(touched_files) > 0:
        features["file_change_frequency"] = denominator / len(touched_files)

    return features


# ---------------------------------------------------------
# Logic adapted from github_enrichment.py for API ONLY
# ---------------------------------------------------------
def get_commit_features_api(client, commit_sha, git_all_built_commits, row):
    logger.info(f"[API MODE] Processing commit {commit_sha}")
    features = {}

    # 1. Commit Date
    commit_ts = None
    commit_date = None
    try:
        c_data = client.get_commit(commit_sha)
        if not c_data:
            logger.error(f"[API] get_commit returned None for {commit_sha}")
            return None
            
        if "commit" in c_data and "author" in c_data["commit"]:
            c_date_str = c_data["commit"]["author"]["date"]
            dt = pd.to_datetime(c_date_str)
            commit_ts = int(dt.timestamp())
            commit_date = datetime.fromtimestamp(commit_ts)
            logger.info(f"[API] Commit date: {commit_date}")
        else:
            logger.error(f"[API] Commit data missing author/date. Data: {c_data}")
            return None
    except Exception as e:
        logger.error(f"[API] Failed to get commit: {e}")
        return None

    # 2. Touched Files & Authors
    touched_files = set()
    build_authors_name = set()

    logger.info(f"[API] Processing {len(git_all_built_commits)} built commits...")
    for sha in git_all_built_commits:
        try:
            c_data = client.get_commit(sha)
            if c_data:
                if "commit" in c_data and "author" in c_data["commit"]:
                    build_authors_name.add(c_data["commit"]["author"]["name"])

                if "files" in c_data:
                    for f in c_data["files"]:
                        touched_files.add(f["filename"])
        except Exception as e:
            logger.error(f"[API] Error parsing commit {sha}: {e}")

    logger.info(f"[API] Touched files: {len(touched_files)}")
    logger.info(f"[API] Build authors: {build_authors_name}")

    if not touched_files:
        return features

    # 3. Author Ownership
    total_commits_scanned = 0
    owned_commits_count = 0
    target_authors = build_authors_name

    since_date = commit_date - timedelta(days=90)

    for filename in touched_files:
        try:
            params = {
                "path": filename,
                "since": since_date.isoformat(),
                "until": commit_date.isoformat(),
                "per_page": 100,
            }
            # Note: client.get handles pagination if paginate=True, but let's be careful with rate limits
            # The original code uses paginate=True
            commits = client.get(
                f"/repos/{client.owner}/{client.repo}/commits",
                params=params,
                paginate=True,
            )

            file_commits = 0
            file_owned = 0

            if isinstance(commits, list):
                for c in commits:
                    file_commits += 1
                    if "commit" in c and "author" in c["commit"]:
                        author_name = c["commit"]["author"]["name"]
                        if author_name in target_authors:
                            file_owned += 1

            total_commits_scanned += file_commits
            owned_commits_count += file_owned
            # logger.info(f"  File {filename}: {file_owned}/{file_commits} owned (API)")

        except Exception as e:
            logger.warning(f"[API] Failed to fetch history for {filename}: {e}")

    commits_on_files = row.get("gh_num_commits_on_files_touched", 0)
    if pd.isna(commits_on_files):
        commits_on_files = 0

    denominator = commits_on_files if commits_on_files > 0 else total_commits_scanned

    logger.info(
        f"[API] Total Scanned: {total_commits_scanned}, Owned: {owned_commits_count}, Denom: {denominator}"
    )

    if denominator > 0:
        features["author_ownership"] = owned_commits_count / denominator
    else:
        features["author_ownership"] = 0.0

    features["file_change_frequency"] = 0
    if len(touched_files) > 0:
        features["file_change_frequency"] = denominator / len(touched_files)

    return features


def main():
    print("Starting main...")
    # Config
    REPO_OWNER = "edx"
    REPO_NAME = "configuration"
    COMMIT_SHA = "6f9851cccb5196aad076e8a812acbf756a077eea"
    BUILT_COMMITS_STR = "6f9851cccb5196aad076e8a812acbf756a077eea#d65d395c658ae7e09abed596050f36a430607298#5ce20f9a04e06f7a11facb8f5faadf3cd03041c3#1d3ad369100b80c91ff3965a145b0e257c0809a7#ca167c8ef6389c79ff0752eb3b74242f18dac7c0#b7787facbc27cce4b18ce01cc1b2601a9b27f3ce"
    
    git_all_built_commits = BUILT_COMMITS_STR.split("#")

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(BASE_DIR, "crawler_config.yml")
    REPOS_DIR = os.path.join(BASE_DIR, "repos_test")

    os.makedirs(REPOS_DIR, exist_ok=True)

    # Load tokens
    config = load_config_local(CONFIG_PATH)
    tokens = config.get("github_tokens", [])
    token_manager = TokenManager(tokens)

    client = GitHubAPIClient(REPO_OWNER, REPO_NAME, token_manager=token_manager)
    
    # Print available tokens
    print(f"Loaded {len(token_manager.tokens)} tokens.")
    if token_manager.tokens:
        print(f"Using token: {token_manager.tokens[0].key}")
    
    # Mock Row
    # We don't have gh_num_commits_on_files_touched, so we set it to 0 to force calculation from history
    row = {
        "gh_project_name": f"{REPO_OWNER}/{REPO_NAME}",
        "git_trigger_commit": COMMIT_SHA,
        "gh_num_commits_on_files_touched": 0,
    }

    print("=" * 60)
    print("TESTING GIT MODE")
    print("=" * 60)

    repo_dir = os.path.join(REPOS_DIR, f"{REPO_OWNER}_{REPO_NAME}")
    repo_url = f"https://github.com/{REPO_OWNER}/{REPO_NAME}.git"

    if clone_repo(repo_url, repo_dir):
        git_features = get_commit_features_git(repo_dir, COMMIT_SHA, git_all_built_commits, row)
        print("\nGIT Results:", git_features)
    else:
        print("Skipping GIT test due to clone failure")

    print("\n" + "=" * 60)
    print("TESTING API MODE")
    print("=" * 60)

    api_features = get_commit_features_api(client, COMMIT_SHA, git_all_built_commits, row)
    print("\nAPI Results:", api_features)

    # Comparison
    if git_features and api_features:
        print("\n" + "=" * 60)
        print("COMPARISON")
        print("=" * 60)
        diff_ownership = abs(
            git_features["author_ownership"] - api_features["author_ownership"]
        )
        print(f"Author Ownership Diff: {diff_ownership:.6f}")
        if diff_ownership < 0.01:
            print(">> MATCH")
        else:
            print(">> MISMATCH")


if __name__ == "__main__":
    main()
