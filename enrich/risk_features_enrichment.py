import os
import sys
import logging
import pandas as pd
import numpy as np
import subprocess
import argparse
import shutil
import math
import yaml
import glob
import duckdb
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Add parent directory to path to import github_api_client and token_pool
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from github_api_client import GitHubAPIClient
    from token_pool import MongoTokenPool, GitHubTokenPoolAdapter, InMemoryTokenPool
except ImportError:
    # Fallback for when running in a different context or if files are missing
    GitHubAPIClient = None
    MongoTokenPool = None
    GitHubTokenPoolAdapter = None
    InMemoryTokenPool = None

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_config(config_path):
    logger.info(f"Loading config from {config_path}")
    try:
        with open(config_path, "r") as f:
            cfg = yaml.safe_load(f)

        # Load tokens from tokens.yml if it exists
        BASE_DIR = os.path.dirname(config_path)
        TOKENS_PATH = os.path.join(BASE_DIR, "tokens.yml")
        if os.path.exists(TOKENS_PATH):
            try:
                with open(TOKENS_PATH, "r") as f_tokens:
                    tokens_config = yaml.safe_load(f_tokens) or {}
                    if "github_tokens" in tokens_config:
                        cfg.setdefault("github_tokens", []).extend(
                            tokens_config["github_tokens"]
                        )
            except Exception:
                pass
        return cfg
    except Exception as e:
        logger.warning(f"Config load failed: {e}")
        return {}


def clone_repo(repo_url, clone_dir):
    """Clones a repo if it doesn't exist."""
    if not os.path.exists(clone_dir):
        try:
            subprocess.check_call(
                ["git", "clone", repo_url, clone_dir],
                stderr=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
            )
            return True
        except subprocess.CalledProcessError:
            return False
    return True


def get_git_commit_date(repo_dir, commit_sha):
    try:
        cmd = ["git", "show", "-s", "--format=%ct", commit_sha]
        ts = subprocess.check_output(
            cmd, cwd=repo_dir, text=True, stderr=subprocess.DEVNULL
        ).strip()
        if ts:
            return datetime.fromtimestamp(int(ts))
    except Exception:
        pass
    return None


def calculate_entropy(file_changes):
    """
    Calculates Shannon entropy of changes across files.
    file_changes: list of integers (lines changed per file)
    """
    total_changes = sum(file_changes)
    if total_changes == 0:
        return 0.0

    entropy = 0.0
    for change in file_changes:
        if change > 0:
            p = change / total_changes
            entropy -= p * math.log2(p)
    return entropy


def get_risk_features(row, repo_dir, client=None):
    features = {}
    missing_log = None  # (tr_build_id, project, commit, reason)

    commit_sha = row["git_trigger_commit"]
    project_name = row["gh_project_name"]
    build_id = row.get("tr_build_id", "unknown")

    # 1. Build Hour Risk Score
    build_time = None
    if "gh_build_started_at" in row and not pd.isna(row["gh_build_started_at"]):
        try:
            build_time = pd.to_datetime(row["gh_build_started_at"])
        except:
            pass

    if build_time is None:
        # Try git
        if repo_dir:
            build_time = get_git_commit_date(repo_dir, commit_sha)

        # Fallback to API
        if build_time is None and client:
            try:
                commit_data = client.get_commit(commit_sha)
                if (
                    commit_data
                    and "commit" in commit_data
                    and "author" in commit_data["commit"]
                ):
                    date_str = commit_data["commit"]["author"]["date"]
                    build_time = pd.to_datetime(date_str)
            except Exception as e:
                pass

    if build_time:
        # 0-23
        hour = build_time.hour
        weekday = build_time.weekday()  # 0=Mon, 6=Sun

        risk_score = 0.0
        if 0 <= hour <= 5:
            risk_score = 1.0  # Late night fatigue
        elif weekday >= 5:
            risk_score = 0.8  # Weekend work
        elif weekday == 4 and hour >= 16:
            risk_score = 0.9  # Friday afternoon rush
        else:
            risk_score = 0.1  # Normal working hours

        features["build_hour_risk_score"] = risk_score
        features["build_hour_sin"] = math.sin(2 * math.pi * hour / 24)
        features["build_hour_cos"] = math.cos(2 * math.pi * hour / 24)
    else:
        features["build_hour_risk_score"] = None
        features["build_hour_sin"] = None
        features["build_hour_cos"] = None
        if not missing_log:
            missing_log = (
                build_id,
                project_name,
                commit_sha,
                "Could not determine build time (Git & API failed)",
            )

    # 2. Src/Test Churn Ratio
    src_churn = float(row.get("git_diff_src_churn", 0) or 0)
    test_churn = float(row.get("git_diff_test_churn", 0) or 0)
    features["src_test_churn_ratio"] = test_churn / (src_churn + 1e-6)

    # 3. Description Length vs Churn
    desc_complexity = float(row.get("gh_description_complexity", 0) or 0)
    total_churn = src_churn + test_churn
    features["description_length_vs_churn"] = desc_complexity / (total_churn + 1e-6)

    # 4. Is New Contributor
    is_new_contributor = None
    if repo_dir and os.path.exists(repo_dir):
        try:
            cmd = ["git", "show", "-s", "--format=%an", commit_sha]
            author = subprocess.check_output(
                cmd, cwd=repo_dir, text=True, stderr=subprocess.DEVNULL
            ).strip()

            if author:
                cmd_log = [
                    "git",
                    "log",
                    "--author",
                    author,
                    "--reverse",
                    "--format=%ct",
                    "-n",
                    "1",
                ]
                first_commit_ts = subprocess.check_output(
                    cmd_log, cwd=repo_dir, text=True, stderr=subprocess.DEVNULL
                ).strip()

                if first_commit_ts:
                    first_ts = int(first_commit_ts)
                    current_ts = (
                        int(build_time.timestamp())
                        if build_time
                        else int(datetime.now().timestamp())
                    )
                    diff_days = (current_ts - first_ts) / (3600 * 24)
                    is_new_contributor = 1 if diff_days < 90 else 0
        except Exception:
            pass

    if is_new_contributor is None:
        if not missing_log:
            missing_log = (
                build_id,
                project_name,
                commit_sha,
                "Skipped is_new_contributor (No Git History)",
            )

    features["is_new_contributor"] = is_new_contributor

    # 5. Change Entropy
    commits_to_scan = []
    if not pd.isna(row.get("git_all_built_commits")):
        commits_to_scan = str(row["git_all_built_commits"]).split("#")
    else:
        commits_to_scan = [commit_sha]

    file_changes_map = {}  # file -> lines changed (add+del)
    entropy_calculated = False

    if repo_dir and os.path.exists(repo_dir):
        try:
            for sha in commits_to_scan:
                cmd_stat = ["git", "show", "--numstat", "--format=", sha]
                output = subprocess.check_output(
                    cmd_stat, cwd=repo_dir, text=True, stderr=subprocess.DEVNULL
                )
                for line in output.splitlines():
                    parts = line.split()
                    if len(parts) >= 3:
                        try:
                            added = int(parts[0]) if parts[0] != "-" else 0
                            deleted = int(parts[1]) if parts[1] != "-" else 0
                            filename = parts[2]
                            file_changes_map[filename] = (
                                file_changes_map.get(filename, 0) + added + deleted
                            )
                        except ValueError:
                            pass
            entropy_calculated = True
        except Exception:
            pass

    if not entropy_calculated and client:
        try:
            for sha in commits_to_scan:
                commit_data = client.get_commit(sha)
                if commit_data and "files" in commit_data:
                    for f in commit_data["files"]:
                        filename = f.get("filename")
                        changes = f.get("changes", 0)
                        if filename:
                            file_changes_map[filename] = (
                                file_changes_map.get(filename, 0) + changes
                            )
            entropy_calculated = True
        except Exception as e:
            if not missing_log:
                missing_log = (
                    build_id,
                    project_name,
                    commit_sha,
                    f"Entropy failed: {e}",
                )

    if entropy_calculated:
        changes_list = list(file_changes_map.values())
        features["change_entropy"] = calculate_entropy(changes_list)
    else:
        features["change_entropy"] = None

    return features, missing_log


def process_project_group(
    project_name, group, repos_dir, config, token_pool_adapter, cleanup=True
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

    # Initialize Client if needed
    client = None
    if GitHubAPIClient and token_pool_adapter:
        client = GitHubAPIClient(
            owner,
            repo,
            token_pool=token_pool_adapter,
            retry_count=config.get("github_api_retry_count", 3),
            retry_delay=config.get("github_api_retry_delay", 1.0),
        )

    results = []
    missing_logs = []

    for idx, row in group.iterrows():
        feats, log = get_risk_features(row, repo_dir, client)
        if log:
            missing_logs.append(log)

        # Merge features into row
        for k, v in feats.items():
            row[k] = v
        results.append(row)

    # Cleanup
    if cleanup and repo_dir and os.path.exists(repo_dir):
        try:
            shutil.rmtree(repo_dir)
        except:
            pass

    return pd.DataFrame(results), missing_logs


def process_batch(batch_df, config, token_pool_adapter, repos_dir, executor):
    grouped = batch_df.groupby("gh_project_name")
    project_groups = [group for _, group in grouped]

    results = []
    all_missing_logs = []

    future_to_project = {
        executor.submit(
            process_project_group,
            group["gh_project_name"].iloc[0],
            group,
            repos_dir,
            config,
            token_pool_adapter,
        ): i
        for i, group in enumerate(project_groups)
    }

    for future in as_completed(future_to_project):
        try:
            res_df, logs = future.result()
            results.append(res_df)
            if logs:
                all_missing_logs.extend(logs)
        except Exception as e:
            logger.error(f"Project failed: {e}")

    if results:
        return pd.concat(results), all_missing_logs
    return pd.DataFrame(), all_missing_logs


def merge_results(output_dir):
    logger.info(f"Starting merge process in {output_dir}")
    files = glob.glob(os.path.join(output_dir, "part_*.parquet"))
    if not files:
        logger.warning("No parquet files found to merge.")
        return

    try:
        con = duckdb.connect(database=":memory:")
        parquet_pattern = os.path.join(output_dir, "part_*.parquet")
        con.execute(
            f"CREATE OR REPLACE VIEW all_data AS SELECT * FROM read_parquet('{parquet_pattern}')"
        )

        merged_dir = os.path.join(output_dir, "merged_results")
        os.makedirs(merged_dir, exist_ok=True)
        output_path = os.path.join(merged_dir, "all_risk_features.csv")

        logger.info(f"Exporting all data to {output_path}")
        con.execute(
            f"COPY (SELECT * FROM all_data) TO '{output_path}' (HEADER, DELIMITER ',')"
        )
        logger.info(f"Successfully merged and saved all data to {output_path}")
    except Exception as e:
        logger.error(f"Merge failed: {e}")
    finally:
        try:
            con.close()
        except:
            pass


def main():
    # Configuration
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    CONFIG_PATH = os.path.join(BASE_DIR, "crawler_config.yml")

    parser = argparse.ArgumentParser(description="Risk Features Enrichment")
    parser.add_argument("--input", required=False, help="Input CSV file")
    parser.add_argument(
        "--output-dir", required=False, help="Output directory for Parquet files"
    )
    parser.add_argument(
        "--repos-dir", default="/tmp/repos_risk", help="Temp dir for repos"
    )
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--merge", action="store_true", help="Merge results at the end")
    parser.add_argument(
        "--no-mongo", action="store_true", help="Use in-memory token pool"
    )
    args = parser.parse_args()

    # Priority: Env Var > Args
    INPUT_CSV = os.environ.get("INPUT_FILE", args.input)
    OUTPUT_DIR = os.environ.get("OUTPUT_DIR", args.output_dir)
    BATCH_SIZE = int(os.environ.get("BATCH_SIZE", args.batch_size))
    ENABLE_MERGE = os.environ.get("ENABLE_MERGE", str(args.merge)).lower() in (
        "true",
        "1",
        "yes",
    )
    NO_MONGO = os.environ.get("NO_MONGO", str(args.no_mongo)).lower() in (
        "true",
        "1",
        "yes",
    )

    if not INPUT_CSV or not OUTPUT_DIR:
        logger.error("INPUT_FILE and OUTPUT_DIR must be provided via args or env vars.")
        sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(args.repos_dir, exist_ok=True)

    # Load Config & Setup Token Pool
    config = load_config(CONFIG_PATH)

    mongo_uri = os.environ.get(
        "MONGO_URI", config.get("mongo_uri", "mongodb://localhost:27017")
    )
    db_name = os.environ.get("DB_NAME", config.get("db_name", "ci_crawler"))

    token_pool = None
    if NO_MONGO or not MongoTokenPool:
        logger.info("Using InMemoryTokenPool")
        token_pool = InMemoryTokenPool(
            token_file=os.environ.get("TOKEN_FILE", "tokens.txt")
        )
    else:
        logger.info(f"Using MongoTokenPool at {mongo_uri}")
        token_pool = MongoTokenPool(mongo_uri, db_name)

    # Seed tokens
    tokens_env = os.environ.get("GITHUB_TOKENS", "")
    if tokens_env:
        tokens = [t.strip() for t in tokens_env.split(",") if t.strip()]
        token_pool.seed_tokens("github", tokens)
    elif "github_tokens" in config:
        token_pool.seed_tokens("github", config["github_tokens"])

    pool_adapter = GitHubTokenPoolAdapter(token_pool) if token_pool else None

    # Resume Capability: Check existing parquet files
    processed_ids = set()
    con = duckdb.connect(database=":memory:")
    parquet_files = glob.glob(os.path.join(OUTPUT_DIR, "part_*.parquet"))
    if parquet_files:
        logger.info(
            f"Found {len(parquet_files)} existing parts. Scanning for processed IDs..."
        )
        try:
            processed_ids_df = con.execute(
                f"SELECT tr_build_id FROM read_parquet('{os.path.join(OUTPUT_DIR, 'part_*.parquet')}')"
            ).fetchdf()
            processed_ids = set(processed_ids_df["tr_build_id"])
            logger.info(f"Found {len(processed_ids)} processed rows.")
        except Exception as e:
            logger.warning(f"Could not read existing parquet files: {e}")

    logger.info(f"Reading {INPUT_CSV}...")
    df_source = pd.read_csv(INPUT_CSV)

    # Initialize new columns
    new_cols = [
        "is_new_contributor",
        "build_hour_risk_score",
        "build_hour_sin",
        "build_hour_cos",
        "src_test_churn_ratio",
        "change_entropy",
        "description_length_vs_churn",
    ]
    for col in new_cols:
        if col not in df_source.columns:
            df_source[col] = None

    # Filter source
    df_to_process = df_source[~df_source["tr_build_id"].isin(processed_ids)]
    logger.info(f"Rows to process: {len(df_to_process)} (Total: {len(df_source)})")

    if df_to_process.empty:
        logger.info("All done!")
        if ENABLE_MERGE:
            merge_results(OUTPUT_DIR)
        return

    # Sort by project to optimize cloning
    df_to_process = df_to_process.sort_values(by="gh_project_name")

    # Batch Processing
    total_chunks = (len(df_to_process) // BATCH_SIZE) + 1
    chunks = [
        df_to_process[i : i + BATCH_SIZE]
        for i in range(0, len(df_to_process), BATCH_SIZE)
    ]

    max_workers = config.get("max_workers", 5)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i, chunk in tqdm(
            enumerate(chunks), total=total_chunks, desc="Processing Batches"
        ):
            logger.info(f"Starting batch {i+1}/{len(chunks)}: rows={len(chunk)}")

            try:
                if isinstance(token_pool, InMemoryTokenPool):
                    token_pool.reload_from_file()

                df_enriched, logs = process_batch(
                    chunk.copy(), config, pool_adapter, args.repos_dir, executor
                )

                # Save logs
                if logs:
                    log_path = os.path.join(
                        os.path.dirname(OUTPUT_DIR), "missing_commits_log.csv"
                    )
                    log_df = pd.DataFrame(
                        logs,
                        columns=[
                            "tr_build_id",
                            "gh_project_name",
                            "commit_sha",
                            "reason",
                        ],
                    )
                    if os.path.exists(log_path):
                        log_df.to_csv(log_path, mode="a", header=False, index=False)
                    else:
                        log_df.to_csv(log_path, index=False)

                # Save Parquet part
                if not df_enriched.empty:
                    part_filename = (
                        f"part_{int(datetime.now().timestamp())}_{i}.parquet"
                    )
                    part_path = os.path.join(OUTPUT_DIR, part_filename)
                    df_enriched.to_parquet(part_path, index=False)
                    logger.info(f"Saved batch to {part_path}")

            except Exception as e:
                logger.error(f"Batch {i} failed: {e}")

    if ENABLE_MERGE:
        merge_results(OUTPUT_DIR)


if __name__ == "__main__":
    main()
