import os
import sys
import logging
import pandas as pd
import duckdb
import yaml
import subprocess
import re
import glob
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from datetime import datetime, timedelta
import gcsfs

# Add parent directory to path to import github_api_client and token_pool
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from github_api_client import GitHubAPIClient
from token_pool import MongoTokenPool, GitHubTokenPoolAdapter

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_config(config_path):
    logger.info(f"Loading config from {config_path}")
    try:
        # Support GCS path
        if config_path.startswith("gs://"):
            fs = gcsfs.GCSFileSystem()
            with fs.open(config_path, "r") as f:
                cfg = yaml.safe_load(f)
        else:
            with open(config_path, "r") as f:
                cfg = yaml.safe_load(f)

        logger.debug(
            f"Loaded config keys: {list(cfg.keys()) if isinstance(cfg, dict) else 'N/A'}"
        )
        return cfg
    except Exception as e:
        logger.error(f"Failed to load config from {config_path}: {e}")
        # Fallback to empty config if optional, or raise
        # For Cloud Run, we might rely on Env Vars entirely
        logger.warning(
            "Config file load failed, proceeding with Env Vars if available."
        )
        return {}


def parse_linked_issues(pr_body):
    logger.debug(
        f"parse_linked_issues called; body_len={len(pr_body) if pr_body else 0}"
    )
    if not pr_body:
        return 0
    keywords = [
        "close",
        "closes",
        "closed",
        "fix",
        "fixes",
        "fixed",
        "resolve",
        "resolves",
        "resolved",
    ]
    pattern = r"(" + "|".join(keywords) + r")\s+#(\d+)"
    matches = re.findall(pattern, pr_body, re.IGNORECASE)
    logger.debug(f"parse_linked_issues found {len(matches)} matches")
    return len(matches)


def calculate_sentiment(text):
    logger.debug(f"calculate_sentiment called; text_len={len(text) if text else 0}")
    if not text:
        return 0
    text = text.lower()
    positive = [
        "good",
        "great",
        "awesome",
        "excellent",
        "lgtm",
        "looks good",
        "perfect",
        "nice",
        "thank",
        "approved",
    ]
    negative = [
        "bad",
        "wrong",
        "error",
        "bug",
        "fix",
        "issue",
        "problem",
        "change",
        "concern",
        "reject",
        "request changes",
    ]
    score = 0
    for w in positive:
        if w in text:
            score += 1
    for w in negative:
        if w in text:
            score -= 1
    logger.debug(f"calculate_sentiment score={score}")
    return score


def clone_repo(repo_url, clone_dir):
    logger.info(
        f"Cloning repo {repo_url} into {clone_dir} (exists={os.path.exists(clone_dir)})"
    )
    if not os.path.exists(clone_dir):
        try:
            subprocess.check_call(
                ["git", "clone", repo_url, clone_dir],
                stderr=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
            )
            logger.info(f"Successfully cloned {repo_url}")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to clone {repo_url}: {e}")
            return False
    logger.info(f"Repo {repo_url} already exists at {clone_dir}")
    return True


def get_pr_features(client, pr_number, row):
    logger.info(f"Fetching PR features for PR #{pr_number}")
    features = {}
    try:
        pr_data = client.get_pull_request(pr_number)
        if pr_data:
            requested_reviewers = pr_data.get("requested_reviewers", [])
            features["gh_num_reviewers"] = len(requested_reviewers)

            body = pr_data.get("body", "")
            features["gh_linked_issues_count"] = parse_linked_issues(body)

            labels = pr_data.get("labels", [])
            has_bug = any(
                "bug" in lbl["name"].lower() or "fix" in lbl["name"].lower()
                for lbl in labels
            )
            features["gh_has_bug_label"] = has_bug
            logger.debug(
                f"PR {pr_number}: reviewers={len(requested_reviewers)}, has_bug_label={has_bug}"
            )

            reviews = client.get_pull_reviews(pr_number)
            approvals = [r for r in reviews if r["state"] == "APPROVED"]
            features["gh_num_approvals"] = len(approvals)
            logger.debug(
                f"PR {pr_number}: reviews={len(reviews)}, approvals={len(approvals)}"
            )

            total_sentiment = 0
            review_count = 0
            for r in reviews:
                body = r.get("body", "")
                if body:
                    total_sentiment += calculate_sentiment(body)
                    review_count += 1
            features["gh_review_sentiment"] = (
                total_sentiment / review_count if review_count > 0 else 0
            )
            logger.debug(
                f"PR {pr_number}: review_sentiment={features['gh_review_sentiment']}"
            )

            try:
                pr_created_at = pd.to_datetime(row["gh_pr_created_at"])
                if reviews:
                    reviews.sort(key=lambda x: x["submitted_at"])
                    first_review_at = pd.to_datetime(reviews[0]["submitted_at"])
                    diff = (first_review_at - pr_created_at).total_seconds() / 3600
                    features["gh_time_to_first_review"] = max(0, diff)
                    logger.debug(
                        f"PR {pr_number}: time_to_first_review={features['gh_time_to_first_review']} hours"
                    )
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Error fetching PR {pr_number}: {e}")
    return features


def get_commit_features(client, commit_sha, git_all_built_commits, repo_dir, row):
    logger.info(f"Fetching commit features for commit {commit_sha}")
    features = {}
    missing_log = None

    # Get trigger commit date for time-based analysis (3 months prior)
    commit_date = None
    try:
        cmd = ["git", "show", "-s", "--format=%ct", commit_sha]
        ts = subprocess.check_output(
            cmd, cwd=repo_dir, text=True, stderr=subprocess.DEVNULL
        ).strip()
        if ts:
            commit_date = datetime.fromtimestamp(int(ts))
            logger.debug(f"Commit {commit_sha} date: {commit_date.isoformat()}")
    except Exception:
        pass

    touched_files = set()
    build_authors_name = set()

    logger.debug(
        f"Commit {commit_sha}: looking up {len(git_all_built_commits)} built commits for touched files"
    )
    for sha in git_all_built_commits:
        try:
            cmd = ["git", "show", "--name-only", "--format=%an", sha]
            output = subprocess.check_output(
                cmd, cwd=repo_dir, text=True, stderr=subprocess.DEVNULL
            ).splitlines()
            if output:
                author_name = output[0]
                build_authors_name.add(author_name)
                for f in output[1:]:
                    if f.strip():
                        touched_files.add(f.strip())
                logger.debug(
                    f"Commit {commit_sha}: git show {sha} found author {author_name} and {len(output)-1} files"
                )
        except subprocess.CalledProcessError:
            # Fallback to API
            try:
                commit = client.get_commit(sha)
                if commit:
                    if "commit" in commit and "author" in commit["commit"]:
                        build_authors_name.add(commit["commit"]["author"]["name"])
                    if "files" in commit:
                        for f in commit["files"]:
                            touched_files.add(f["filename"])
                else:
                    missing_log = f"{row.get('tr_build_id', '')},{row.get('gh_project_name', '')},{sha},Commit not found"
            except Exception as e:
                logger.error(f"Error fetching commit {sha}: {e}")

    logger.debug(
        f"Commit {commit_sha}: total touched files discovered {len(touched_files)}"
    )
    if not touched_files:
        return features, missing_log

    # File Change Frequency
    num_touched_files = len(touched_files)
    if num_touched_files > 0 and "gh_num_commits_on_files_touched" in row:
        commits_on_files = row["gh_num_commits_on_files_touched"]
        if not pd.isna(commits_on_files):
            features["file_change_frequency"] = commits_on_files / num_touched_files

    # Author Ownership
    total_commits_scanned = 0
    owned_commits_count = 0
    target_authors = build_authors_name

    for filename in touched_files:
        full_path = os.path.join(repo_dir, filename)
        git_success = False
        if os.path.exists(full_path):
            try:
                if commit_date:
                    since_date = commit_date - timedelta(days=90)
                    cmd = [
                        "git",
                        "log",
                        f"--since={since_date.isoformat()}",
                        f"--until={commit_date.isoformat()}",
                        "--pretty=format:%an",
                        filename,
                    ]
                else:
                    cmd = ["git", "log", "-n", "50", "--pretty=format:%an", filename]
                output = subprocess.check_output(
                    cmd, cwd=repo_dir, text=True, stderr=subprocess.DEVNULL
                )
                for author in output.splitlines():
                    total_commits_scanned += 1
                    if author in target_authors:
                        owned_commits_count += 1
                git_success = True
            except subprocess.CalledProcessError:
                pass

        if not git_success:
            # Fallback to API
            try:
                params = {"path": filename}
                if commit_date:
                    since_date = commit_date - timedelta(days=90)
                    params["since"] = since_date.isoformat()
                    params["until"] = commit_date.isoformat()
                    commits = client.get(
                        f"/repos/{client.owner}/{client.repo}/commits",
                        params=params,
                        paginate=True,
                    )
                else:
                    params["per_page"] = 50
                    commits = client.get(
                        f"/repos/{client.owner}/{client.repo}/commits", params=params
                    )

                if isinstance(commits, list):
                    for c in commits:
                        total_commits_scanned += 1
                        if "commit" in c and "author" in c["commit"]:
                            author_name = c["commit"]["author"]["name"]
                            if author_name in target_authors:
                                owned_commits_count += 1
            except Exception:
                pass

    logger.debug(
        f"Commit {commit_sha}: total_commits_scanned={total_commits_scanned}, owned_commits_count={owned_commits_count}"
    )
    if total_commits_scanned > 0:
        features["author_ownership"] = owned_commits_count / total_commits_scanned
        logger.info(
            f"Commit {commit_sha}: author_ownership={features['author_ownership']}"
        )

    if missing_log:
        logger.warning(f"Commit {commit_sha} missing log: {missing_log}")
    return features, missing_log


# --- Pipeline Logic ---


def process_project_group(project_name, group, config, token_pool_adapter, repos_dir):
    """
    Process a group of rows belonging to the same project.
    This runs inside a thread or process, but since we use ThreadPool for API calls,
    we might want to run this sequentially per project to avoid git lock issues,
    or use a lock for git operations.
    """
    if pd.isna(project_name):
        return group, []

    logger.info(f"[{project_name}] Processing {len(group)} rows")

    owner, repo = project_name.split("/")
    client = GitHubAPIClient(
        owner,
        repo,
        token_pool=token_pool_adapter,
        retry_count=config.get("github_api_retry_count", 5),
        retry_delay=config.get("github_api_retry_delay", 1.0),
    )

    repo_url = f"https://github.com/{project_name}.git"
    repo_dir = os.path.join(repos_dir, f"{owner}_{repo}")

    # Clone repo (Thread-safe check needed if multiple threads access same repo?
    # But we group by project, so one thread per project usually.
    # If we have multiple batches processing same project, we might have issues.
    # Ideally, we sort input by project so batches are contiguous.)
    clone_repo(repo_url, repo_dir)

    missing_logs = []

    # 1. PR Features
    unique_prs = group[group["gh_is_pr"]]["gh_pull_req_num"].unique()
    logger.info(f"[{project_name}] Found {len(unique_prs)} unique PRs to fetch")
    pr_features_cache = {}

    # We can parallelize PR fetching here
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_pr = {
            executor.submit(
                get_pr_features,
                client,
                int(pr_num),
                group[group["gh_pull_req_num"] == pr_num].iloc[0],
            ): int(pr_num)
            for pr_num in unique_prs
            if not pd.isna(pr_num)
        }
        for future in as_completed(future_to_pr):
            pr_num = future_to_pr[future]
            try:
                pr_features_cache[pr_num] = future.result()
            except Exception as e:
                logger.error(f"PR fetch failed: {e}")

    # 2. Commit Features
    unique_commits = group["git_trigger_commit"].unique()
    logger.info(f"[{project_name}] Found {len(unique_commits)} unique Commits to fetch")
    commit_features_cache = {}

    # We can parallelize Commit fetching here
    # Note: git operations in get_commit_features might conflict if running in parallel on same repo?
    # git show is read-only, should be fine.
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_sha = {}
        for commit_sha in unique_commits:
            if pd.isna(commit_sha):
                continue
            rep_row = group[group["git_trigger_commit"] == commit_sha].iloc[0]
            git_all_built_commits = (
                str(rep_row["git_all_built_commits"]).split("#")
                if not pd.isna(rep_row["git_all_built_commits"])
                else []
            )

            future = executor.submit(
                get_commit_features,
                client,
                commit_sha,
                git_all_built_commits,
                repo_dir,
                rep_row,
            )
            future_to_sha[future] = commit_sha

        for future in as_completed(future_to_sha):
            commit_sha = future_to_sha[future]
            try:
                feats, log = future.result()
                commit_features_cache[commit_sha] = feats
                if log:
                    missing_logs.append(log)
            except Exception as e:
                logger.error(f"Commit fetch failed: {e}")

    # 3. Apply
    applied_pr_features = 0
    applied_commit_features = 0
    for index, row in group.iterrows():
        # Apply PR features
        if row["gh_is_pr"] and not pd.isna(row["gh_pull_req_num"]):
            pr_num = int(row["gh_pull_req_num"])
            if pr_num in pr_features_cache:
                for k, v in pr_features_cache[pr_num].items():
                    group.at[index, k] = v
                applied_pr_features += 1

        # Apply Commit features
        commit_sha = row["git_trigger_commit"]
        if not pd.isna(commit_sha) and commit_sha in commit_features_cache:
            for k, v in commit_features_cache[commit_sha].items():
                group.at[index, k] = v
            applied_commit_features += 1

    logger.info(
        f"[{project_name}] Applied PR features: {applied_pr_features}, Commit features: {applied_commit_features}"
    )
    return group, missing_logs


def process_batch(batch_df, config, token_pool_adapter, repos_dir):
    logger.info(f"Processing batch: rows={len(batch_df)}")
    # Group by project within the batch
    grouped = batch_df.groupby("gh_project_name")
    try:
        project_count = len(list(grouped))
    except Exception:
        project_count = None
    logger.info(f"Batch contains {project_count} projects to process")
    results = []
    all_missing_logs = []

    # Process each project in the batch
    # We could parallelize this loop too, but let's keep it simple:
    # Parallelism is inside process_project_group (fetching PRs/Commits).
    for project_name, group in grouped:
        processed_group, logs = process_project_group(
            project_name, group.copy(), config, token_pool_adapter, repos_dir
        )
        results.append(processed_group)
        all_missing_logs.extend(logs)

    return pd.concat(results), all_missing_logs


def merge_results(output_dir):
    """
    Merge all parquet files in output_dir and save as per-project CSVs.
    """
    logger.info(f"Starting merge process in {output_dir}")

    # List all parquet files
    if output_dir.startswith("gs://"):
        fs = gcsfs.GCSFileSystem()
        # fs.glob returns paths without gs:// prefix usually
        files = fs.glob(os.path.join(output_dir, "part_*.parquet"))
        files = [f"gs://{f}" if not f.startswith("gs://") else f for f in files]
    else:
        files = glob.glob(os.path.join(output_dir, "part_*.parquet"))

    if not files:
        logger.warning("No parquet files found to merge.")
        return

    logger.info(f"Found {len(files)} parquet files. Reading...")

    # Read all parquet files
    # If dataset is huge, this might OOM.
    # Better approach for huge data: Use DuckDB to aggregate and write.
    try:
        # Use DuckDB to read and group
        con = duckdb.connect(database=":memory:")

        # Create a view of all parquet files
        # DuckDB supports reading list of files or glob
        # For GCS, we might need to register httpfs/gcsfs if not implicit,
        # but pandas read is safer for now if we don't want to deal with duckdb extensions in docker.
        # Let's stick to pandas for simplicity if memory allows (500k rows is fine for 4GB RAM).

        dfs = []
        for f in tqdm(files, desc="Reading Parquet"):
            dfs.append(pd.read_parquet(f))

        full_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Total rows loaded: {len(full_df)}")

        # Group by project
        grouped = full_df.groupby("gh_project_name")

        # Output directory for merged files
        merged_dir = os.path.join(output_dir, "merged_results")
        if output_dir.startswith("gs://"):
            # GCS doesn't need makedirs
            pass
        else:
            os.makedirs(merged_dir, exist_ok=True)

        for project_name, group in tqdm(grouped, desc="Saving Project CSVs"):
            if pd.isna(project_name):
                continue

            # Sanitize project name for filename (owner/repo -> owner_repo)
            safe_name = str(project_name).replace("/", "_")
            filename = f"{safe_name}.csv"
            output_path = os.path.join(merged_dir, filename)

            group.to_csv(output_path, index=False)

        logger.info(
            f"Successfully merged and saved {len(grouped)} project CSVs to {merged_dir}"
        )

    except Exception as e:
        logger.error(f"Merge failed: {e}")


def main():
    # Configuration
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    CONFIG_PATH = os.path.join(BASE_DIR, "crawler_config.yml")

    parser = argparse.ArgumentParser(
        description="Optimized GitHub Feature Enrichment Pipeline"
    )
    parser.add_argument("--input", required=False, help="Path to input CSV file")
    parser.add_argument(
        "--output-dir", required=False, help="Directory to save output Parquet files"
    )
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Batch size for processing"
    )
    parser.add_argument(
        "--merge",
        action="store_true",
        help="Merge results into per-project CSVs at the end",
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

    if not INPUT_CSV or not OUTPUT_DIR:
        logger.error("INPUT_FILE and OUTPUT_DIR must be provided via args or env vars.")
        sys.exit(1)

    # Ensure output dir exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Load Config & Setup Token Pool
    # Load Config & Setup Token Pool
    # Allow CONFIG_PATH env var
    config_path_env = os.environ.get("CONFIG_PATH", CONFIG_PATH)
    config = load_config(config_path_env)

    mongo_uri = os.environ.get(
        "MONGO_URI", config.get("mongo_uri", "mongodb://localhost:27017")
    )
    db_name = os.environ.get("DB_NAME", config.get("db_name", "ci_crawler"))

    token_pool = MongoTokenPool(mongo_uri, db_name)

    # Load tokens from Env Var (comma separated) or Config
    tokens_env = os.environ.get("GITHUB_TOKENS", "")
    if tokens_env:
        tokens = [t.strip() for t in tokens_env.split(",") if t.strip()]
    else:
        tokens = config.get("github_tokens", [])

    if tokens:
        token_pool.seed_tokens("github", tokens)
    pool_adapter = GitHubTokenPoolAdapter(token_pool)

    # Use a local temporary directory for cloning repos
    # We cannot clone to GCS directly.
    repos_dir = os.environ.get("REPOS_DIR", "/tmp/repos")
    os.makedirs(repos_dir, exist_ok=True)

    # 1. Load Source
    logger.info(f"Loading source data from {INPUT_CSV}")
    df_source = pd.read_csv(INPUT_CSV)

    # Initialize new columns
    new_cols = [
        "gh_num_reviewers",
        "gh_num_approvals",
        "gh_time_to_first_review",
        "gh_review_sentiment",
        "gh_linked_issues_count",
        "gh_has_bug_label",
        "file_change_frequency",
        "author_ownership",
    ]
    for col in new_cols:
        if col not in df_source.columns:
            df_source[col] = None

    # 2. Check processed IDs using DuckDB
    # We assume 'tr_build_id' is unique. If not, create a composite key or index.
    # Let's assume tr_build_id is unique for now.

    con = duckdb.connect(
        database=":memory:"
    )  # Use in-memory duckdb to scan parquet files

    # Check if we have any parquet files
    # Check if we have any parquet files
    processed_ids = set()

    if OUTPUT_DIR.startswith("gs://"):
        fs = gcsfs.GCSFileSystem()
        try:
            # List files in GCS
            # glob might return full paths like bucket/path/file.parquet
            parquet_files = fs.glob(os.path.join(OUTPUT_DIR, "part_*.parquet"))
            if parquet_files:
                logger.info(
                    f"Found {len(parquet_files)} existing parts in GCS. Scanning for processed IDs..."
                )
                # Use pandas to read GCS files directly
                dfs = []
                for pf in parquet_files:
                    # fs.glob returns path without gs:// prefix usually, but read_parquet needs it or valid url
                    # gcsfs paths are usually 'bucket/path'. Add gs:// if missing.
                    path = pf if pf.startswith("gs://") else f"gs://{pf}"
                    try:
                        df_ids = pd.read_parquet(path, columns=["tr_build_id"])
                        dfs.append(df_ids)
                    except Exception as e:
                        logger.warning(f"Failed to read {path}: {e}")

                if dfs:
                    processed_ids_df = pd.concat(dfs)
                    processed_ids = set(processed_ids_df["tr_build_id"])
                    logger.info(f"Found {len(processed_ids)} processed rows.")
        except Exception as e:
            logger.warning(f"Could not read existing parquet files from GCS: {e}")

    else:
        parquet_files = glob.glob(os.path.join(OUTPUT_DIR, "part_*.parquet"))

        if parquet_files:
            logger.info(
                f"Found {len(parquet_files)} existing parts. Scanning for processed IDs..."
            )
            # DuckDB can read all parquet files at once: read_parquet('folder/*.parquet')
            try:
                processed_ids_df = con.execute(
                    f"SELECT tr_build_id FROM read_parquet('{os.path.join(OUTPUT_DIR, 'part_*.parquet')}')"
                ).fetchdf()
                processed_ids = set(processed_ids_df["tr_build_id"])
                logger.info(f"Found {len(processed_ids)} processed rows.")
            except Exception as e:
                logger.warning(f"Could not read existing parquet files: {e}")

    # Filter source
    df_to_process = df_source[~df_source["tr_build_id"].isin(processed_ids)]
    logger.info(f"Rows to process: {len(df_to_process)} (Total: {len(df_source)})")

    if df_to_process.empty:
        logger.info("All done!")
        return

    # Sort by project to optimize cloning
    df_to_process = df_to_process.sort_values(by="gh_project_name")

    # 3. Batch Processing
    total_chunks = (len(df_to_process) // BATCH_SIZE) + 1
    chunks = [
        df_to_process[i : i + BATCH_SIZE]
        for i in range(0, len(df_to_process), BATCH_SIZE)
    ]

    if OUTPUT_DIR.startswith("gs://"):
        missing_commits_log_path = os.path.join(OUTPUT_DIR, "missing_commits_log.csv")
    else:
        missing_commits_log_path = os.path.join(
            os.path.dirname(OUTPUT_DIR), "missing_commits_log.csv"
        )

    for i, chunk in tqdm(
        enumerate(chunks), total=total_chunks, desc="Processing Batches"
    ):
        logger.info(f"Starting batch {i+1}/{len(chunks)}: rows={len(chunk)}")
        try:
            # Process
            df_enriched, logs = process_batch(
                chunk.copy(), config, pool_adapter, repos_dir
            )

            # Save logs
            if logs:
                # Append to log file. For GCS, appending is hard. We might just write a new log file per batch?
                # Or just log to stdout/stderr since Cloud Logging captures it.
                # But user wants a file.
                # Let's write a batch-specific log file to avoid concurrency issues/appending issues on GCS.
                batch_log_filename = (
                    f"missing_commits_log_{int(datetime.now().timestamp())}_{i}.csv"
                )
                batch_log_path = os.path.join(OUTPUT_DIR, batch_log_filename)

                try:
                    # Create a DF and save to csv/parquet
                    log_df = pd.DataFrame(logs, columns=["log_entry"])
                    # If GCS, use string buffer or just pandas
                    if OUTPUT_DIR.startswith("gs://"):
                        # pandas to_csv supports gs://
                        log_df.to_csv(batch_log_path, index=False, header=False)
                    else:
                        with open(missing_commits_log_path, "a") as f:
                            for log in logs:
                                f.write(log + "\n")
                except Exception as e:
                    logger.error(f"Failed to write logs: {e}")

            # Save Batch to Parquet
            # Use a timestamp or batch index in filename
            batch_filename = f"part_{int(datetime.now().timestamp())}_{i}.parquet"
            output_path = os.path.join(OUTPUT_DIR, batch_filename)
            df_enriched.to_parquet(output_path, index=False)
            logger.info(
                f"Saved batch {i+1} file {output_path} (rows={len(df_enriched)})"
            )

        except Exception as e:
            logger.error(f"Error processing batch {i}: {e}")
            # Continue to next batch? Yes.
            continue

    if ENABLE_MERGE:
        merge_results(OUTPUT_DIR)

    logger.info("Pipeline finished.")


if __name__ == "__main__":
    main()
