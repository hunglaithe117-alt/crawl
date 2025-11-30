import os
import sys
import logging
import pandas as pd
import duckdb
import yaml
import glob
import argparse
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from datetime import datetime

# Add parent directory to path to import shared clients
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from github_api_client import GitHubAPIClient
from token_pool import TokenManager


class TqdmLoggingHandler(logging.Handler):
    """Logging handler that writes messages through tqdm to avoid interfering with the progress bar."""

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


def load_config(config_path):
    logger.info(f"Loading config from {config_path}")
    try:
        with open(config_path, "r") as f:
            cfg = yaml.safe_load(f)

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
                    if "travis_tokens" in tokens_config:
                        cfg.setdefault("travis_tokens", []).extend(
                            tokens_config["travis_tokens"]
                        )
                logger.info(f"Loaded and merged tokens from {tokens_path}")
            except Exception as e:
                logger.warning(f"Failed to load tokens from {tokens_path}: {e}")

        logger.debug(
            f"Loaded config keys: {list(cfg.keys()) if isinstance(cfg, dict) else 'N/A'}"
        )
        return cfg
    except Exception as e:
        logger.error(f"Failed to load config from {config_path}: {e}")
        logger.warning("Config file load failed, proceeding with Env Vars if available.")
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


def get_pr_features(client, pr_number, row):
    logger.info(f"Fetching PR features for PR #{pr_number} (GraphQL)")
    features = {}

    query = """
    query ($owner: String!, $repo: String!, $pr_number: Int!) {
      repository(owner: $owner, name: $repo) {
        pullRequest(number: $pr_number) {
          title
          body
          state
          createdAt
          closedAt
          mergedAt
          labels(first: 100) {
            nodes {
              name
            }
          }
          reviewRequests(first: 100) {
            nodes {
              requestedReviewer {
                ... on User {
                  login
                }
              }
            }
          }
          reviews(first: 100) {
            nodes {
              state
              body
              submittedAt
              author {
                login
              }
            }
          }
        }
      }
    }
    """

    try:
        data = client.graphql(
            query,
            {"owner": client.owner, "repo": client.repo, "pr_number": int(pr_number)},
        )
        pr_data = data.get("data", {}).get("repository", {}).get("pullRequest")

        if pr_data:
            requested_reviewers = pr_data.get("reviewRequests", {}).get("nodes", [])
            features["gh_num_reviewers"] = len(requested_reviewers)

            body = pr_data.get("body", "")
            features["gh_linked_issues_count"] = parse_linked_issues(body)

            labels = pr_data.get("labels", {}).get("nodes", [])
            has_bug = any(
                "bug" in lbl["name"].lower() or "fix" in lbl["name"].lower()
                for lbl in labels
            )
            features["gh_has_bug_label"] = has_bug
            logger.debug(
                f"PR {pr_number}: reviewers={len(requested_reviewers)}, has_bug_label={has_bug}"
            )

            reviews = pr_data.get("reviews", {}).get("nodes", [])
            approvals = [r for r in reviews if r["state"] == "APPROVED"]
            features["gh_num_approvals"] = len(approvals)
            logger.debug(
                f"PR {pr_number}: reviews={len(reviews)}, approvals={len(approvals)}"
            )

            total_sentiment = 0
            review_count = 0
            for r in reviews:
                r_body = r.get("body", "")
                if r_body:
                    total_sentiment += calculate_sentiment(r_body)
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
                    valid_reviews = [r for r in reviews if r.get("submittedAt")]
                    if valid_reviews:
                        valid_reviews.sort(key=lambda x: x["submittedAt"])
                        first_review_at = pd.to_datetime(
                            valid_reviews[0]["submittedAt"]
                        )

                        if pr_created_at.tz is None:
                            pr_created_at = pr_created_at.tz_localize("UTC")
                        if first_review_at.tz is None:
                            first_review_at = first_review_at.tz_localize("UTC")

                        diff = (first_review_at - pr_created_at).total_seconds() / 3600
                        features["gh_time_to_first_review"] = max(0, diff)
                        logger.debug(
                            f"PR {pr_number}: time_to_first_review={features['gh_time_to_first_review']} hours"
                        )
            except Exception as e:
                logger.debug(f"Time calc error: {e}")
    except Exception as e:
        logger.error(f"Error fetching PR {pr_number}: {e}")
    return features


def prepare_project_context(project_name, config, token_manager):
    owner, repo = project_name.split("/")
    return {
        "client": GitHubAPIClient(
            owner,
            repo,
            token_manager=token_manager,
            retry_count=config.get("github_api_retry_count", 5),
            retry_delay=config.get("github_api_retry_delay", 1.0),
            cache_dir=config.get("github_api_cache_dir", None),
        )
    }


def apply_pr_feature_cache(group, pr_features_cache):
    applied_pr_features = 0

    if pr_features_cache:
        pr_df = pd.DataFrame.from_dict(pr_features_cache, orient="index")
        group["_tmp_pr_key"] = group["gh_pull_req_num"].fillna(-1).astype(int)

        for col in pr_df.columns:
            mapped = group["_tmp_pr_key"].map(pr_df[col])
            if col not in group.columns:
                group[col] = None
            group[col] = group[col].fillna(mapped).infer_objects(copy=False)

        if "gh_num_reviewers" in pr_df.columns:
            applied_pr_features = group["_tmp_pr_key"].isin(pr_df.index).sum()

        group.drop(columns=["_tmp_pr_key"], inplace=True)

    return group, applied_pr_features


def process_batch(batch_df, config, token_manager, executor):
    logger.info(f"Processing batch: rows={len(batch_df)}")
    project_groups = []
    for name, group in batch_df.groupby("gh_project_name"):
        if pd.isna(name):
            logger.warning("Skipping rows with missing project name")
            continue
        project_groups.append((name, group))
    logger.info(f"Batch contains {len(project_groups)} projects to process")

    project_contexts = {
        name: prepare_project_context(name, config, token_manager)
        for name, _ in project_groups
    }

    pr_futures = {}

    for project_name, group in project_groups:
        ctx = project_contexts[project_name]
        client = ctx["client"]

        unique_prs = (
            group[group["gh_is_pr"]]["gh_pull_req_num"].dropna().unique().tolist()
        )
        logger.info(f"[{project_name}] Found {len(unique_prs)} unique PRs to fetch")
        for pr_num in unique_prs:
            representative_row = group[group["gh_pull_req_num"] == pr_num].iloc[0]
            future = executor.submit(
                get_pr_features, client, int(pr_num), representative_row
            )
            pr_futures[future] = (project_name, int(pr_num))

    pr_features_cache = defaultdict(dict)
    for future in as_completed(pr_futures):
        project_name, pr_num = pr_futures[future]
        try:
            pr_features_cache[project_name][pr_num] = future.result()
        except Exception as e:
            logger.error(f"[{project_name}] PR fetch failed: {e}")

    results = []
    for project_name, group in project_groups:
        processed_group, applied_pr = apply_pr_feature_cache(
            group.copy(), pr_features_cache.get(project_name, {})
        )
        logger.info(f"[{project_name}] Applied PR features: {applied_pr} (rows)")
        results.append(processed_group)

    if results:
        return pd.concat(results)
    return pd.DataFrame()


def merge_results(output_dir):
    logger.info(f"Starting merge process in {output_dir}")
    files = glob.glob(os.path.join(output_dir, "part_*.parquet"))

    if not files:
        logger.warning("No parquet files found to merge.")
        return

    logger.info(f"Found {len(files)} parquet files. Processing with DuckDB...")

    try:
        con = duckdb.connect(database=":memory:")
        parquet_pattern = os.path.join(output_dir, "part_*.parquet")
        # union_by_name helps when earlier parts have columns that are all NULL
        # (DuckDB would otherwise fix the schema from the first file only).
        con.execute(
            f"CREATE OR REPLACE VIEW all_data AS SELECT * FROM read_parquet('{parquet_pattern}', union_by_name=true)"
        )

        merged_dir = os.path.join(output_dir, "merged_results")
        os.makedirs(merged_dir, exist_ok=True)

        output_path = os.path.join(merged_dir, "all_enriched_data.csv")
        logger.info(f"Exporting all data to {output_path}")

        query = f"""
            COPY (
                SELECT * FROM all_data 
            ) TO '{output_path}' (HEADER, DELIMITER ',')
        """
        con.execute(query)

        logger.info(f"Successfully merged and saved all data to {output_path}")

    except Exception as e:
        logger.error(f"Merge failed: {e}")
    finally:
        try:
            con.close()
        except Exception:
            pass


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "crawler_config.yml")

    parser = argparse.ArgumentParser(
        description="GitHub PR feature enrichment pipeline (PR-only)."
    )
    parser.add_argument("--input", required=True, help="Path to input CSV file")
    parser.add_argument(
        "--output-dir", required=True, help="Directory to save output Parquet files"
    )
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Batch size for processing"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    parser.add_argument(
        "--merge",
        action="store_true",
        help="Merge results into per-project CSVs at the end",
    )
    args = parser.parse_args()

    try:
        level = getattr(logging, args.log_level.upper(), logging.INFO)
        logging.getLogger().setLevel(level)
        logger.debug(f"Logging level set to {args.log_level}")
        active_handlers = [type(h).__name__ for h in logging.getLogger().handlers]
        logger.info(f"Active logging handlers: {active_handlers}")
    except Exception:
        pass

    input_csv = args.input
    output_dir = args.output_dir
    batch_size = args.batch_size
    enable_merge = args.merge

    if not input_csv or not output_dir:
        logger.error("INPUT_FILE and OUTPUT_DIR must be provided via args.")
        sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)

    config = load_config(config_path)
    tokens = config.get("github_tokens", [])
    mongo_uri = config.get("mongo_uri")

    if mongo_uri:
        try:
            from token_pool import MongoTokenManager

            db_name = config.get("db_name", "ci_crawler")
            logger.info(f"Initializing MongoTokenManager with DB {db_name}")
            token_manager = MongoTokenManager(mongo_uri, db_name)

            if tokens:
                logger.info(f"Seeding {len(tokens)} tokens into MongoDB...")
                for t in tokens:
                    token_manager.add_token("github", t)
        except Exception as e:
            logger.warning(
                f"Failed to init MongoTokenManager: {e}. Falling back to memory."
            )
            token_manager = TokenManager(tokens)
    else:
        logger.info(f"Initializing TokenManager with {len(tokens)} tokens")
        token_manager = TokenManager(tokens)

    logger.info(f"Loading source data from {input_csv}")
    try:
        df_source = duckdb.read_csv(input_csv).df()
    except Exception as e:
        logger.error(f"Failed to read CSV with DuckDB: {e}")
        sys.exit(1)

    new_cols = [
        "gh_num_reviewers",
        "gh_num_approvals",
        "gh_time_to_first_review",
        "gh_review_sentiment",
        "gh_linked_issues_count",
        "gh_has_bug_label",
    ]
    for col in new_cols:
        if col not in df_source.columns:
            df_source[col] = None

    con = duckdb.connect(database=":memory:")
    processed_ids = set()
    parquet_files = glob.glob(os.path.join(output_dir, "part_*.parquet"))

    if parquet_files:
        logger.info(
            f"Found {len(parquet_files)} existing parts. Scanning for processed IDs..."
        )
        try:
            processed_ids_df = con.execute(
                f"SELECT tr_build_id FROM read_parquet('{os.path.join(output_dir, 'part_*.parquet')}')"
            ).fetchdf()
            processed_ids = set(processed_ids_df["tr_build_id"])
            logger.info(f"Found {len(processed_ids)} processed rows.")
        except Exception as e:
            logger.warning(f"Could not read existing parquet files: {e}")

    df_to_process = df_source[~df_source["tr_build_id"].isin(processed_ids)]
    logger.info(f"Rows to process: {len(df_to_process)} (Total: {len(df_source)})")

    if df_to_process.empty:
        logger.info("All done!")
        return

    df_to_process = df_to_process.sort_values(by="gh_project_name")

    chunks = [
        df_to_process[i : i + batch_size]
        for i in range(0, len(df_to_process), batch_size)
    ]
    total_chunks = len(chunks)

    max_workers = config.get("max_workers", 5)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i, chunk in tqdm(
            enumerate(chunks), total=total_chunks, desc="Processing Batches"
        ):
            logger.info(f"Starting batch {i+1}/{len(chunks)}: rows={len(chunk)}")
            try:
                df_enriched = process_batch(chunk.copy(), config, token_manager, executor)

                batch_filename = f"part_{int(datetime.now().timestamp())}_{i}.parquet"
                output_path = os.path.join(output_dir, batch_filename)
                df_enriched.to_parquet(output_path, index=False)
                logger.info(
                    f"Saved batch {i+1} file {output_path} (rows={len(df_enriched)})"
                )
            except Exception as e:
                logger.error(f"Error processing batch {i}: {e}")
                continue

    if enable_merge:
        merge_results(output_dir)

    logger.info("PR-only enrichment pipeline finished.")


if __name__ == "__main__":
    main()
