import argparse
import glob
import logging
import os
import shutil
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb
import pandas as pd
from tqdm import tqdm

# Add parent directory to path to import shared helpers
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from token_pool import TokenManager  # noqa: E402

# Local enrichment modules
import risk_features_enrichment as risk_mod  # noqa: E402
import github_enrichment as gh_mod  # noqa: E402
import consecutive_builds_enrichment as cb_mod  # noqa: E402


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


def compute_consecutive_features(df: pd.DataFrame, max_workers: int) -> pd.DataFrame:
    """
    Compute consecutive-build features for the whole dataset (parallel by project).
    """
    df = df.copy()
    df["gh_build_started_at"] = pd.to_datetime(df["gh_build_started_at"])

    project_groups = list(df.groupby("gh_project_name"))
    logger.info(
        f"Computing consecutive-build features for {len(project_groups)} projects with {max_workers} workers"
    )

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_project = {
            executor.submit(cb_mod._process_project_group, group.copy()): name
            for name, group in project_groups
            if pd.notna(name)
        }
        for future in as_completed(future_to_project):
            project_name = future_to_project[future]
            try:
                results.append(future.result())
            except Exception:
                logger.exception(f"[{project_name}] Consecutive feature computation failed:")

    if results:
        merged = pd.concat(results, ignore_index=True)
    else:
        merged = pd.DataFrame(columns=df.columns)

    # Ensure deterministic ordering after merge
    merged = merged.sort_values(
        by=["gh_project_name", "gh_build_started_at"], ascending=[True, True]
    )
    return merged


def merge_parts(parts_dir: str, output_path: str) -> None:
    logger.info(f"Merging parts from {parts_dir} into {output_path}")
    pattern = os.path.join(parts_dir, "part_*.parquet")
    files = glob.glob(pattern)
    if not files:
        logger.warning("No parts found to merge.")
        return

    con = duckdb.connect(database=":memory:")
    try:
        con.execute(
            f"CREATE OR REPLACE VIEW all_data AS SELECT * FROM read_parquet('{pattern}')"
        )
        if output_path.lower().endswith(".csv"):
            con.execute(
                f"COPY (SELECT * FROM all_data) TO '{output_path}' (HEADER, DELIMITER ',')"
            )
        else:
            con.execute(
                f"COPY (SELECT * FROM all_data) TO '{output_path}' (FORMAT PARQUET)"
            )
        logger.info(f"Saved merged data to {output_path}")
    finally:
        try:
            con.close()
        except Exception:
            pass


def ensure_columns(df: pd.DataFrame, cols: list) -> None:
    for col in cols:
        if col not in df.columns:
            df[col] = None


def main():
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    CONFIG_PATH = os.path.join(BASE_DIR, "crawler_config.yml")

    parser = argparse.ArgumentParser(
        description="Enrich build dataset with all features (risk, GitHub, consecutive) in one pass."
    )
    parser.add_argument("--input", required=True, help="Path to input CSV file")
    parser.add_argument("--output", required=True, help="Path to write merged output (csv or parquet)")
    parser.add_argument(
        "--repos-dir", default="/tmp/repos_risk", help="Directory for temporary git clones"
    )
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for risk/GitHub enrichment")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    parser.add_argument(
        "--keep-parts",
        action="store_true",
        help="Keep intermediate parquet parts instead of deleting after merge",
    )
    args = parser.parse_args()

    try:
        level = getattr(logging, args.log_level.upper(), logging.INFO)
        logging.getLogger().setLevel(level)
    except Exception:
        pass

    os.makedirs(args.repos_dir, exist_ok=True)

    config = risk_mod.load_config(CONFIG_PATH)

    tokens_env = os.environ.get("GITHUB_TOKENS", "")
    if tokens_env:
        tokens = [t.strip() for t in tokens_env.split(",") if t.strip()]
    else:
        tokens = config.get("github_tokens", [])

    logger.info(f"Initializing TokenManager with {len(tokens)} tokens")
    token_manager = TokenManager(tokens)

    logger.info(f"Reading {args.input} via DuckDB")
    try:
        df_source = duckdb.read_csv(args.input).df()
    except Exception as e:
        logger.error(f"Failed to read input CSV: {e}")
        sys.exit(1)

    all_new_cols = [
        # Risk
        "is_new_contributor",
        "build_hour_risk_score",
        "build_hour_sin",
        "build_hour_cos",
        "src_test_churn_ratio",
        "change_entropy",
        # GitHub
        "gh_num_reviewers",
        "gh_num_approvals",
        "gh_time_to_first_review",
        "gh_review_sentiment",
        "gh_linked_issues_count",
        "gh_has_bug_label",
        "file_change_frequency",
        "author_ownership",
        # Consecutive
        "prev_tr_status",
        "is_prev_failed",
        "time_since_prev_build",
        "prev_fail_streak",
        "avg_src_churn_last_5",
        "fail_rate_last_10",
        "churn_ratio_vs_avg",
    ]
    ensure_columns(df_source, all_new_cols)

    max_workers = config.get("max_workers", 5)

    # 1) Consecutive features (needs full dataset for correct history)
    df_with_consecutive = compute_consecutive_features(df_source, max_workers=max_workers)

    # 2) Risk + GitHub features in batches
    df_to_process = df_with_consecutive.sort_values(by="gh_project_name")
    total_chunks = (len(df_to_process) // args.batch_size) + 1
    chunks = [
        df_to_process[i : i + args.batch_size]
        for i in range(0, len(df_to_process), args.batch_size)
    ]

    parts_dir = os.path.join(os.path.dirname(args.output), "all_features_parts")
    os.makedirs(parts_dir, exist_ok=True)
    all_logs = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i, chunk in tqdm(
            enumerate(chunks), total=total_chunks, desc="Processing Batches"
        ):
            if chunk.empty:
                continue
            logger.info(f"Starting batch {i+1}/{len(chunks)}: rows={len(chunk)}")

            try:
                risk_df, risk_logs = risk_mod.process_batch(
                    chunk.copy(), config, token_manager, args.repos_dir, executor
                )
                if risk_logs:
                    all_logs.extend(
                        [
                            (tr_id, proj, sha, reason)
                            for tr_id, proj, sha, reason in risk_logs
                        ]
                    )

                next_projects = set()
                if i + 1 < len(chunks):
                    next_projects = set(chunks[i + 1]["gh_project_name"].unique())

                gh_df, gh_logs = gh_mod.process_batch(
                    risk_df, config, token_manager, args.repos_dir, executor, projects_to_keep=next_projects
                )
                if gh_logs:
                    # GitHub logs are strings; normalize to tuple form
                    for log_line in gh_logs:
                        all_logs.append((None, None, None, log_line))

                part_path = os.path.join(
                    parts_dir, f"part_{int(datetime.now().timestamp())}_{i}.parquet"
                )
                gh_df.to_parquet(part_path, index=False)
                logger.info(f"Saved batch to {part_path}")
            except Exception as e:
                logger.error(f"Batch {i} failed: {e}")

    merge_parts(parts_dir, args.output)

    if all_logs:
        log_path = os.path.join(os.path.dirname(args.output), "missing_commits_log.csv")
        log_df = pd.DataFrame(
            all_logs, columns=["tr_build_id", "gh_project_name", "commit_sha", "reason"]
        )
        if os.path.exists(log_path):
            log_df.to_csv(log_path, mode="a", header=False, index=False)
        else:
            log_df.to_csv(log_path, index=False)
        logger.info(f"Wrote logs to {log_path}")

    if not args.keep_parts:
        try:
            shutil.rmtree(parts_dir)
        except Exception:
            pass


if __name__ == "__main__":
    main()
