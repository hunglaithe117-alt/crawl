import argparse
import logging
import os
import sys
import pandas as pd
import numpy as np
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def _process_project_group(group: pd.DataFrame) -> pd.DataFrame:
    """Compute linear-history features for a single project."""
    project_name = group["gh_project_name"].iloc[0]
    group = group.sort_values(by="gh_build_started_at", ascending=True).copy()

    n = len(group)
    prev_tr_status_arr = np.empty(n, dtype=object)
    is_prev_failed_arr = np.zeros(n, dtype=bool)
    time_since_prev_build_arr = np.full(n, -1.0)
    prev_fail_streak_arr = np.zeros(n, dtype=int)
    avg_src_churn_last_5_arr = np.zeros(n, dtype=float)
    fail_rate_last_10_arr = np.zeros(n, dtype=float)

    triggers = group["git_trigger_commit"].values
    prev_commits = group["git_prev_built_commit"].values
    statuses = group["tr_status"].values
    times = group["gh_build_started_at"].values
    churns = group["git_diff_src_churn"].fillna(0).values

    commit_stats = {}

    for i in range(n):
        if i and i % 50000 == 0:
            logging.info(f"[{project_name}] Đã xử lý {i}/{n} dòng...")

        trigger = triggers[i]
        prev_commit = prev_commits[i]
        status = statuses[i]
        timestamp = times[i]
        churn = churns[i]

        has_prev = False
        if pd.notna(prev_commit) and prev_commit in commit_stats:
            prev_stats = commit_stats[prev_commit]
            has_prev = True

        if has_prev:
            p_status = prev_stats["status"]
            prev_tr_status_arr[i] = p_status

            is_prev_failed_arr[i] = p_status == "failed"

            p_time = prev_stats["time"]
            diff = (timestamp - p_time) / np.timedelta64(1, "h")
            time_since_prev_build_arr[i] = diff

            p_streak = prev_stats["fail_streak"]
            prev_fail_streak_arr[i] = p_streak

            p_churn_hist = prev_stats["churn_history"]
            if len(p_churn_hist) > 0:
                avg_src_churn_last_5_arr[i] = sum(p_churn_hist) / len(p_churn_hist)
            else:
                avg_src_churn_last_5_arr[i] = 0.0

            p_fail_hist = prev_stats["fail_history"]
            if len(p_fail_hist) > 0:
                fail_rate_last_10_arr[i] = sum(p_fail_hist) / len(p_fail_hist)
            else:
                fail_rate_last_10_arr[i] = 0.0

            curr_churn_hist = p_churn_hist[-4:] + [churn]
            curr_fail_hist = p_fail_hist[-9:] + [1 if status == "failed" else 0]

            if status == "failed":
                curr_streak = p_streak + 1
            else:
                curr_streak = 0
        else:
            prev_tr_status_arr[i] = None
            is_prev_failed_arr[i] = False
            time_since_prev_build_arr[i] = -1.0
            prev_fail_streak_arr[i] = 0
            avg_src_churn_last_5_arr[i] = 0.0
            fail_rate_last_10_arr[i] = 0.0

            curr_churn_hist = [churn]
            curr_fail_hist = [1 if status == "failed" else 0]
            curr_streak = 1 if status == "failed" else 0

        commit_stats[trigger] = {
            "status": status,
            "time": timestamp,
            "fail_streak": curr_streak,
            "churn_history": curr_churn_hist,
            "fail_history": curr_fail_hist,
        }

    group["prev_tr_status"] = prev_tr_status_arr
    group["is_prev_failed"] = is_prev_failed_arr
    group["time_since_prev_build"] = time_since_prev_build_arr
    group["prev_fail_streak"] = prev_fail_streak_arr
    group["avg_src_churn_last_5"] = avg_src_churn_last_5_arr
    group["fail_rate_last_10"] = fail_rate_last_10_arr
    group["churn_ratio_vs_avg"] = group["git_diff_src_churn"] / (
        group["avg_src_churn_last_5"] + 1
    )

    return group


def process_software_defect_data(input_csv_path: str, output_csv_path: str) -> None:
    """Load build CSV data, compute linear-history features and save enriched CSV."""
    logging.info("--- 1. Đang tải dữ liệu ---")
    df = pd.read_csv(input_csv_path)

    df["gh_build_started_at"] = pd.to_datetime(df["gh_build_started_at"])
    df = df.sort_values(
        by=["gh_project_name", "gh_build_started_at"], ascending=[True, True]
    )

    logging.info("--- 2. Tạo Features dựa trên chuỗi commit (Linear History) ---")
    groups = list(df.groupby("gh_project_name"))
    max_workers = max(1, min(32, (os.cpu_count() or 1) + 4))
    logging.info(
        f"Đang xử lý {len(groups)} project với {max_workers} thread workers..."
    )

    start_time = time.time()
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_project = {
            executor.submit(_process_project_group, group.copy()): name
            for name, group in groups
        }
        for future in as_completed(future_to_project):
            proj = future_to_project[future]
            try:
                results.append(future.result())
            except Exception:
                logging.exception(f"Project {proj} failed:")

    if results:
        df_processed = pd.concat(results, ignore_index=True)
    else:
        df_processed = pd.DataFrame(columns=df.columns)

    logging.info(f"Xử lý xong trong {time.time() - start_time:.2f} giây.")
    logging.info("--- 3. Tính toán các Feature phụ thuộc ---")

    features_to_fill_0 = [
        "is_prev_failed",
        "prev_fail_streak",
        "avg_src_churn_last_5",
        "fail_rate_last_10",
    ]
    df_processed[features_to_fill_0] = df_processed[features_to_fill_0].fillna(0)

    df_processed["churn_ratio_vs_avg"] = df_processed["git_diff_src_churn"] / (
        df_processed["avg_src_churn_last_5"] + 1
    )

    df_processed = df_processed.sort_values(
        by=["gh_project_name", "gh_build_started_at"], ascending=[True, True]
    )

    logging.info("--- 4. Dọn dẹp và Lưu file ---")
    df_processed.to_csv(output_csv_path, index=False)
    logging.info(f"Hoàn tất! File đã lưu tại: {output_csv_path}")
    logging.info(f"Kích thước dữ liệu: {df_processed.shape}")


def _validate_paths(input_path: str, output_path: str, force: bool) -> None:
    """Check input exists and handle output overwrite behavior."""
    if not os.path.exists(input_path):
        logging.error(f"Input file not found: {input_path}")
        sys.exit(1)

    if os.path.exists(output_path) and not force:
        # Ask user to confirm overwrite
        try:
            confirm = input(f"Output file '{output_path}' exists. Overwrite? (y/N): ")
        except EOFError:
            confirm = 'n'
        if confirm.strip().lower() != 'y':
            logging.info("Aborted by user. No file written.")
            sys.exit(0)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Enrich build dataset with consecutive-build features (linear-history)."
    )
    parser.add_argument("input_csv", help="Path to the input CSV file")
    parser.add_argument(
        "output_csv",
        nargs="?",
        default=None,
        help="Path to write the output CSV. Default: input file + '.enriched.csv'",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite output file if it exists without prompting",
    )
    parser.add_argument(
        "--loglevel",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set the logging level",
    )

    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, args.loglevel))

    input_path = args.input_csv
    output_path = args.output_csv or os.path.splitext(input_path)[0] + ".enriched.csv"

    _validate_paths(input_path, output_path, args.force)

    try:
        process_software_defect_data(input_path, output_path)
    except Exception:
        logging.exception("Processing failed due to an error:")
        sys.exit(1)


if __name__ == "__main__":
    main()
