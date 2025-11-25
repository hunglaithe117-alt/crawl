import pandas as pd
import numpy as np
import time

def process_software_defect_data(input_csv_path, output_csv_path):
    print("--- 1. Đang tải dữ liệu ---")
    df = pd.read_csv(input_csv_path)

    # Chuyển đổi cột thời gian sang datetime object để tính toán
    df["gh_build_started_at"] = pd.to_datetime(df["gh_build_started_at"])

    # Sắp xếp dữ liệu: Gom theo Project trước, sau đó xếp theo Thời gian
    df = df.sort_values(
        by=["gh_project_name", "gh_build_started_at"], ascending=[True, True]
    )

    print("--- 2. Tạo Features dựa trên chuỗi commit (Linear History) ---")
    print("Đang xử lý từng dòng để xây dựng chuỗi commit...")

    # Chuẩn bị các mảng kết quả
    n = len(df)
    prev_tr_status_arr = np.empty(n, dtype=object)
    is_prev_failed_arr = np.zeros(n, dtype=bool)
    time_since_prev_build_arr = np.full(n, -1.0)
    prev_fail_streak_arr = np.zeros(n, dtype=int)
    avg_src_churn_last_5_arr = np.zeros(n, dtype=float)
    fail_rate_last_10_arr = np.zeros(n, dtype=float)

    # Lấy dữ liệu ra numpy array để duyệt cho nhanh
    projects = df["gh_project_name"].values
    triggers = df["git_trigger_commit"].values
    prev_commits = df["git_prev_built_commit"].values
    statuses = df["tr_status"].values
    times = df["gh_build_started_at"].values
    churns = df["git_diff_src_churn"].fillna(0).values

    # Dictionary lưu trạng thái của các commit: commit_hash -> stats
    commit_stats = {}
    current_project = None

    start_time = time.time()
    
    for i in range(n):
        if i % 50000 == 0:
            print(f"Đã xử lý {i}/{n} dòng...")

        proj = projects[i]
        trigger = triggers[i]
        prev_commit = prev_commits[i]
        status = statuses[i]
        timestamp = times[i]
        churn = churns[i]

        # Reset dict nếu sang project mới
        if proj != current_project:
            commit_stats = {}
            current_project = proj

        # Tìm thông tin build trước đó dựa trên git_prev_built_commit
        has_prev = False
        if pd.notna(prev_commit) and prev_commit in commit_stats:
            prev_stats = commit_stats[prev_commit]
            has_prev = True

        if has_prev:
            # 1. Trạng thái build trước
            p_status = prev_stats['status']
            prev_tr_status_arr[i] = p_status
            
            # 2. Build trước có fail không?
            is_prev_failed_arr[i] = (p_status == 'failed')
            
            # 3. Khoảng cách thời gian
            p_time = prev_stats['time']
            diff = (timestamp - p_time) / np.timedelta64(1, 'h')
            time_since_prev_build_arr[i] = diff
            
            # 4. Streak fail trước đó
            p_streak = prev_stats['fail_streak']
            prev_fail_streak_arr[i] = p_streak
            
            # 5. Trung bình churn 5 build trước (theo chuỗi)
            p_churn_hist = prev_stats['churn_history']
            if len(p_churn_hist) > 0:
                avg_src_churn_last_5_arr[i] = sum(p_churn_hist) / len(p_churn_hist)
            else:
                avg_src_churn_last_5_arr[i] = 0.0
                
            # 6. Tỷ lệ fail 10 build trước (theo chuỗi)
            p_fail_hist = prev_stats['fail_history']
            if len(p_fail_hist) > 0:
                fail_rate_last_10_arr[i] = sum(p_fail_hist) / len(p_fail_hist)
            else:
                fail_rate_last_10_arr[i] = 0.0
            
            # Cập nhật lịch sử cho build hiện tại (kế thừa từ cha)
            # Giữ lại 4 churn gần nhất + churn hiện tại = 5
            curr_churn_hist = p_churn_hist[-4:] + [churn]
            # Giữ lại 9 status gần nhất + status hiện tại = 10
            curr_fail_hist = p_fail_hist[-9:] + [1 if status == 'failed' else 0]
            
            # Tính streak hiện tại
            if status == 'failed':
                curr_streak = p_streak + 1
            else:
                curr_streak = 0
        else:
            # Không tìm thấy build trước (build đầu tiên hoặc đứt chuỗi)
            prev_tr_status_arr[i] = None
            is_prev_failed_arr[i] = False
            time_since_prev_build_arr[i] = -1.0
            prev_fail_streak_arr[i] = 0
            avg_src_churn_last_5_arr[i] = 0.0
            fail_rate_last_10_arr[i] = 0.0
            
            # Khởi tạo lịch sử mới
            curr_churn_hist = [churn]
            curr_fail_hist = [1 if status == 'failed' else 0]
            curr_streak = 1 if status == 'failed' else 0

        # Lưu trạng thái của commit hiện tại vào dict
        # Nếu commit này kích hoạt nhiều build, build chạy sau cùng (theo sort time) sẽ chốt trạng thái
        commit_stats[trigger] = {
            'status': status,
            'time': timestamp,
            'fail_streak': curr_streak,
            'churn_history': curr_churn_hist,
            'fail_history': curr_fail_hist
        }

    print(f"Xử lý xong trong {time.time() - start_time:.2f} giây.")

    # Gán lại vào DataFrame
    df["prev_tr_status"] = prev_tr_status_arr
    df["is_prev_failed"] = is_prev_failed_arr
    df["time_since_prev_build"] = time_since_prev_build_arr
    df["prev_fail_streak"] = prev_fail_streak_arr
    df["avg_src_churn_last_5"] = avg_src_churn_last_5_arr
    df["fail_rate_last_10"] = fail_rate_last_10_arr

    print("--- 3. Tính toán các Feature phụ thuộc ---")

    # Feature: Tỷ lệ churn hiện tại so với trung bình
    df["churn_ratio_vs_avg"] = df["git_diff_src_churn"] / (
        df["avg_src_churn_last_5"] + 1
    )

    print("--- 4. Dọn dẹp và Lưu file ---")

    # Điền các giá trị NaN (nếu còn sót)
    features_to_fill_0 = [
        "is_prev_failed",
        "prev_fail_streak",
        "avg_src_churn_last_5",
        "fail_rate_last_10",
    ]
    df[features_to_fill_0] = df[features_to_fill_0].fillna(0)

    # Xóa cột prev_tr_status nếu không cần thiết (hoặc giữ lại để debug)
    # df.drop(columns=["prev_tr_status"], inplace=True, errors="ignore")

    # Lưu file
    df.to_csv(output_csv_path, index=False)
    print(f"Hoàn tất! File đã lưu tại: {output_csv_path}")
    print(f"Kích thước dữ liệu: {df.shape}")
