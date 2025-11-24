import pandas as pd
import numpy as np


def process_software_defect_data(input_csv_path, output_csv_path):
    print("--- 1. Đang tải dữ liệu ---")
    df = pd.read_csv(input_csv_path)

    # Chuyển đổi cột thời gian sang datetime object để tính toán
    df["gh_build_started_at"] = pd.to_datetime(df["gh_build_started_at"])

    # Sắp xếp dữ liệu: Gom theo Project trước, sau đó xếp theo Thời gian
    # (Bước quan trọng nhất để các hàm shift/diff hoạt động đúng)
    df = df.sort_values(
        by=["gh_project_name", "gh_build_started_at"], ascending=[True, True]
    )

    print("--- 2. Tạo các Features cơ bản dựa trên dòng trước (Lag Features) ---")

    # Nhóm theo project (để dữ liệu dự án A không bị trôi sang dự án B)
    grouped = df.groupby("gh_project_name")

    # 2.1 Lấy trạng thái build trước đó
    df["prev_tr_status"] = grouped["tr_status"].shift(1)

    # 2.2 Feature: Build trước có fail không?
    # Coi cả 'failed' là thất bại
    df["is_prev_failed"] = df["prev_tr_status"].apply(
        lambda x: True if x in ["failed"] else False
    )

    # 2.4 Feature: Người commit có thay đổi so với build trước không?
    # (Cần cột tr_original_commit hoặc thông tin author nếu có)
    # Giả sử dùng git_trigger_commit để đại diện
    df["prev_trigger_commit"] = grouped["git_trigger_commit"].shift(1)

    print("--- 3. Tạo Features về Thời gian (Time Gap) ---")

    # 3.1 Tính khoảng cách thời gian so với build trước (giờ)
    df["prev_build_time"] = grouped["gh_build_started_at"].shift(1)
    df["time_since_prev_build"] = (
        df["gh_build_started_at"] - df["prev_build_time"]
    ).dt.total_seconds() / 3600

    # Xử lý NaN cho dòng đầu tiên của mỗi project (thay bằng -1 hoặc giá trị trung bình)
    df["time_since_prev_build"] = df["time_since_prev_build"].fillna(-1)

    print("--- 4. Tạo Features nâng cao: Chuỗi thất bại (Consecutive Failures) ---")

    # Logic: Tạo ID nhóm cho mỗi chuỗi trạng thái liên tiếp
    # Đây là kỹ thuật Vectorization để tránh vòng lặp for (cực nhanh)

    # Đánh dấu các dòng bị fail
    fail_mask = df["tr_status"].isin(["failed"])

    # Tạo nhóm: Mỗi khi trạng thái đổi (Pass -> Fail hoặc Fail -> Pass) thì tăng ID lên 1
    # ne() nghĩa là Not Equal
    # shift() so sánh với dòng trước
    # cumsum() cộng dồn để tạo ID
    # Chỉ tính trong cùng 1 project
    project_change = df["gh_project_name"] != df["gh_project_name"].shift(1)
    status_change = df["tr_status"] != df["tr_status"].shift(1)

    # Gom nhóm các chuỗi giống nhau
    chunk_id = (project_change | status_change).cumsum()

    # Đếm số lượng trong mỗi nhóm (cumcount bắt đầu từ 0 nên phải +1)
    df["consecutive_streak"] = df.groupby(chunk_id).cumcount() + 1

    # Nếu dòng hiện tại là Pass, thì streak fail = 0
    # Nếu dòng hiện tại là Fail, streak giữ nguyên giá trị vừa tính
    df["fail_streak"] = np.where(fail_mask, df["consecutive_streak"], 0)

    # QUAN TRỌNG: Feature chúng ta cần là "Fail streak TRƯỚC KHI build này chạy"
    # Nên phải shift 1 lần nữa
    df["prev_fail_streak"] = grouped["fail_streak"].shift(1).fillna(0)

    print("--- 5. Tạo Features thống kê trượt (Rolling Window) ---")

    # 5.1 Trung bình churn của 5 build gần nhất
    # transform để trả về kích thước y hệt dataframe gốc
    df["avg_src_churn_last_5"] = grouped["git_diff_src_churn"].transform(
        lambda x: x.rolling(window=5, min_periods=1).mean().shift(1)
    )

    # 5.2 Tỷ lệ churn hiện tại so với trung bình
    # Tránh chia cho 0 bằng cách cộng 1
    df["churn_ratio_vs_avg"] = df["git_diff_src_churn"] / (
        df["avg_src_churn_last_5"] + 1
    )

    # 5.3 Tỷ lệ fail trong 10 build gần nhất (Fail Rate History)
    df["fail_rate_last_10"] = grouped["is_prev_failed"].transform(
        lambda x: x.rolling(window=10, min_periods=1).mean()
    )

    print("--- 6. Dọn dẹp và Lưu file ---")

    # Điền các giá trị NaN sinh ra do shift (thường là các dòng đầu tiên của project)
    features_to_fill_0 = [
        "is_prev_failed",
        "prev_fail_streak",
        "avg_src_churn_last_5",
        "fail_rate_last_10",
    ]
    df[features_to_fill_0] = df[features_to_fill_0].fillna(0)

    # Xóa các cột tạm không cần thiết để file nhẹ hơn
    cols_to_drop = [
        "prev_tr_status",
        "prev_trigger_commit",
        "prev_build_time",
        "consecutive_streak",
        "fail_streak",
    ]
    df.drop(columns=cols_to_drop, inplace=True, errors="ignore")

    # Lưu file
    df.to_csv(output_csv_path, index=False)
    print(f"Hoàn tất! File đã lưu tại: {output_csv_path}")
    print(f"Kích thước dữ liệu: {df.shape}")
