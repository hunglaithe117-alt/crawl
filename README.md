# Module Thu Thập & Làm Giàu Dữ Liệu (Crawler & Enrichment)

Module này xử lý việc thu thập dữ liệu và làm giàu các tính năng từ GitHub và TravisTorrent. Hệ thống được thiết kế để xử lý khối lượng dữ liệu lớn với các tính năng như quản lý token thông minh, tự động xoay IP trên Google Cloud, và tối ưu hóa việc gọi GitHub API (GraphQL & ETag caching).

## 🚀 Tính Năng Nổi Bật

* **Quản Lý Token Thông Minh (Smart Token Management)**: Tự động xử lý các giới hạn rate limit (403/429), xoay vòng token, và thực hiện chiến lược "ngủ" thông minh dựa trên thời gian reset của token.
* **Xoay IP Tự Động (GCE IP Rotation)**: Tích hợp với Google Cloud Compute Engine để tự động đổi địa chỉ IP Public của VM khi phát hiện bị chặn (429/403).
* **Tối Ưu Hóa GitHub API**:
  * **GraphQL**: Lấy thông tin Pull Request, reviews, comments, và labels chỉ trong 1 request thay vì nhiều REST calls rời rạc.
  * **ETag Caching**: Sử dụng Conditional Requests (If-None-Match) để tiết kiệm quota API cho các tài nguyên chưa thay đổi.
* **Pipelines Làm Giàu Dữ Liệu**:
  * `github_enrichment.py`: Trích xuất các chỉ số PR, phân tích cảm xúc (sentiment analysis), và các mẫu review.
  * `risk_features_enrichment.py`: Tính toán độ biến động code (churn), entropy, và quyền sở hữu của tác giả (author ownership).

## 🛠 Cài Đặt & Cấu Hình

### 1. Yêu Cầu Tiên Quyết

* Python 3.9+
* Google Cloud SDK (`gcloud`) đã được cài đặt và xác thực (nếu sử dụng tính năng xoay IP).
* Git

### 2. Cài Đặt

Di chuyển vào thư mục `crawl`:

```bash
# 1. Cập nhật hệ thống
sudo apt-get update

# 2. Cài Python, pip và venv (môi trường ảo)
sudo apt-get install -y python3-pip python3-venv git
sudo apt-get install tmux
sudo apt install htop -y
cd crawl
```

Cài đặt các thư viện cần thiết:

```bash
pip install pandas requests pyyaml tqdm duckdb google-cloud-sdk
```

### 3. Cấu Hình

Tạo các file cấu hình từ file mẫu:

```bash
cp crawler_config.example.yml crawler_config.yml
cp tokens.example.yml tokens.yml
```

#### `tokens.yml`

Thêm các GitHub Personal Access Tokens của bạn vào đây. Hệ thống sẽ sử dụng chúng theo cơ chế Round-Robin và cân bằng tải.

```yaml
github_tokens:
  - "ghp_your_token_1..."
  - "ghp_your_token_2..."
  - "ghp_your_token_3..."
```

#### `crawler_config.yml`

Điều chỉnh các cài đặt cho crawler.

```yaml
max_workers: 5                  # Số lượng luồng xử lý song song
github_api_retry_count: 5       # Số lần thử lại khi request thất bại
github_api_retry_delay: 1.0     # Thời gian chờ cơ bản (backoff)
# ...
```

### 4. Cấu Hình Xoay IP Google Cloud (Tùy Chọn)

Nếu chạy trên Google Cloud VM, hệ thống có thể tự đổi IP để vượt qua lỗi 429.

1. **Quyền Hạn**: Đảm bảo Service Account của VM có quyền **Compute Instance Admin (v1)** (hoặc đủ quyền để cập nhật network interfaces).
2. **Cấu Hình**: Script sẽ tự động phát hiện tên instance và zone. Bạn có thể ghi đè bằng biến môi trường:
   * `GCE_INSTANCE_NAME`: Tên VM của bạn.
   * `GCE_ZONE`: Zone (ví dụ: `us-central1-a`).

## 🏃 Hướng Dẫn Sử Dụng

### Làm Giàu Dữ Liệu GitHub (GitHub Enrichment)

Bổ sung thông tin về Pull Request, sentiment, thời gian review.

```bash
python enrich/github_enrichment.py \
  --input /path/to/input.csv \
  --output-dir /path/to/output_gh \
  --merge
```

### Làm Giàu Tính Năng Rủi Ro (Risk Features Enrichment)

Tính toán entropy, churn, và rủi ro thời gian build.

```bash
python enrich/risk_features_enrichment.py \
  --input /path/to/input.csv \
  --output-dir /path/to/output_risk \
  --merge
```

### Các Tham Số Chung

* `--input`: Đường dẫn file CSV đầu vào (cần chứa `gh_project_name`, `git_trigger_commit`, v.v.).
* `--output-dir`: Thư mục lưu các file Parquet đầu ra.
* `--batch-size`: Số dòng xử lý mỗi batch (mặc định: 1000).
* `--merge`: Nếu có cờ này, sẽ gộp tất cả file Parquet thành 1 file CSV cuối cùng.
* `--no-mongo`: Bắt buộc sử dụng quản lý token trong bộ nhớ (mặc định trong phiên bản này).

## 🏗 Kiến Trúc Hệ Thống

* **`github_api_client.py`**: Client chính. Kiểm tra cờ `network_ready_event` trước mỗi request. Nếu đang xoay IP, tất cả các luồng sẽ tạm dừng.
* **`token_pool.py`**: Class `TokenManager`. Theo dõi header `X-RateLimit-Remaining` và `X-RateLimit-Reset` để chọn token tốt nhất hoặc ngủ chờ token reset.
* **`gce_rotator.py`**: Wrapper gọi lệnh `gcloud compute instances delete-access-config` và `add-access-config` để đổi IP ephemeral.
