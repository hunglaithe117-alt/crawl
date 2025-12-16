# Module Thu Thập & Scan Dữ Liệu Build Logs (Crawler & Scanner)

Module này xử lý việc thu thập dữ liệu từ GitHub, scan repositories để tìm build logs từ GitHub Actions/Travis CI, và làm giàu các tính năng. Hệ thống được thiết kế để xử lý khối lượng dữ liệu lớn với các tính năng như quản lý token thông minh, tự động xoay IP trên Google Cloud, tối ưu hóa GitHub API (GraphQL & ETag caching), và pipelines làm giàu dữ liệu.

## 🚀 Tính Năng Nổi Bật

* **Scan Repositories cho Build Logs**: Tự động tìm kiếm repos trên GitHub dựa trên filters (languages, stars, updated date), detect CI providers (GitHub Actions/Travis CI), và đánh giá khả năng thu thập logs (min_builds).
* **Quản Lý Token Thông Minh (Smart Token Management)**: Tự động xử lý rate limits (403/429), xoay vòng token, và ngủ thông minh dựa trên reset time.
* **Xoay IP Tự Động (GCE IP Rotation)**: Tích hợp Google Cloud để đổi IP khi bị chặn.
* **Tối Ưu Hóa GitHub API**:
  * **GraphQL**: Lấy PR info, reviews, comments, labels trong 1 request.
  * **ETag Caching**: Conditional requests để tiết kiệm quota.
* **Pipelines Làm Giàu Dữ Liệu**:
  * `github_enrichment.py`: Trích xuất metrics PR, sentiment analysis, review patterns.
  * `risk_features_enrichment.py`: Tính churn, entropy, author ownership.
* **Lưu Trữ MongoDB**: Persistent storage cho scan results và tokens.

## 🛠 Cài Đặt & Cấu Hình

### 1. Yêu Cầu Tiên Quyết

* Python 3.9+
* MongoDB (local hoặc remote)
* Google Cloud SDK (`gcloud`) nếu dùng IP rotation
* Git

### 2. Cài Đặt

```bash
cd crawl
uv sync  # Cài dependencies từ pyproject.toml
```

### 3. Cấu Hình

Copy và edit config files:

```bash
cp crawler_config.example.yml crawler_config.yml
cp tokens.example.yml tokens.yml
```

#### `tokens.yml`

Thêm GitHub/Travis tokens:

```yaml
github_tokens:
  - "ghp_your_token_1..."
travis_tokens: []  # Optional
```

#### `crawler_config.yml`

Cấu hình scan và API:

```yaml
mongo_uri: "mongodb://localhost:27017"
db_name: "ci_crawler"
languages: ["Python", "Ruby", "Java"]
min_stars: 50
min_builds: 30
max_workers: 5
github_api_retry_count: 5
github_api_retry_delay: 1.0
# ...
```

Khởi động MongoDB nếu local.

## 🏃 Hướng Dẫn Sử Dụng

### Scan Repositories cho Build Logs

Scan repos, detect CI, evaluate logs:

```bash
uv run python scanner.py --config crawler_config.yml --limit 10 --verbose
```

Options:
* `--limit <n>`: Max repos scan.
* `--loop`: Scan vô hạn.
* `--min-builds <n>`: Override min builds.
* `--add-github-token <token>`: Thêm token động.

Kết quả lưu trong MongoDB collection `scan_results`.

### Làm Giàu Dữ Liệu GitHub

```bash
python enrich/github_enrichment.py --input /path/to/input.csv --output-dir /path/to/output --merge
```

### Làm Giàu Tính Năng Rủi Ro

```bash
python enrich/risk_features_enrichment.py --input /path/to/input.csv --output-dir /path/to/output --merge
```

Tham số chung: `--input`, `--output-dir`, `--batch-size`, `--merge`, `--no-mongo`.

## 🏗 Kiến Trúc Hệ Thống

* **`scanner.py`**: Entry point scan repos.
* **`config.py`**: Load config từ YAML.
* **`store.py`**: MongoDB interface cho scan results.
* **`token_pool.py`**: Token management với rate limiting.
* **`github_api_client.py`**: GitHub API client với retries.
* **`manage_tokens.py`**: CLI tool quản lý tokens.
* **`gce_rotator.py`**: IP rotation cho GCE.

## 📊 Kết Quả Scan

Status trong DB:
* `ready`: Đủ logs downloadable.
* `insufficient`: Ít logs.
* `auth_failed`: Cần permissions.
* `logs_gone`: Logs deleted.

## 🔧 Troubleshooting

* **Mongo Auth Error**: Đảm bảo Mongo chạy và URI đúng.
* **Rate Limits**: Thêm tokens.
* **Import Errors**: Chạy `uv sync`.

## 📝 Notes

* Sử dụng `--no-mongo` nếu không có MongoDB.
* Tokens không commit vào Git.
