FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libstdc++6 \
        libgcc-s1 \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir \
        pymongo>=4.6 \
        requests>=2.31 \
        PyYAML>=6.0 \
        pyarrow>=22.0.0 \
        duckdb>=1.4.2 \
        pandas>=2.3.3 \
        tqdm>=4.67.1

COPY . .

ENTRYPOINT ["python", "scanner.py"]
CMD ["--config", "crawler_config.docker.yml"]
