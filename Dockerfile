# ビルドステージ
FROM rust:latest as builder

# GDAL開発ライブラリと依存関係をインストール
RUN apt-get update && apt-get install -y \
    software-properties-common \
    gnupg \
    wget \
    && apt-get install -y \
    build-essential \
    cmake \
    pkg-config \
    libgdal-dev \
    gdal-bin \
    libpq-dev \
    zip \
    unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# GDAL バージョンを確認
RUN gdalinfo --version

# アプリケーションのビルド
WORKDIR /app
COPY . .
RUN cargo build --release

# 実行ステージ
FROM debian:bookworm-slim

# ランタイム依存関係をインストール
RUN apt-get update && apt-get install -y \
    libgdal32 \
    gdal-bin \
    libpq5 \
    postgresql-client \
    curl \
    ca-certificates \
    libssl3 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# GDAL バージョンを確認
RUN gdalinfo --version

# GDAL_DATAパスを設定
ENV GDAL_DATA=/usr/share/gdal

# 一時ファイル用のディレクトリを作成
WORKDIR /app
RUN mkdir -p /app/tmp
VOLUME ["/app/tmp"]

# ビルドステージからバイナリをコピー
COPY --from=builder /app/target/release/jpksj-to-sql /usr/local/bin/jpksj-to-sql

# 実行時の待機スクリプト
COPY ./docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["jpksj-to-sql"]
