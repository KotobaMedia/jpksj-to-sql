use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
pub struct Cli {
    /// Postgresデータベースに接続する文字列。 ogr2ogr に渡されます。冒頭の `PG:` は省略してください。
    pub postgres_url: String,

    /// 中間ファイルの保存先 (Zip等)
    /// デフォルトは `./tmp` となります。
    #[arg(long)]
    pub tmp_dir: Option<PathBuf>,

    /// データのダウンロードをスキップします
    /// データが存在しない場合はスキップされます
    #[arg(long, default_value = "false")]
    pub skip_download: bool,

    /// 既に存在するテーブルをスキップします
    /// プロセスが途中で中断された場合、テーブルが中途半端な状態にある可能性があります
    #[arg(long, default_value = "false")]
    pub skip_sql_if_exists: bool,
}

pub fn main() -> Cli {
    Cli::parse()
}
