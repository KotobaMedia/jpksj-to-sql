use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
pub struct Cli {
    /// Postgresデータベースに接続する文字列
    pub postgres_url: String,

    /// 中間ファイルの保存先 (Zip等)
    /// デフォルトは `./tmp` となります。
    #[arg(short, long)]
    pub tmp_dir: Option<PathBuf>,

    /// データのダウンロードをスキップします
    /// データが存在しない場合はスキップされます
    #[arg(short, long, default_value = "false")]
    pub skip_download: bool,
}

pub fn main() -> Cli {
    Cli::parse()
}
