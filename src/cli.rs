use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[command(version)]
pub struct Cli {
    /// 出力フォーマット（GDAL driver 名、または PostgreSQL を示す文字列）
    /// 指定しない場合は postgresql が使用されます
    #[arg(
        long = "format",
        value_name = "OUTPUT_FORMAT",
        default_value = "postgresql"
    )]
    pub output_format: String,

    /// 出力先（PostgreSQL の場合は接続文字列、その他は出力ディレクトリ）
    #[arg(value_name = "OUTPUT_DESTINATION")]
    pub output_destination: String,

    /// 中間ファイルの保存先 (Zip等)
    /// デフォルトはシステムのtmpディレクトリを利用します
    #[arg(long)]
    pub tmp_dir: Option<PathBuf>,

    /// データのダウンロードをスキップします
    /// データが存在しない場合はスキップされます
    #[arg(long, default_value = "false")]
    pub skip_download: bool,

    /// 既に存在する出力をスキップします
    /// プロセスが途中で中断された場合、出力が中途半端な状態にある可能性があります
    #[arg(long, alias = "skip-sql-if-exists")]
    pub skip_if_exists: bool,

    /// 読み込むデータセットの識別子
    /// 指定しない場合は全てのデータセットが読み込まれます
    /// 複数指定する場合は `,` で区切ってください
    #[arg(long, value_delimiter = ',')]
    pub filter_identifiers: Option<Vec<String>>,

    /// 取得するデータセットの年（例: 2019）
    /// 指定しない場合は最新のデータセットが使用されます
    #[arg(long)]
    pub year: Option<u32>,
}

pub fn main() -> Cli {
    Cli::parse()
}
