# 国土数値情報データをダウンロードするツール

このツールは、[国土数値情報](https://nlftp.mlit.go.jp/ksj/)のデータとメタデータをPostgreSQL (PostGIS) 用のデータベースに取り込み、すぐに自由な分析ができる状態に整理します。

取り込むデータは、[JPGIS2.1準拠整備データ一覧](https://nlftp.mlit.go.jp/ksj/gml/gml_datalist.html)から選ばれ、指定した形式に変換します。全国のデータがあればそのまま使いますが、都道府県やメッシュコードを指定してダウンロードが必要のものに関しては、このツールがすべてダウンロードして一つの出力に統合します。同一ダウンロードに複数のデータセット(例えば、[医療圏](https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-A38-2020.html)データでは1,2,3次医療圏を別々として管理している)がある場合は、別々のテーブル(またはファイル)として出力します。

なお、各データセットには「商用」「非商用」「CC BY 4.0」など利用条件が設定されているため、[利用規約の確認](https://nlftp.mlit.go.jp/ksj/other/agreement.html)の上使用してください。

## データベースの概要

* データの識別子をテーブル名とし、カラム名は日本語へマッピング後となります。
    * 位置情報は `geom` カラムに入っています
    * Feature ID は `ogc_fid`（ogr2ogr により自動生成）
* `datasets` テーブルにメタデータが入っています
    * メタデータは [to-sql シリーズと共通](https://github.com/KotobaMedia/km-to-sql/)になっています

### メタデータの形

識別子 `P05` からの引用

```jsonc
{
    "desc": "全国の市役所、区役所、町役場、村役場、及びこれらの支所、出張所、連絡所等、及び市区町村が主体的に設置・管理・運営する公民館、集会所等の公的集会施設について、その位置と名称、所在地、施設分類コード、行政コードをGISデータとして整備したものである。",
    "name": "市町村役場等及び公的集会施設",
    "source_url": "https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-P05-2022.html",
    "primary_key": "ogc_fid",
    "columns": [
        {
            // ogr2ogr が自動で作るプライマリキー
            // `name` はデータベース上のカラム名
            "name": "ogc_fid",
            "data_type": "int4"
        },
        {
            // 外部キーの場合。「行政区域」はすべて admini_boundary_cd を利用します。
            "desc": "都道府県コードと市区町村コードからなる、行政区を特定するためのコード",
            "name": "行政区域コード",
            "data_type": "varchar",
            "foreign_key": {
                "foreign_table": "admini_boundary_cd",
                "foreign_column": "改正後のコード"
            }
        },
        {
            "desc": "市町村役場等及び公的集会施設の分類を表すコード",
            "name": "施設分類",
            "data_type": "varchar",
            "enum_values": [
                {
                    "desc": "集会施設",
                    "value": "5"
                },
                {
                    "desc": "上記以外の行政サービス施設",
                    "value": "3"
                },
                {
                    "desc": "公立公民館",
                    "value": "4"
                },
                {
                    "desc": "本庁（市役所、区役所、町役場、村役場）",
                    "value": "1"
                },
                {
                    "desc": "支所、出張所、連絡所",
                    "value": "2"
                }
            ]
            // コードリストではなくて定数の場合は `[{"value":"値1"},{"value":"値2"},...]` となります。
        },
        {
            "desc": "市町村役場等及び公的集会施設の名称",
            "name": "名称",
            "data_type": "varchar"
        },
        {
            "desc": "市町村役場等及び公的集会施設の所在地。",
            "name": "所在地",
            "data_type": "varchar"
        },
        {
            "name": "geom",
            "data_type": "geometry(MULTIPOINT, 6668)"
        }
    ],
    "license": "CC_BY_4.0"
}
```
※ 'admini_boundary_cd' テーブルは別途インポートが必要です（例: 行政区域データ）。

## 利用方法

### 使用手順（概要）
1. バイナリをダウンロード
2. PostgreSQL + PostGIS を用意
3. コマンドを実行:
   jpksj-to-sql "host=127.0.0.1 dbname=jpksj"

バイナリを [最新リリース](https://github.com/keichan34/jpksj-to-sql/releases/) からダウンロードするのがおすすめです。

GDAL 3.9以上必要です (`ogr2ogr` または `ogrinfo` が実行できる環境。 `ogrinfo` は `-limit` 引数使うので、 3.9 が必要です)。PostgreSQL がデフォルトなので、この場合は `--format` 指定は不要です。

```
jpksj-to-sql "host=127.0.0.1 dbname=jpksj"
```

GeoParquet/GeoJSON/FlatGeobuf で出力する場合は、`--format` で GDAL driver 名を指定して出力先ディレクトリを渡します:

```
jpksj-to-sql --format GeoParquet ./output
jpksj-to-sql --format GeoJSON ./output
jpksj-to-sql --format FlatGeobuf ./output
```

macOS の場合、GitHub Release からダウンロードしたバイナリが Gatekeeper によりブロックされることがあります。その場合は、次のコマンドで実行を許可できます: `xattr -d com.apple.quarantine ./jpksj-to-sql`

インターネット接続、メモリ、SSD転送速度等によって処理時間が大幅に左右します。途中からの続きを再開するために幾つかのオプションがあるので、 `jpksj-to-sql --help` で確認してください。

ダウンロードした ZIP ファイルや解凍した shapefile をデフォルトで実行ディレクトリ内 `./tmp` に保存されます。

### Docker環境での利用方法

Docker環境を使うことで、PostgreSQLとGDALの設定を自動化し、簡単に利用することができます。

1. リポジトリをクローンします。
   ```
   git clone https://github.com/keichan34/jpksj-to-sql.git
   cd jpksj-to-sql
   ```

2. Dockerコンテナをビルドして起動します。
   ```
   docker compose build
   docker compose up
   ```

3. アプリケーションのログを確認するには:
   ```
   docker compose logs -f jpksj-to-sql
   ```

4. データベースに接続するには:
   ```
   docker compose exec db psql -U postgres -d jpksj
   ```

Docker環境では以下の設定が適用されます：
- PostgreSQL + PostGIS（バージョン15-3.4）がデータベースコンテナとして実行されます
- データベースのデータはDockerボリューム（postgres-data）に保存されます
- ダウンロードファイルはホストマシンの`./tmp`ディレクトリに保存されます
- デフォルトのデータベース接続情報:
  - ホスト: `db`
  - ユーザー: `postgres`
  - パスワード: `postgres`
  - データベース名: `jpksj`

## コンパイル

Rust の開発環境が必要です。構築後、 cargo を使ってください

```
cargo build
```

## ステータス

こちらは実験的なツールであり、商用環境での使用は推奨していません。データの実験のために使われているので適当な実装が多いのですが、機能について下記をご覧ください。
「PR歓迎」となっているところは挑戦してみてください。

### 実装済み
- データ一覧やそのメタデータは [JPKSJ API](https://jpksj-api.kmproj.com/) を利用します。
- データのダウンロードおよび解凍
- メタデータの取得およびパース（JPKSJ API）
- メタデータを元に属性名マッピング（shapefileでの `G04a_001` みたいなのを SQL カラム名 `3次メッシュコード` に変換）
- メタデータをデータベース内に保存（ `datasets` テーブル参照してください ）
- 読み込むデータセットの指定
- 文字コードの認識

### 未対応・PR歓迎
- [読み込み失敗しているデータセットはまだある](https://github.com/KotobaMedia/jpksj-to-sql/labels/%E8%AA%AD%E3%81%BF%E8%BE%BC%E3%81%BF%E5%A4%B1%E6%95%97)
- VRTによるレイヤー統合から並列処理に変更
- 同一データセット内に複数識別子が存在する時のハンドリング
- 複数年のデータが存在する場合のハンドリング
- 部分更新（必要ないかも）

## ライセンス

こちらのレポジトリのソースコードには MIT ライセンスのもとで提供されます。

> [!IMPORTANT]
> このツールでダウンロード・加工したデータを利用するときに、[国土数値情報の利用規約](https://nlftp.mlit.go.jp/ksj/other/agreement_01.html)を確認した上で利用ください。
