# 国土数値情報データをSQLデータベースに取り込む

このツールは、[国土数値情報](https://nlftp.mlit.go.jp/ksj/)のデータとメタデータをPostgreSQL (PostGIS) 用のデータベースに取り込み、すぐに自由な分析ができる状態に整理します。

取り込むデータは、[JPGIS2.1準拠整備データ一覧](https://nlftp.mlit.go.jp/ksj/gml/gml_datalist.html)から選ばれ、同一データセットに複数ファイルがある場合は1つのテーブルにまとめます（現状は最新年度のみ対応）。

なお、各データセットには「商用」「非商用」「CC BY 4.0」など利用条件が設定されているため、使用時は十分にご注意ください。（今のところ、非商用データはフィルタされています）

## データベースの概要

* データの識別子をテーブル名とし、カラム名は日本語へマッピング後となります。
    * 位置情報は `geom` カラムに入っています
    * Feature ID は `ogc_fid`
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

## 利用方法

バイナリを [最新リリース](https://github.com/keichan34/jpksj-to-sql/releases/) からダウンロードするのがおすすめです。

GDAL 3.9以上必要です (`ogr2ogr` または `ogrinfo` が実行できる環境。 `ogrinfo` は `-limit` 引数使うので、 3.9 が必要です)

```
jpksj-to-sql "host=127.0.0.1 dbname=jpksj"
```

macOS の場合: Gatekeeper の設定で GitHub Release でダウンロードしたバイナリを実行できない場合があります。 `xattr -d com.apple.quarantine ./jpksj-to-sql` を実行したら突破できます。

インターネット接続、メモリ、SSD転送速度等によって処理時間が大幅に左右します。途中からの続きを再開するために幾つかのオプションがあるので、 `jpksj-to-sql --help` で確認してください。

ダウンロードした ZIP ファイルや解凍した shapefile をデフォルトで実行ディレクトリ内 `./tmp` に保存されます。

## コンパイル

Rust の開発環境が必要です。構築後、 cargo を使ってください

```
cargo build
```

## ステータス

こちらは実験的なツールであり、商用運用はお勧めしておりません。データの実験のために使われているので適当な実装が多いのですが、機能について下記をご覧ください。
「PR歓迎」となっているところは挑戦してみてください。

- [x] 国土数値情報のウェブサイトからデータ一覧の取得
- [x] データのダウンロードおよび解凍
- [x] メタデータの取得およびパース（ `shape_property_table2.xlsx` ）
- [x] メタデータを元に属性名マッピング（shapefileでの `G04a_001` みたいなのを SQL カラム名 `3次メッシュコード` に変換）
- [x] メタデータをデータベース内に保存（ `datasets` テーブル参照してください ）
- [x] 読み込むデータセットの指定
- [x] 文字コードの認識
- [ ] VRTによるレイヤー統合から並列処理に変更
- [ ] 同一データセット内に複数識別子が存在する時のハンドリング（PR歓迎）
- [ ] 複数年のデータが存在する場合のハンドリング（PR歓迎）
- [ ] PostgreSQL以外のデータベースにも保存（PR歓迎）
- [ ] 部分更新（必要ないかも）

## ライセンス

こちらのレポジトリのソースコードには MIT ライセンスが適用します。
