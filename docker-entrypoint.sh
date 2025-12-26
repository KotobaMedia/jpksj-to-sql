#!/bin/bash
set -e

# データベース接続が確立されるまで待機する関数
wait_for_db() {
  echo "データベース接続を待機しています..."
  
  host=$(echo $1 | grep -oP 'host=\K[^ ]+')
  user=$(echo $1 | grep -oP 'user=\K[^ ]+')
  password=$(echo $1 | grep -oP 'password=\K[^ ]+')
  dbname=$(echo $1 | grep -oP 'dbname=\K[^ ]+')

  until PGPASSWORD=$password psql -h $host -U $user -d $dbname -c '\q' > /dev/null 2>&1; do
    echo "PostgreSQLサーバーが起動するのを待っています..."
    sleep 2
  done
  
  echo "データベース接続が確立されました！"
}

# データベース接続文字列が引数で渡された場合は待機する
output_format=""
connection_arg=""

if [[ "$1" == "jpksj-to-sql" ]]; then
  output_format="$2"
  connection_arg="$3"
else
  output_format="$1"
  connection_arg="$2"
fi

normalized_format=$(echo "$output_format" | tr '[:upper:]' '[:lower:]')
if [[ "$normalized_format" == "postgres" || "$normalized_format" == "postgresql" || "$normalized_format" == "postgis" || "$normalized_format" == "pg" ]]; then
  if [[ -n "$connection_arg" && "$connection_arg" == host=* ]]; then
    wait_for_db "$connection_arg"
  fi
fi

# コマンドを実行
exec "$@"
