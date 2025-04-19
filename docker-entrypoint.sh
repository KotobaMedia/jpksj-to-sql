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
if [[ "$2" == host=* ]]; then
  wait_for_db "$2"
fi

# コマンドを実行
exec "$@"