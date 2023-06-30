#!/bin/bash

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONFIG="${BASEDIR}/../../config/config.sh"
source "${CONFIG}"


for table in "${TABLES[@]}"
do
    TARGET_DATABASE="$TARGET_DATABASE"
    DFS_DIR="/datalake/raw/$table"
    TARGET_TABLE_EXTERNAL="$table"
    TARGET_TABLE_GERENCIADA="tbl_$table"

docker exec -i hive-server /bin/bash <<EOF

    beeline -u jdbc:hive2://localhost:10000 \
    --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
    --hivevar DFS_DIR="${DFS_DIR}"\
    --hivevar TARGET_TABLE_EXTERNAL="${TARGET_TABLE_EXTERNAL}"\
    --hivevar TARGET_TABLE_GERENCIADA="${TARGET_TABLE_GERENCIADA}"\
    --hivevar PARTICAO="${PARTICAO}"\
    -f /input/scripts/hql/create_table_$table.hql
EOF
done

# docker exec -i hive-server /bin/bash <<EOF

#     beeline -u jdbc:hive2://localhost:10000 \
#     --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
#     --hivevar DFS_DIR="${DFS_DIR}"\
#     --hivevar TARGET_TABLE_EXTERNAL="${TARGET_TABLE_EXTERNAL}"\
#     --hivevar TARGET_TABLE_GERENCIADA="${TARGET_TABLE_GERENCIADA}"\
#     --hivevar PARTICAO="${PARTICAO}"\
#     -f /input/scripts/hql/create_table_$table.hql

# EOF