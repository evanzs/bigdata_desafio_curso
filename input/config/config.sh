#!/bin/bash

DATE="$(date --date="-0 day" "+%Y%m%d")"

TABLES=("clientes" "divisao" "endereco" "regiao" "vendas")

TARGET_DATABASE="desafio_curso"
SERVER="prod"
HDFS_DIR="/datalake/raw"
TARGET_TABLE_EXTERNAL=""
TARGET_TABLE_GERENCIADA=""

PARTICAO="$(date --date="-0 day" "+%Y%m%d")"