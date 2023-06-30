#!/bin/bash

# Criação das pastas

DADOS=("clientes" "divisao" "endereco" "regiao" "vendas")
cd ../../raw

for i in "${DADOS[@]}"
do
    echo "gravando arquivo no datalake: $caminho_atual"

    #entra no bash do docker pra executar a ação
    docker exec -i namenode /bin/bash <<EOF
    hdfs dfs -mkdir -p /datalake/raw/$i
    hdfs dfs -chmod 777 /datalake/raw/$i
    hdfs dfs -copyFromLocal /input/raw/$i.csv /datalake/raw/$i
EOF

done
