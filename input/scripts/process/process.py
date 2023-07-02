
## REALIZANDO IMPORTS DE LIBS E PREPRANDO O AMBIENTE
from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql.window import Window
import importlib
import json
from typing import Dict, Tuple
import argparse
import pandas as pd
import os
import subprocess

# Obtenha o diretório do script atual
current_dir = os.path.dirname(os.path.abspath(__file__))

# Caminho para o diretório pre_process
pre_process_dir = os.path.join(current_dir, '..', 'pre_process') 

#funcao para executar scripts .sh
def executar_script(script_path):
    # Define permissões de execução para o arquivo
    subprocess.run(['chmod', '+x', script_path], check=True)
    # Executa o arquivo
    subprocess.run(['bash', script_path])


# RODA O SCRIPT CREATE_ENV_ALL PARA CRIAR OS ARQUIVOS NO HDFS
script_path = pre_process_dir + "/create_env_all.sh"
executar_script(script_path)

# RODA O SCRIPT CREATE_ENV_ALL PARA CRIAR AS TABELAS NO HIVE
script_path = pre_process_dir + "/carga_tabelas.sh"
executar_script(script_path)



### INICIO DAS QUERYS EM PYTHON 
spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()



#transferindo as tabelas para o spark e transformando em dataframe 

# Lista com nomes tabelas
tabelas = ["clientes", "divisao", "endereco", "regiao", "vendas"]

# Dicionário para armazenar os DataFrames
df_pandas = {}

# Loop para ler as tabelas e armazenar os DataFrames no dicionário
for tabela in tabelas:
    query = "SELECT * FROM desafio_curso." + tabela
    df = spark.sql(query)
    
    #transforma em df_pandas ja removendo o index e resetando
    df_pandas[tabela] = df.toPandas().drop(index=0).reset_index(drop=True)


### REMOVENDO REPETIDOS NAS TABELAS
df_pandas['clientes']  = df_pandas['clientes'].drop_duplicates(subset='customerkey')
df_pandas['endereco']  = df_pandas['endereco'] .drop_duplicates(subset='address_number')
df_pandas['vendas']  = df_pandas['vendas'] .drop_duplicates(subset='order_number')



# Tratando valores vazios do tipo String pra cada coluna
##### ENDERECO

#identificando e preenchendo valores vazios de endereço

#selecionando colunas para verificação
colunas= ['city','country','customer_address_1','customer_address_2','customer_address_3','customer_address_4','state','zip_code']

df = df_pandas['endereco']
# Verificar valores vazios na coluna
for coluna in colunas:
    #remove os espaços em branco e troca pra "nao informado"
    df[coluna] = df[coluna].str.strip().replace('','Não informado')
  
    
df_pandas['endereco'].head()


##### VENDAS

#identificando e preenchendo valores vazios de vendas

#selecionando colunas para verificação
colunas= ['item','item_class','um']

df = df_pandas['vendas']
# Verificar valores vazios na coluna
for coluna in colunas:
    #remove os espaços em branco e troca pra "nao informado"
    df[coluna] = df[coluna].str.strip().replace('','Não informado')
  
    
df_pandas['vendas'].head()




#### CLIENTES 
#identificando e preenchendo valores vazios de clientes

#selecionando colunas para verificação
colunas= ['customer','customer_type','line_of_business','phone']

df = df_pandas['clientes']
# Verificar valores vazios na coluna
for coluna in colunas:
    #remove os espaços em branco e troca pra "nao informado"
    df[coluna] = df[coluna].str.strip().replace('','Não informado')
  
    
df_pandas['clientes'].head()



# Tratando valores vazios do tipo decimais ou inteiros  pra cada coluna

#### ENDERECO

#identificando e preenchendo valores vazios de endereço

#selecionando colunas para verificação
colunas= ['address_number']

df = df_pandas['endereco']
# Verificar valores vazios na coluna
for coluna in colunas:
    #remove os espaços em branco e troca pra "nao informado"
    df[coluna] = df[coluna].str.strip().replace('','0')
  
    


##### VENDAS

#identificando e preenchendo valores vazios de endereço

colunas= ['customerkey','discount_amount','invoice_number','item_number','sales_amount','sales_amount_based_on_list_price','sales_price','sales_quantity']

df = df_pandas['vendas']
# Verificar valores vazios na coluna
for coluna in colunas:
    #remove os espaços em branco e troca pra "0"
    df[coluna] = df[coluna].str.strip().replace('','0')

        

##### CLIENTES
#identificando e preenchendo valores vazios de endereço

colunas= ['address_number','business_unit','customerkey','division']
df = df_pandas['clientes']
# Verificar valores vazios na coluna
for coluna in colunas:
    #remove os espaços em branco e troca pra "0"
    df[coluna] = df[coluna].str.strip().replace('','0')
  


### nesse momento trabalharei com spark, apenas para utilizar os modos de manipulação mesmo 

### convertendo para spark 
df_spark = {}
df_panda ={}
for tabela in tabelas:
    df_panda = df_pandas[tabela] 
    # Converter DataFrame do pandas para DataFrame do Spark
    df_spark[tabela] = spark.createDataFrame(df_panda)


### JUNTANDO COLUNAS entra tabelas

## CLIENTE E DIVISAO
df_cliente_divisao = df_spark['clientes'].join(df_spark['divisao'], on='division', how='inner')
df_cliente_divisao = df_spark['clientes'].join(df_spark['divisao'], on='division', how='inner')

## CLIENTE E REGIAO
df_cliente_stage = df_cliente_divisao.join(df_spark['regiao'], on='region_code', how='inner')


#CLIENTE E ENDERECO
df_cliente_stage = df_cliente_stage.join(df_spark['endereco'], on='address_number', how='inner')


### JUNTANDO TABELA DE VENDAS 

#note: antes de manipular mais a tabela, irei remover colunas e fazer contas

#criando um fitro
colunas_remover = ['actual_delivery_date','datekey','line_number','promised_delivery_date','sales_amount_based_on_list_price','sales_margin_amount','sales_rep','um']

#removendo as colunas
df_vendas_filtrada = df_spark['vendas'].drop(*colunas_remover)



### Criando o indicador de soma total das quantidades

# Converter a coluna 'sales_quantity' para o tipo numérico
df = df_vendas_filtrada.withColumn('sales_quantity', col('sales_quantity').cast('double'))

# Definir a janela de operação
window = Window.orderBy()

# Adicionar uma nova coluna 'total_sales' com a soma dos valores da coluna 'sales_quantity'
df = df.withColumn('total_sales', sum(col('sales_quantity')).over(window))

##repete o mesmos passoas para calcular o total do valor das vendas
df_aux = df.withColumn('sales_amount', col('sales_amount').cast('double'))
df = df_aux.withColumn('total_sales_price', sum(col('sales_amount')).over(window))

# Dividir a coluna "invoice_date"
df = df.withColumn("day", split(col("invoice_date"), "/")[0].cast("int"))
df = df.withColumn("month", split(col("invoice_date"), "/")[1].cast("int"))
df = df.withColumn("year", split(col("invoice_date"), "/")[2].cast("int"))



df_vendas_filtrada = df 

### JUNTANDO AS INFORMAÇÔES DE VENDA COM CLIENTE

df_stage = df_vendas_filtrada.join(df_cliente_stage, on='customerkey', how='inner')



### CRIANDO CHAVES PRAS TABELAS FINALS


#alterando o nome da coluna para ficar mais padronizada 

df_stage = df_stage.withColumnRenamed("division", "division_code")
df_stage = df_stage.withColumnRenamed("item", "item_name")
df_stage = df_stage.withColumnRenamed("customerkey", "customer_code")
df_stage = df_stage.withColumnRenamed("customer", "customer_name")




#DIMENSAO LOCALIDADE
df_stage = df_stage.withColumn("DW_LOCALIDADE", sha2(concat_ws("", df_stage.address_number,df_stage.state,df_stage.division_name, df_stage.division_code), 256))

#DIMENSAO TEMPO
df_stage = df_stage.withColumn("DW_TEMPO", sha2(concat_ws("", df_stage.invoice_date, df_stage.year,df_stage.month,df_stage.day), 256))

#DIMENSAO CLEINTES
df_stage = df_stage.withColumn("DW_CLIENTES", sha2(concat_ws("", df_stage.customer_name, df_stage.customer_code), 256))




### CRIANDO DIMENSÔES E FATO


df_stage.createOrReplaceTempView('stage')

#Criando a dimensão Clientes
dim_clientes = spark.sql('''
    SELECT DISTINCT
        DW_CLIENTES,
        customer_code,
        customer_name     
    FROM stage    
''')


#Criando a dimensão TEMPO
dim_tempo = spark.sql('''
    SELECT DISTINCT
        DW_TEMPO,
        year,
        month,
        day,
        invoice_date
    FROM stage    
''')


#Criando a dimensão LOCALIDADE
dim_localidade = spark.sql('''
    SELECT DISTINCT
        DW_LOCALIDADE,
        address_number,
        state,
        division_name,
        division_code
    FROM stage    
''')



#FATO!!


#Criando a Fato VENDAS!
ft_vendas = spark.sql('''
    SELECT 
        DW_CLIENTES,
        DW_TEMPO,
        DW_LOCALIDADE,   
        total_sales,
        sales_amount,
        total_sales_price
    FROM stage
    group by 
        DW_CLIENTES, 
        DW_TEMPO,
        DW_LOCALIDADE,
        total_sales,
        sales_amount,
        total_sales_price
''')



### SALVANDO OS DADOS
# função para salvar os dados
#Essa função deve salvar o .csv gerados separados por ; na pasta: input/gold/
def salvar_df(df, file):
    output = "/input/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/gold/"+file+".csv"
    print(rename)
    
    
    df.coalesce(1).write\
        .format("csv")\
        .option("header", True)\
        .option("delimiter", ";")\
        .mode("overwrite")\
        .save("/datalake/gold/"+file+"/")

    os.system(erase)
    os.system(rename)

salvar_df(ft_vendas, 'ft_vendas')
salvar_df(dim_clientes, 'dim_cliente')
salvar_df(dim_tempo, 'dim_tempo')
salvar_df(dim_localidade, 'dim_localidade')