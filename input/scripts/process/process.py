# from pyspark.sql import SparkSession, dataframe
# from pyspark.sql.types import StructType, StructField
# from pyspark.sql.types import DoubleType, IntegerType, StringType
# from pyspark.sql import HiveContext
# from pyspark.sql.functions import *
# from pyspark.sql import functions as f
import subprocess
import os
import re



# Obtenha o diretório do script atual
current_dir = os.path.dirname(os.path.abspath(__file__))

# Caminho para o diretório pre_process
pre_process_dir = os.path.join(current_dir, '..', 'pre_process')

script_path = pre_process_dir + "/create_env_all.sh"
# Define permissões de execução para o arquivo
subprocess.run(['chmod', '+x',script_path], check=True)
# Executa o arquivo create_env_all.sh
subprocess.run(['bash', script_path])




# spark = SparkSession.builder.master("local[*]")\
#     .enableHiveSupport()\
#     .getOrCreate()

# # Criando dataframes diretamente do Hive
# df_clientes = spark.sql("SELECT * FROM DESAFIO_CURSO.TBL_CLIENTES")

# # Espaço para tratar e juntar os campos e a criação do modelo dimensional

# # criando o fato
# ft_vendas = []

# #criando as dimensões
# dim_clientes = []

# # função para salvar os dados
# def salvar_df(df, file):
#     output = "/input/desafio_hive/gold/" + file
#     erase = "hdfs dfs -rm " + output + "/*"
#     rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/desafio_hive/gold/"+file+".csv"
#     print(rename)
    
    
#     df.coalesce(1).write\
#         .format("csv")\
#         .option("header", True)\
#         .option("delimiter", ";")\
#         .mode("overwrite")\
#         .save("/datalake/gold/"+file+"/")

#     os.system(erase)
#     os.system(rename)

# salvar_df(dim_clientes, 'dimclientes')