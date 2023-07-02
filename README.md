# bigdata_desafio_curso

- Projeto criado para programa de capaciação bigData minsait.

O Escopo do projeto pode ser visto: 

[Clique aqui](input/readme.md) para acessar o escopo.


Esse projeto foi desenvolvido usando:
 - Docker
 - vsCode
 - Power BI

Totalmente rodas em ambiente linux exceto o Power BI


# A Estrutura de Pasta: 

![Screenshot from 2023-07-01 21-48-11](https://github.com/evanzs/bigdata_desafio_curso/assets/24463238/38d9898c-b529-49bc-acae-8b34a5fd20e2)


# Raiz do projeto: 

 - bigadata_docker - pasta contem o ambiente docker para as ferramentas utilizadas 
 - input - pasta compartilhada com o ambiente que contem o projeto.
 - .gitignore - arquivo criado para ignorar pastas e arquivos para o git


# Estrutura Principal de pastas: 

![image](https://github.com/evanzs/bigdata_desafio_curso/assets/24463238/5606dfa0-7a49-451c-9742-3a9c8d773cf9)

data - contem arquivos pertinentes ao ambiente docker 
docker-compose-yml  - arquivo para rodar o ambiente docker 

Para saber mais sobre essa estrutura é possivel visualizar abaixo: 

[Clique aqui]([input/readme.md](https://github.com/fabiogjardim/bigdata_docker/blob/master/README.md)https://github.com/fabiogjardim/bigdata_docker/blob/master/README.md) para acessar estrutura do ambiente para engenharia de dados.



# Estruturas Internas de pastas: 

![image](https://github.com/evanzs/bigdata_desafio_curso/assets/24463238/3c05229e-e90b-4894-ba25-3a9046c89054)



app - contêm o template do POWER BI ontem mostrá graficamente o resultado do projeto.

config -> config.sh - contêm as variaveis de ambiente do projeto que poderam ser alteradas. 

gold - contêm o arquivo final gerado pelo processoem .CSV

raw - contêm os arquivos inicias brutos para serem trabalhos em formato  .CSV




![image](https://github.com/evanzs/bigdata_desafio_curso/assets/24463238/2a35b84b-debf-446b-b2e3-8db824b3a7d3)

run - pasta que será usado para executar todo ambiente.

scripts - pasta que conterá os scripts a ser executado
scripts -> script.hql - responsavem por criar as tabelas no hive
scripts -> create_env.all.sh - script shell responsavel por enviar os arquivos.csv brutos para dentro do HDFS 
scripts -> carga_tabelas.sh - script shell para executar a os hqls e criar as tabelas no ambiente 

process.py -> arquivo que conterá toda a logica para executar e rodar o projeto de uma só vez.

# Execução do Projeto

- para saber como rodar o projeto em sua maquina:
- 
[Clique aqui](README-EXECUCAO.md) para acessar o escopo.



# Resultado 

Ao final teremos um grafico em POWER BI 

![image](https://github.com/evanzs/bigdata_desafio_curso/assets/24463238/42004460-bf18-4011-852e-95c40a23efcd)








