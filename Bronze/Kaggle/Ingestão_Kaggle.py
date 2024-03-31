# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC - Carregue o conjunto de dados no Databricks.
# MAGIC - Explore o esquema dos dados e faça ajustes conforme necessário.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Geral
# MAGIC
# MAGIC ## Camada de ingestão dos dados
# MAGIC
# MAGIC - Realiza o download dos arquivos do dataset disposto no link <https://www.kaggle.com/datasets/dillonmyrick/bike-store-sample-database> do site Kaggle.
# MAGIC - Aloca os dados no DBFS pois os dados já vem com uma certa estrutura e são em pouco quantidade, não julguei necessário, para o teste utilizar o blogstorage, o que seria mais ideal pensando em um volume maior de dados.
# MAGIC - Realizado controle de cópia com marca d'agua para que somente seja realizada a cópia quando for um novo dia.
# MAGIC - Esta notebook é responsável apenas por conectar ao Kaggle e obter os dados, caso ainda não tenha feito no dia.
# MAGIC - Utilizei a API do Kaggle para buscar os datasets
# MAGIC - Separei em métodos que é uma boa prática
# MAGIC - Retirei o número de comparações no código para deixá-lo mais legível

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Imports

# COMMAND ----------

import os
from datetime import datetime
import json
from pathlib import Path
from pyspark.sql import SparkSession
import subprocess
import sys

# COMMAND ----------

PACOTE = 'kaggle'

try:
    __import__(PACOTE)
    print(f"{PACOTE} já está instalado.")
except ImportError:
    print(f"{PACOTE} não encontrado. Instalando {PACOTE}...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", PACOTE])

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Funções
# MAGIC
# MAGIC - Como boa prática de programação, separei em pequenas funções a cada ação.
# MAGIC - Pelo mesmo motivo de serem poucos dados e uma aplicação que não terá continuidade apenas separei em métodos e não criei \
# MAGIC estruturas ou classes que abstraiam um comportamento geral, mas dependendo da estruturação da aplicação geral, é interessante.

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.1 Controle de cópia com marca d'agua
# MAGIC - Verifica a data da última cópia, atualizando diariamente, para evitar processamento desnecessário.
# MAGIC - A marca d'agua aqui é de controle próprio comparado com a data atual, mas poderia também ser realizado via data de atualização do dataset

# COMMAND ----------

'''
    Retorna a data atual
    @returns String
'''
def getDataAtual():
    return datetime.now().strftime("%Y-%m-%d")

# COMMAND ----------

'''
    Verifica a data da última cópia realizada
    Não existindo um arquivo ou data, retorna None
    @returns String
'''
def getDataUltimaCopia():
    with open(MARCA_DAGUA, "r") as arquivo:
        return arquivo.read().strip()

# COMMAND ----------

'''
    Atualiza a data da última cópia do Kaggler
    @returns Void
'''
def atualizaDataCopia() : 
    sDataAtual = datetime.now().strftime("%Y-%m-%d")

    with open(os.path.join('/dbfs/FileStore/tables/acesso_kaggler', "last_updated.txt"), "w") as f:
        f.write(sDataAtual)

    print(f"Marca d'agua atualizada para: {sDataAtual}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.2 Autenticação e obtenção dos dados no Kaggler
# MAGIC
# MAGIC - Autenticação realizada via API do Kaggler para download dos dados.

# COMMAND ----------

'''
    Define os dados de acesso do Kaggler de acordo com JSON obtivdo no site para consumo da API
    @returns Void
'''
def setDadosLoginKakkler() :
    with open(JSON_KAGGLE, 'r') as arquivo:
        dados = json.load(arquivo)
    
    os.environ['KAGGLE_USERNAME'] = dados['username']
    os.environ['KAGGLE_KEY']      = dados['key']

# COMMAND ----------

'''
    Cria uma nova sessão no Kaggler de acordo com os dados de login da API
    @returns Void
'''
def getDatasetKaggler() :
    print('Iniciando conexão com o Kaggle via API para obtenção dos dados')

    setDadosLoginKakkler()

    from kaggle.api.kaggle_api_extended import KaggleApi # Este import é realizado aqui, pois da erro de JSON devido aos dados setados acima 

    oApiKaggler = KaggleApi()
    oApiKaggler.authenticate()
    oApiKaggler.dataset_download_files(DATASET_KAGGLE, path=DATASET_PATH, unzip=True)

    print('Dados do Kaggler copiados')

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.3 Funções úteis
# MAGIC - Para deixar o código mais fácil de ler, batendo o olho e entendendo, optei por criar uns métodos simples.

# COMMAND ----------

'''
    Verifica se existe uma pasta  ou um arquiv,  o que for passado por parâmetro
    @returns Boolean
'''
def hasObjeto(sCaminhoPastaArquivo):
    return os.path.isfile(sCaminhoPastaArquivo)

# COMMAND ----------

'''
    De modo a impedir erros de estrutura, e para já deixar a estrutura pronta, verifica-se a estrutura de pastas
    para criar caso não exista
    @returns Void
'''
def verificaEstrutaDBFS() :
    print('Verificando última data de atualização dos dados obtivos via Kaggler')

    os.makedirs(DATASET_PATH, exist_ok=True)

    if(not os.path.isfile(MARCA_DAGUA)) :
        with open(MARCA_DAGUA, 'w') as f:
            f.write(datetime.now().strftime("%Y-%m-%d"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##2.4 Controle de cópia
# MAGIC - Lê os dados do arquivo da marca d'agua e encerra a execução em caso de não ter necessidade de cópia.
# MAGIC - Como foi definido para copiar os dados uma vez por dia, se no dia em questão já tiver cópia, nada mais é realizado no mesmo dia.

# COMMAND ----------

def verificaSeCopia() :
    print('Verificando se temos data para copiar dados.')

    with open(MARCA_DAGUA, "r") as oArquivo:
        sUltimaDataLeitura = oArquivo.read()

    if(getDataAtual() < getDataUltimaCopia()) :
        print('Última cópia já foi realizada no dia atual, não é necessário nova cópia.')
        dbutils.notebook.exit("Encerrando a execução do notebook.")


# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Execução
# MAGIC
# MAGIC Como boa prática, a ideia é que a execução geral não abstraia regras específicas. Sendo assim, na execução geral deixei o mais legível possível. \
# MAGIC Isto é muito útil para facilitar a identificação de possíveis problemas e tambem facilitar a manutenção, pois conseguimos isolar o problema sem comprometer o resto do código.
# MAGIC \
# MAGIC \
# MAGIC Poderíamos também adicionar Logs para deixar mais fácil uma consulta futura, mas como é uma aplicação de teste, optei por não criar, uma vez que a aplicação é bem legível.

# COMMAND ----------

JSON_KAGGLE    = '/dbfs/FileStore/tables/acesso_kaggler/kaggle.json'
MARCA_DAGUA    = '/dbfs/FileStore/tables/acesso_kaggler/last_updated.txt'
DATASET_KAGGLE = 'dillonmyrick/bike-store-sample-database'
DATASET_PATH   = '/dbfs/bronze/bike_store_sample_database/'

sUltimaDataLeitura = None
oApiKaggler        = None

verificaEstrutaDBFS()
verificaSeCopia()
getDatasetKaggler()
atualizaDataCopia()

print("Processo concluído.")
