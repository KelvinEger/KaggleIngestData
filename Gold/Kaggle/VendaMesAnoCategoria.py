# Databricks notebook source
# MAGIC %md
# MAGIC # Total de venda por Ano, mês e categoria.
# MAGIC - O indicador serve para identificar pontos de mais venda de determinados produtos (por categoria), dentro de um período.
# MAGIC - O indicador é útil para que o gestor tome a decisão se investe mais em marketing para alavancar vendas de outros produtos /
# MAGIC ou se investe no que já vende.

# COMMAND ----------

# MAGIC %md
# MAGIC #1. Imports

# COMMAND ----------

from pyspark.sql.functions import col, year, sum

# COMMAND ----------

# MAGIC %md
# MAGIC #2. Carregamento dos dados

# COMMAND ----------

dfOrdens        = spark.read.format("delta").load('/silver/delta/bike_store_sample_database/orders')
dfOrdensProduto = spark.read.format("delta").load('/silver/delta/bike_store_sample_database/order_items')
dfProdutos      = spark.read.format("delta").load('/silver/delta/bike_store_sample_database/products')
dfCategorias    = spark.read.format("delta").load('/silver/delta/bike_store_sample_database/categories')

# COMMAND ----------

# MAGIC %md
# MAGIC #3. Transformação

# COMMAND ----------

dfvendasCategoriaAnual = dfOrdens \
    .join(dfOrdensProduto, dfOrdens.order_id == dfOrdensProduto.order_id) \
    .join(dfProdutos, dfOrdensProduto.product_id == dfProdutos.products_id) \
    .join(dfCategorias, dfProdutos.category_id == dfCategorias.category_id) \
    .groupBy(dfCategorias.category_name, year(dfOrdens.order_date).alias('year')) \
    .agg(sum((dfOrdensProduto.quantity * (dfOrdensProduto.list_price - dfOrdensProduto.discount))).alias('total_sales')) \
    .withColumnRenamed("category_name", "Categoria") \
    .orderBy('year', 'category_name')

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Persistência dos dados na Gold

# COMMAND ----------

PATH_PERSISTENCIA_GOLD    = "/gold/delta/bike_store_sample_database/vendaMesAnoCategoria"
PATH_PERSISTENCIA_GOLD_PQ = "/gold/parquet/bike_store_sample_database/vendaMesAnoCategoria"

dfvendasCategoriaAnual.write.format('delta').mode('overwrite').option('mergeSchema', 'true').save(PATH_PERSISTENCIA_GOLD)
dfvendasCategoriaAnual.write.format('parquet').mode('overwrite').save(PATH_PERSISTENCIA_GOLD_PQ)


# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Visualização

# COMMAND ----------

display(dfvendasCategoriaAnual)

# COMMAND ----------

del dfvendasCategoriaAnual
