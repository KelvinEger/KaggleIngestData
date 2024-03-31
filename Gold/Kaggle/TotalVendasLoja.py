# Databricks notebook source
# MAGIC %md
# MAGIC #1. Imports

# COMMAND ----------

from pyspark.sql.functions import sum, col

# COMMAND ----------

# MAGIC %md
# MAGIC #2. Carregamento dos dados

# COMMAND ----------

dfOrdens      = spark.read.format("delta").load('/silver/delta/bike_store_sample_database/orders')
dfOrdensItens = spark.read.format("delta").load('/silver/delta/bike_store_sample_database/order_items')
dfLojas       = spark.read.format("delta").load('/silver/delta/bike_store_sample_database/stores')

# COMMAND ----------

# MAGIC %md
# MAGIC #3. Transformação

# COMMAND ----------

dfOrdensLoja = (
    dfOrdens
    .join(dfOrdensItens, dfOrdens.order_id == dfOrdensItens.order_id)
    .join(dfLojas, dfOrdens.store_id == dfLojas.store_id)
    .groupBy(dfLojas.city, dfLojas.state)
    .agg(sum(col("quantity") * col("list_price") - col("discount")).alias("total_sales"))
    .orderBy("total_sales", ascending=False)
    .withColumnRenamed('state', 'Estado')
    .withColumnRenamed('city', 'Cidade')
    .withColumnRenamed('total_sales', 'TotalVendas')
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Persistência dos dados na Gold

# COMMAND ----------

PATH_GOLD         = 'dbfs:/gold/delta/bike_store_sample_database/totalVendasLoja'
PATH_GOLD_PARQUET = 'dbfs:/gold/parquet/bike_store_sample_database/totalVendasLoja'

dfOrdensLoja.write.format('delta').mode('overwrite').option('mergeSchema', 'true').save(PATH_GOLD)
dfOrdensLoja.write.format('parquet').mode('overwrite').save(PATH_GOLD_PARQUET)

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Visualização

# COMMAND ----------

display(dfOrdensLoja)

# COMMAND ----------

del dfOrdensLoja
