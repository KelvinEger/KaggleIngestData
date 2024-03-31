# Databricks notebook source
# MAGIC %md
# MAGIC #1. Carregamento dos dados

# COMMAND ----------

dfOrders      = spark.read.format("delta").load('/silver/delta/bike_store_sample_database/orders')
dfOrdersItems = spark.read.format("delta").load('/silver/delta/bike_store_sample_database/order_items')
dfStores      = spark.read.format("delta").load('/silver/delta/bike_store_sample_database/stores')

# COMMAND ----------

# MAGIC %md
# MAGIC #2. Transformação

# COMMAND ----------

dfOrders.createOrReplaceTempView("orders")
dfOrdersItems.createOrReplaceTempView("order_items")
dfStores.createOrReplaceTempView("stores")

# COMMAND ----------

# Total vendas por mês de cada loja
dfFinal = spark.sql("""
  SELECT SUM(order_items.quantity)  As total_venda_mes,
         orders.order_compet AS ano_mes_venda,
         stores.store_id   AS loja_codigo,
         stores.store_name AS Loja
    FROM orders
    JOIN order_items
      ON order_items.order_id = orders.order_id
    JOIN stores
      ON stores.store_id = orders.store_id
GROUP BY stores.store_id,
         stores.store_name,
         orders.order_compet
ORDER BY SUM(order_items.quantity)  DESC,
         stores.store_name
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Persistência dos dados na Gold

# COMMAND ----------

PATH_GOLD         = 'dbfs:/gold/delta/bike_store_sample_database/vendasMesLoja'
PATH_GOLD_PARQUET = 'dbfs:/gold/parquet/bike_store_sample_database/vendasMesLoja'

dfFinal.write.format('delta').mode('overwrite').option('mergeSchema', 'true').save(PATH_GOLD)
dfFinal.write.format('parquet').mode('overwrite').save(PATH_GOLD_PARQUET)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Visualização

# COMMAND ----------

display(dfFinal) #criar visualização

# COMMAND ----------

del dfFinal
