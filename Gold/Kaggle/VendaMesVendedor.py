# Databricks notebook source
# MAGIC %md
# MAGIC #1. Carregamento dos dados

# COMMAND ----------

dfOrders      = spark.read.format("delta").load('/silver/delta/bike_store_sample_database/orders')
dfOrdersItems = spark.read.format("delta").load('/silver/delta/bike_store_sample_database/order_items')
dfStaff       = spark.read.format("delta").load('/silver/delta/bike_store_sample_database/staffs')

# COMMAND ----------

# MAGIC %md
# MAGIC #2. Transformação

# COMMAND ----------

dfOrders.createOrReplaceTempView("orders")
dfOrdersItems.createOrReplaceTempView("order_items")
dfStaff.createOrReplaceTempView("staffs")

# COMMAND ----------

dfFinal = spark.sql("""
  SELECT SUM(order_items.list_price) / COUNT(order_items.item_id) AS valor_media_venda,
         orders.order_compet AS ano_mes_venda,
         staffs.staff_id   AS vendedor_codigo,
         staffs.first_name AS Vendedor
    FROM orders
    JOIN order_items
      ON order_items.order_id = orders.order_id
    JOIN staffs
      ON staffs.staff_id = orders.staff_id
GROUP BY staffs.staff_id,
         staffs.first_name,
         orders.order_compet
ORDER BY orders.order_compet
""")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Persistência dos dados na Gold

# COMMAND ----------

PATH_GOLD         = 'dbfs:/gold/delta/bike_store_sample_database/vendasMesVendedor'
PATH_GOLD_PARQUET = 'dbfs:/gold/parquet/bike_store_sample_database/vendasMesVendedor'

dfFinal.write.format('delta').mode('overwrite').option('mergeSchema', 'true').save(PATH_GOLD)
dfFinal.write.format('parquet').mode('overwrite').save(PATH_GOLD_PARQUET)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Visualização

# COMMAND ----------

display(dfFinal)

# COMMAND ----------

del dfFinal
