# Databricks notebook source
# MAGIC %md
# MAGIC #1. Imports

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.sql.functions import col, expr, to_date, when

# COMMAND ----------

# MAGIC %md
# MAGIC #2. Define a estrutura

# COMMAND ----------

oStructType = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("item_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("list_price", FloatType(), False),
    StructField("discount", FloatType(), True)
])

# COMMAND ----------

dfOrdersItems = spark.read.csv(
    '/bronze/bike_store_sample_database/order_items.csv',
    schema = oStructType,
    header = True
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Trata dados
# MAGIC - Adicionada coluna de percentual de desconto. Cálculo: % Desconto = (Valor desconto / Preço) * 100
# MAGIC - Remove valores nulos
# MAGIC - Remove valores de preço e quantidade negativos

# COMMAND ----------

dfOrdersItems = dfOrdersItems.withColumn(
    "discount_percentage", 
    when(col("list_price") != 0, (col("discount") / col("list_price")) * 100).otherwise(0)
)

dfOrdersItems = dfOrdersItems.filter(
    col("order_id").isNotNull() & 
    col("item_id").isNotNull() & 
    col("product_id").isNotNull() & 
    col("quantity").isNotNull() & 
    (col("quantity") > 0) &
    col("list_price").isNotNull() & 
    (col("list_price") > 0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Exporta DeltaLake

# COMMAND ----------

PATH_SILVER = 'dbfs:/silver/delta/bike_store_sample_database/order_items'

dfOrdersItems.write.format('delta').mode('overwrite').option('mergeSchema', 'true').save(PATH_SILVER)

# COMMAND ----------

del dfOrdersItems
