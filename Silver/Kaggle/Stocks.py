# Databricks notebook source
# MAGIC %md
# MAGIC #1. Imports

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC #2. Define a estrutura

# COMMAND ----------

oStructType = StructType([
    StructField("store_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Trata dados
# MAGIC - Remove valores nulos

# COMMAND ----------

dfStocks = spark.read.csv(
    '/bronze/bike_store_sample_database/stocks.csv',
    schema = oStructType,
    header = True
)

# COMMAND ----------

dfStocks = dfStocks.filter(
    col("store_id").isNotNull() & 
    col("product_id").isNotNull() & 
    col("quantity").isNotNull()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Exporta DeltaLake

# COMMAND ----------

PATH_SILVER = 'dbfs:/silver/delta/bike_store_sample_database/stocks'

dfStocks.write.format('delta').mode('overwrite').option('mergeSchema', 'true').save(PATH_SILVER)

# COMMAND ----------

del dfStocks
