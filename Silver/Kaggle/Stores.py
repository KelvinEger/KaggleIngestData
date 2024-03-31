# Databricks notebook source
# MAGIC %md
# MAGIC #1. Imports

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC #2. Define a estrutura

# COMMAND ----------

oStructType = StructType([
    StructField("store_id", IntegerType(), False),
    StructField("store_name", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("email", StringType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
    StructField("zip_code", IntegerType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Trata dados
# MAGIC - Remove valores nulos

# COMMAND ----------

dfStore = spark.read.csv(
    '/bronze/bike_store_sample_database/stores.csv',
    schema = oStructType,
    header = True
)

# COMMAND ----------

dfStore = dfStore.filter(
    col("store_id").isNotNull() & 
    col("store_name").isNotNull() & 
    col("city").isNotNull() & 
    col("state").isNotNull()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Exporta DeltaLake

# COMMAND ----------

PATH_SILVER = 'dbfs:/silver/delta/bike_store_sample_database/stores'

dfStore.write.format('delta').mode('overwrite').option('mergeSchema', 'true').save(PATH_SILVER)

# COMMAND ----------

del dfStore
