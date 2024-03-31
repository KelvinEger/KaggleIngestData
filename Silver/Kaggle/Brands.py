# Databricks notebook source
# MAGIC %md
# MAGIC #1. Imports

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC #2. Define estrutura

# COMMAND ----------

oStructType = StructType([
  StructField("brand_id", IntegerType(), False),
  StructField("brand_name", StringType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Trata dados
# MAGIC - Remove valores nulos

# COMMAND ----------

dfBrands = spark.read.csv(
    '/bronze/bike_store_sample_database/brands.csv',
    schema = oStructType,
    header = True
)

# COMMAND ----------

dfBrands = dfBrands.filter(col("brand_id").isNotNull() & col("brand_name").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Exporta DeltaLake

# COMMAND ----------

PATH_SILVER = 'dbfs:/silver/delta/bike_store_sample_database/brands'

dfBrands.write.format('delta').mode('overwrite').option('mergeSchema', 'true').save(PATH_SILVER)

# COMMAND ----------

del dfBrands
