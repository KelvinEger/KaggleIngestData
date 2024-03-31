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
  StructField("category_id", IntegerType(), False),
  StructField("category_name", StringType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Trata dados
# MAGIC - Remove valores nulos

# COMMAND ----------

dfCategories = spark.read.csv(
  '/bronze/bike_store_sample_database/categories.csv',
  schema = oStructType,
  header = True
)

# COMMAND ----------

dfCategories = dfCategories.filter(
    col("category_id").isNotNull() & 
    col("category_name").isNotNull()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Exporta Delta lake

# COMMAND ----------

PATH_SILVER = 'dbfs:/silver/delta/bike_store_sample_database/categories'

dfCategories.write.format('delta').mode('overwrite').option('mergeSchema', 'true').save(PATH_SILVER)

# COMMAND ----------

del dfCategories
