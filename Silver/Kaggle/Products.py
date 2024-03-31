# Databricks notebook source
# MAGIC %md
# MAGIC #1. Imports

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC #2. Define a estrutura

# COMMAND ----------

oStructType = StructType([
    StructField("products_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("brand_id", IntegerType(), True),
    StructField("category_id", IntegerType(), True),
    StructField("model_year", IntegerType(), True),
    StructField("list_price", FloatType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Trata dados
# MAGIC - Remove valores nulos
# MAGIC - Filtra somente valores positivos

# COMMAND ----------

dfProducts = spark.read.csv(
    '/bronze/bike_store_sample_database/products.csv',
    schema = oStructType,
    header = True
)

# COMMAND ----------

dfProducts = dfProducts.filter(
    col("products_id").isNotNull() & 
    col("product_name").isNotNull() & 
    col("list_price").isNotNull() &
    (col("list_price") >= 0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Exporta DeltaLake

# COMMAND ----------

PATH_SILVER = 'dbfs:/silver/delta/bike_store_sample_database/products'

dfProducts.write.format('delta').mode('overwrite').option('mergeSchema', 'true').save(PATH_SILVER)

# COMMAND ----------

del dfProducts
