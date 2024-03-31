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
    StructField("staff_id", IntegerType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("active", IntegerType(), False),
    StructField("store_id", IntegerType(), False),
    StructField("manager_id", IntegerType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Trata dados
# MAGIC - Remove valores nulos
# MAGIC - Remove registros inativos

# COMMAND ----------

dfStaff = spark.read.csv(
    '/bronze/bike_store_sample_database/staffs.csv',
    schema = oStructType,
    header = True
)

# COMMAND ----------

dfStaff = dfStaff.filter(
    col("staff_id").isNotNull() & 
    col("first_name").isNotNull() & 
    col("active").isNotNull() & 
    col("store_id").isNotNull() &
    (col("active") == 1)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Exporta DeltaLake

# COMMAND ----------

PATH_SILVER = 'dbfs:/silver/delta/bike_store_sample_database/staffs'

dfStaff.write.format('delta').mode('overwrite').option('mergeSchema', 'true').save(PATH_SILVER)

# COMMAND ----------

del dfStaff
