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
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("email", StringType(), False),
    StructField("street", StringType(), True),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
    StructField("zip_code", IntegerType(), False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Trata dados
# MAGIC - Remove valores nulos

# COMMAND ----------

dfCustomers = spark.read.csv(
    '/bronze/bike_store_sample_database/customers.csv',
    schema = oStructType,
    header = True
)

# COMMAND ----------

dfCustomers = dfCustomers.filter(
    col('customer_id').isNotNull() &
    col('first_name').isNotNull() &
    col('email').isNotNull() &
    col('city').isNotNull() &
    col('state').isNotNull() &
    col('zip_code').isNotNull()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Exporta DeltaLake

# COMMAND ----------

PATH_SILVER = 'dbfs:/silver/delta/bike_store_sample_database/customers'

dfCustomers.write.format('delta').mode('overwrite').option('mergeSchema', 'true').save(PATH_SILVER)

# COMMAND ----------

del dfCustomers
