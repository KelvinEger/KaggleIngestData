# Databricks notebook source
# MAGIC %md
# MAGIC #1. Imports

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, expr, to_date, date_format

# COMMAND ----------

# MAGIC %md
# MAGIC #2. Define estrutura

# COMMAND ----------

oStructType = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_status", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("required_date", StringType(), True),
    StructField("shipped_date", StringType(), False),
    StructField("store_id", IntegerType(), False),
    StructField("staff_id", IntegerType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Trata dados
# MAGIC - Adicionada coluna calculando diferença entre datas considerando a data de compra e a data de envio
# MAGIC - Remove valores nulos
# MAGIC - Adiciona coluna com competência (mes/ano) 

# COMMAND ----------

dfOrders = spark.read.csv(
    '/bronze/bike_store_sample_database/orders.csv',
    schema = oStructType,
    header = True
)

# COMMAND ----------

dfOrders = (dfOrders
    .withColumn('order_date', to_date(col('order_date'), 'yyyy-MM-dd'))
    .withColumn('shipped_date', to_date(col('shipped_date'), 'yyyy-MM-dd'))
    .withColumn('required_date', to_date(col('shipped_date'), 'yyyy-MM-dd'))
    .withColumn('delivery_span', (col('shipped_date') - col('order_date')).cast('integer'))
    .withColumn('order_compet', date_format("order_date", "yyyy/MM"))
    .filter(
        col("order_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("shipped_date").isNotNull() &
        col("store_id").isNotNull()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Exporta DeltaLake

# COMMAND ----------

PATH_SILVER = 'dbfs:/silver/delta/bike_store_sample_database/orders'

dfOrders.write.format('delta').mode('overwrite').option('mergeSchema', 'true').save(PATH_SILVER)

# COMMAND ----------

del dfOrders
