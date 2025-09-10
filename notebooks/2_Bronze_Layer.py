# Databricks notebook source
# Read your raw data
sales_df = spark.read.parquet("dbfs:/mnt/raw/sales_fact.parquet")
stores_df = spark.read.parquet("dbfs:/mnt/raw/store_info_india.parquet")
products_df = spark.read.parquet("dbfs:/mnt/raw/product_hierarchy_info.parquet")

# COMMAND ----------

# Add ingestion metadata
from pyspark.sql.functions import current_timestamp
sales_df = sales_df.withColumn("ingestion_date", current_timestamp())
stores_df = stores_df.withColumn("ingestion_date", current_timestamp())
products_df = products_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# Write to Delta Bronze layer
sales_df.write.format("delta").save("/mnt/bronze/sales_fact")
stores_df.write.format("delta").save("/mnt/bronze/store_info")
products_df.write.format("delta").save("/mnt/bronze/product_info")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/bronze"))