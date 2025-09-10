# Databricks notebook source
from pyspark.sql.functions import col, regexp_extract, when, to_date
from pyspark.sql.types import IntegerType, DoubleType

# COMMAND ----------

# Read the Bronze layer data from its current location in DBFS
sales_bronze_df = spark.read.format("delta").load("dbfs:/mnt/bronze/sales_fact")
stores_bronze_df = spark.read.format("delta").load("dbfs:/mnt/bronze/store_info")
products_bronze_df = spark.read.format("delta").load("dbfs:/mnt/bronze/product_info")

# COMMAND ----------

# Covert all columns to lower case
stores_bronze_df = stores_bronze_df.toDF(*[c.lower() for c in stores_bronze_df.columns])
sales_bronze_df = sales_bronze_df.toDF(*[c.lower() for c in sales_bronze_df.columns])
products_bronze_df = products_bronze_df.toDF(*[c.lower() for c in products_bronze_df.columns])

# Separate valid and invalid store_id (integer and not null)
stores_valid = stores_bronze_df.filter(col("store_id").isNotNull() & col("store_id").cast(IntegerType()).isNotNull())
store_null = stores_bronze_df.filter(col("store_id").isNull() | ~col("store_id").cast(IntegerType()).isNotNull())

# Separate valid and invalid product_id (not null and matches pattern)
products_valid = products_bronze_df.filter(
    (col("item_id").isNotNull()) & (regexp_extract(col("item_id"), r'^ITM\d+$', 0) != "")
)
products_null = products_bronze_df.filter(
    col("item_id").isNull() | (regexp_extract(col("item_id"), r'^ITM\d+$', 0) == "")
)

# Separate valid and invalid rows (key fields not null and integer)
sales_valid = sales_bronze_df.filter(
    col("sales_id").isNotNull() & col("sales_id").cast(IntegerType()).isNotNull() &
    col("store_id").isNotNull() & col("store_id").cast(IntegerType()).isNotNull() &
    col("product_id").isNotNull() & col("product_id").cast(IntegerType()).isNotNull() &
    col("period_start").isNotNull() & 
    col("period_end").isNotNull()
)
sales_null = sales_bronze_df.filter(
    col("sales_id").isNull() | ~col("sales_id").cast(IntegerType()).isNotNull() |
    col("store_id").isNull() | ~col("store_id").cast(IntegerType()).isNotNull() |
    col("product_id").isNull() | ~col("product_id").cast(IntegerType()).isNotNull() |
    col("period_start").isNull() | 
    col("period_end").isNull()
)

# COMMAND ----------

silver_stores = stores_valid.withColumn("store_state", 
                                        when(col("store_state").isNull(), "Others").otherwise(col("store_state")))\
                            .withColumn("store_district",
                                        when(col("store_district").isNull(), "Others").otherwise(col("store_district")))\
                            .withColumn("store_name", col("store_name").cast("string"))\
                            .withColumn("store_addr", col("store_addr").cast("string"))\
                            .withColumn("store_district", col("store_district").cast("string"))\
                            .withColumn("store_state", col("store_state").cast("string"))\
                            .withColumn("store_country", col("store_country").cast("string"))\
                            .dropDuplicates(["store_id"])

silver_products = products_valid.withColumnRenamed("item_id", "product_id")\
                                .withColumnRenamed("item_name", "item")\
                                .withColumn('product_id', col('product_id').substr(4, 100).cast(IntegerType()))\
                                .withColumn("department", when(col("department").isNull(), "Not Available").otherwise(col("department")))\
                                .withColumn("category", when(col("category").isNull(), "Not Available").otherwise(col("category")))\
                                .withColumn("department", col("department").cast("string"))\
                                .withColumn("category", col("category").cast("string"))\
                                .withColumn("item", col("item").cast("string"))\
                                .dropDuplicates(["product_id"])


silver_sales = (
    sales_valid
    .withColumn("sales_amount", col("sales_amount").cast(DoubleType()))
    .withColumn("sales_units", col("sales_units").cast(DoubleType()))
    .withColumn("period_start", to_date("period_start"))
    .withColumn("period_end", to_date("period_end"))
    .withColumn("product_id", col("product_id").cast(IntegerType()))
    .withColumn("sales_amount", when(col("sales_amount").isNull(), 0.0).otherwise(col("sales_amount")))
    .withColumn("sales_units", when(col("sales_units").isNull(), 0.0).otherwise(col("sales_units")))
    .dropDuplicates(["sales_id"])
)

# COMMAND ----------

# Write clean silver tables
silver_stores.write.format("delta").mode("overwrite").save("dbfs:/mnt/silver/stores")
silver_products.write.format("delta").mode("overwrite").save("dbfs:/mnt/silver/products")
silver_sales.write.format("delta").mode("overwrite").save("dbfs:/mnt/silver/sales")

# COMMAND ----------

# Write quarantine tables for analysis
store_null.write.format("delta").mode("overwrite").save("dbfs:/mnt/quarantine/stores_invalid_id")
products_null.write.format("delta").mode("overwrite").save("dbfs:/mnt/quarantine/products_invalid_id")
sales_null.write.format("delta").mode("overwrite").save("dbfs:/mnt/quarantine/sales_invalid_id")

# COMMAND ----------

