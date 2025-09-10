# Databricks notebook source
# Generate date range for the next 10 years
from pyspark.sql.functions import sequence, to_date, lit, col, year, month, dayofmonth

# COMMAND ----------

date_df = spark.sql("""
    SELECT
        row_number() over (order by date) as period_id,
        LAG(date) over (order BY date) + 1 as period_start_date,
        date as period_end_date
    FROM (
        SELECT explode(sequence(to_date('2020-01-05'), to_date('2030-12-31'), interval 7 day)) as date
    )
""")
date_df.write.format("delta").save("dbfs:/mnt/gold/dim_date")

# COMMAND ----------

from pyspark.sql.functions import lit, monotonically_increasing_id

products = spark.read.format("delta").load("dbfs:/mnt/silver/products")

dept_level = products.select("department").dropDuplicates() \
    .withColumn("level", lit("department")) \
    .withColumn("category", lit(None)) \
    .withColumn("item", lit(None)) \
    .withColumn("product_id", lit(None))

cat_level = products.select("department", "category").dropDuplicates() \
    .withColumn("level", lit("category")) \
    .withColumn("item", lit(None)) \
    .withColumn("product_id", lit(None))

item_level = products.select("department", "category", "item", "product_id") \
    .dropDuplicates() \
    .withColumn("level", lit("item"))

product_hierarchy = dept_level.unionByName(cat_level).unionByName(item_level)

product_hierarchy = product_hierarchy.withColumn("hierarchy_id", monotonically_increasing_id())

product_hierarchy = product_hierarchy.select("hierarchy_id","department", "category", "item", "product_id", "level")

# Write the dimension table
product_hierarchy.write.format("delta").mode("overwrite").save("dbfs:/mnt/gold/dim_product")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Read the silver stores table
stores = spark.read.format("delta").load("dbfs:/mnt/silver/stores")

# Build geography hierarchy
country_level = stores.select("store_country").dropDuplicates() \
    .withColumn("level", lit("country")) \
    .withColumn("store_state", lit(None)) \
    .withColumn("store_district", lit(None))

state_level = stores.select("store_country", "store_state").dropDuplicates() \
    .withColumn("level", lit("state")) \
    .withColumn("store_district", lit(None))

district_level = stores.select("store_country", "store_state", "store_district").dropDuplicates() \
    .withColumn("level", lit("district"))

geo_hierarchy = country_level.unionByName(state_level).unionByName(district_level)

window = Window.orderBy("store_country", "store_state", "store_district")
# product_hierarchy = product_hierarchy.withColumn("hierarchy_id", row_number().over(window))
geo_hierarchy = geo_hierarchy.withColumn("geography_id",  row_number().over(window))


geo_hierarchy = geo_hierarchy.select("geography_id","store_country", "store_state", "store_district", "level")

# Write the dimension table
geo_hierarchy.write.format("delta").mode("overwrite").save("dbfs:/mnt/gold/dim_geography")

# COMMAND ----------

display(geo_hierarchy)

# COMMAND ----------

# Create a small dataframe manually
geo_levels = spark.createDataFrame(
    [("country", 1),
     ("state", 2),
     ("district", 3)],
    ["level", "level_order"]
)
geo_levels = geo_levels.select("level_order", "level")

# Save to Gold
geo_levels.write.format("delta") \
    .mode("overwrite") \
    .save("dbfs:/mnt/gold/dim_geo_level")


# COMMAND ----------

display(geo_levels)

# COMMAND ----------



# COMMAND ----------

## TODO: 
# add the hierarchy ID to product hierarchy table - Done
# 





## DIM_1 is GEOLEVEL - Done
## State | District

## DIM_2 is PERIOD - done
## DIM_3 is Product Hierarchy - done

## DIM_4 is Region - Done
## Value as per GEO LEVEL which is linked to DIM_1 ID

# COMMAND ----------

# # 1. CREATE SILVER SCHEMA AND TABLES
# spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
# spark.sql("CREATE TABLE IF NOT EXISTS silver.sales USING DELTA LOCATION 'dbfs:/mnt/silver/sales'")
# spark.sql("CREATE TABLE IF NOT EXISTS silver.products USING DELTA LOCATION 'dbfs:/mnt/silver/products'")
# spark.sql("CREATE TABLE IF NOT EXISTS silver.stores USING DELTA LOCATION 'dbfs:/mnt/silver/stores'")

# # 2. CREATE GOLD SCHEMA AND DIMENSION TABLES
# spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
# spark.sql("CREATE TABLE IF NOT EXISTS gold.dim_date USING DELTA LOCATION 'dbfs:/mnt/gold/dim_date'")
# spark.sql("CREATE TABLE IF NOT EXISTS gold.dim_product USING DELTA LOCATION 'dbfs:/mnt/gold/dim_product'")
# spark.sql("CREATE TABLE IF NOT EXISTS gold.dim_geography USING DELTA LOCATION 'dbfs:/mnt/gold/dim_geography'")
# spark.sql("CREATE TABLE IF NOT EXISTS gold.dim_geo_level USING DELTA LOCATION 'dbfs:/mnt/gold/dim_geo_level'")
# # spark.sql("CREATE TABLE IF NOT EXISTS gold.unified_geo_table USING DELTA LOCATION 'dbfs:/mnt/gold/unified_geo_table'")

# print("All tables registered successfully!")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- create table gold.lkp_store_geography (geography_id int, store_id int)
# MAGIC -- INSERT INTO gold.lkp_store_geography (geography_id, store_id)
# MAGIC -- SELECT 
# MAGIC --     geo.geography_id,
# MAGIC --     sto.store_id
# MAGIC -- FROM 
# MAGIC --     gold.dim_geography geo
# MAGIC -- JOIN 
# MAGIC --     silver.stores sto ON (
# MAGIC --         (geo.level = 'country' AND geo.store_country = sto.store_country) OR
# MAGIC --         (geo.level = 'state' AND geo.store_country = sto.store_country 
# MAGIC --          AND geo.store_state = sto.store_state) OR
# MAGIC --         (geo.level = 'district' AND geo.store_country = sto.store_country 
# MAGIC --          AND geo.store_state = sto.store_state 
# MAGIC --          AND geo.store_district = sto.store_district)
# MAGIC --     );

# COMMAND ----------

# %sql

# create table gold.lkp_product_hierarchy (hierarchy_id bigint, product_id bigint);
# INSERT INTO gold.lkp_product_hierarchy (hierarchy_id, product_id)
# SELECT 
#     geo.hierarchy_id,
#     sto.product_id
# FROM 
#     gold.dim_product geo
# JOIN 
#     silver.products sto ON (
#         (geo.level = 'department' AND geo.department = sto.department) OR
#         (geo.level = 'category' AND geo.category = sto.category 
#          AND geo.department = sto.department) OR
#         (geo.level = 'item' AND geo.category = sto.category 
#          AND geo.department = sto.department
#          AND geo.product_id = sto.product_id) 
#     );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from gold.lkp_product_hierarchy
# MAGIC where hierarchy_id = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from gold.lkp_store_geography

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from silver.sales s
# MAGIC join gold.dim_date p
# MAGIC on s.period_start = p.period_start_date and s.period_end = p.period_end_date
# MAGIC where period_id = 284

# COMMAND ----------

# MAGIC %sql
# MAGIC select geography_id, h.hierarchy_id, period_id, sum(sales_amount) as sales, sum(sales_units) as units
# MAGIC from silver.sales s
# MAGIC join gold.dim_date p
# MAGIC on s.period_start = p.period_start_date and s.period_end = p.period_end_date
# MAGIC join gold.lkp_store_geography g
# MAGIC on s.store_id = g.store_id
# MAGIC join gold.lkp_product_hierarchy h
# MAGIC on s.product_id = h.product_id
# MAGIC group by geography_id, h.hierarchy_id, period_id

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte_date as (
# MAGIC SELECT 
# MAGIC     period_id,
# MAGIC     period_start_date,
# MAGIC     period_end_date,
# MAGIC     YEAR(period_start_date)  AS year,
# MAGIC     WEEKOFYEAR(period_start_date) AS week_num
# MAGIC FROM gold.dim_date),
# MAGIC cte_date2 as (
# MAGIC select d2.period_id as period_id, d1.period_id as period_id_ya
# MAGIC from cte_date d1
# MAGIC join cte_date d2
# MAGIC on  d1.week_num = d2.week_num
# MAGIC and  d1.year = d2.year + 1)
# MAGIC select * 
# MAGIC from cte_date2

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte_date AS (
# MAGIC     SELECT 
# MAGIC         period_id,
# MAGIC         period_start_date,
# MAGIC         period_end_date,
# MAGIC         YEAR(period_start_date)  AS year,
# MAGIC         WEEKOFYEAR(period_start_date) AS week_num
# MAGIC     FROM gold.dim_date
# MAGIC ),
# MAGIC cte_date2 AS (
# MAGIC     SELECT d2.period_id AS period_id, d1.period_id AS period_id_ya
# MAGIC     FROM cte_date d1
# MAGIC     JOIN cte_date d2
# MAGIC       ON d1.week_num = d2.week_num
# MAGIC      AND d1.year = d2.year + 1
# MAGIC ),
# MAGIC cte_fact_sales AS (
# MAGIC     SELECT 
# MAGIC         geography_id, 
# MAGIC         h.hierarchy_id, 
# MAGIC         period_id, 
# MAGIC         SUM(s.sales_amount) AS sales, 
# MAGIC         SUM(s.sales_units) AS units
# MAGIC     FROM silver.sales s
# MAGIC     JOIN gold.dim_date p
# MAGIC       ON s.period_start = p.period_start_date 
# MAGIC      AND s.period_end   = p.period_end_date
# MAGIC     JOIN gold.lkp_store_geography g
# MAGIC       ON s.store_id = g.store_id
# MAGIC     JOIN gold.lkp_product_hierarchy h
# MAGIC       ON s.product_id = h.product_id
# MAGIC     GROUP BY geography_id, h.hierarchy_id, period_id
# MAGIC )
# MAGIC SELECT 
# MAGIC     fct.geography_id, 
# MAGIC     fct.hierarchy_id, 
# MAGIC     fct.period_id, 
# MAGIC     fct.sales, 
# MAGIC     fct.units, 
# MAGIC     coalesce(fct_ya.sales,0) AS sales_ya, 
# MAGIC     coalesce(fct_ya.units,0) AS units_ya,
# MAGIC     ((fct.sales - coalesce(fct_ya.sales,0)) / NULLIF(fct_ya.sales,0)) * 100 AS sales_growth_pct,
# MAGIC     ((fct.units - coalesce(fct_ya.units,0)) / NULLIF(fct_ya.units,0)) * 100 AS units_growth_pct
# MAGIC FROM cte_fact_sales fct
# MAGIC JOIN cte_date2 dt2
# MAGIC   ON fct.period_id = dt2.period_id
# MAGIC LEFT JOIN cte_fact_sales fct_ya
# MAGIC   ON fct_ya.period_id   = dt2.period_id_ya
# MAGIC  AND fct.geography_id   = fct_ya.geography_id
# MAGIC  AND fct.hierarchy_id   = fct_ya.hierarchy_id;
# MAGIC

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE gold.fct_sales
# USING DELTA
# AS
# WITH cte_date AS (
#     SELECT 
#         period_id,
#         period_start_date,
#         period_end_date,
#         YEAR(period_start_date)  AS year,
#         WEEKOFYEAR(period_start_date) AS week_num
#     FROM gold.dim_date
# ),
# cte_date2 AS (
#     SELECT d2.period_id AS period_id, d1.period_id AS period_id_ya
#     FROM cte_date d1
#     JOIN cte_date d2
#       ON d1.week_num = d2.week_num
#      AND d1.year = d2.year + 1
# ),
# cte_fact_sales AS (
#     SELECT 
#         geography_id, 
#         h.hierarchy_id, 
#         period_id, 
#         SUM(s.sales_amount) AS sales, 
#         SUM(s.sales_units)  AS units,
#         COUNT(DISTINCT s.store_id) AS stores_selling
#     FROM silver.sales s
#     JOIN gold.dim_date p
#       ON s.period_start = p.period_start_date 
#      AND s.period_end   = p.period_end_date
#     JOIN gold.lkp_store_geography g
#       ON s.store_id = g.store_id
#     JOIN gold.lkp_product_hierarchy h
#       ON s.product_id = h.product_id
#     GROUP BY geography_id, h.hierarchy_id, period_id
# ),
# total_stores AS (
#     SELECT geography_id, COUNT(DISTINCT store_id) AS total_stores
#     FROM gold.lkp_store_geography
#     GROUP BY geography_id
# )
# SELECT 
#     fct.geography_id, 
#     fct.hierarchy_id, 
#     fct.period_id, 
#     fct.sales, 
#     fct.units, 
#     coalesce(fct_ya.sales,0) AS sales_ya, 
#     coalesce(fct_ya.units,0) AS units_ya,
#     ROUND(((fct.sales - coalesce(fct_ya.sales,0)) / NULLIF(fct_ya.sales,0)) * 100,2) AS sales_growth_pct,
#     ROUND(((fct.units - coalesce(fct_ya.units,0)) / NULLIF(fct_ya.units,0)) * 100,2) AS units_growth_pct,
#     fct.stores_selling,
#     coalesce(fct_ya.stores_selling,0) AS stores_selling_ya,
#     ts.total_stores,
#     ROUND(fct.stores_selling * 100.0 / ts.total_stores, 2) AS store_penetration_pct,
#     ROUND(coalesce(fct_ya.stores_selling,0) * 100.0 / ts.total_stores, 2) AS store_penetration_pct_ya,
#     ROUND(((fct.stores_selling - coalesce(fct_ya.stores_selling,0)) / NULLIF(fct_ya.stores_selling,0)) * 100,2) AS stores_growth_pct
# FROM cte_fact_sales fct
# JOIN cte_date2 dt2
#   ON fct.period_id = dt2.period_id
# LEFT JOIN cte_fact_sales fct_ya
#   ON fct_ya.period_id   = dt2.period_id_ya
#  AND fct.geography_id   = fct_ya.geography_id
#  AND fct.hierarchy_id   = fct_ya.hierarchy_id
# LEFT JOIN total_stores ts
#   ON fct.geography_id = ts.geography_id;


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.fct_sales
# MAGIC where geography_id = 1 and hierarchy_id = 0 and period_id = 284

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from gold.dim_date
# MAGIC where period_id = 284
# MAGIC
# MAGIC --Stationery & Supplies
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte_date as (
# MAGIC SELECT 
# MAGIC     period_id,
# MAGIC     period_start_date,
# MAGIC     period_end_date,
# MAGIC     YEAR(period_start_date)  AS year,
# MAGIC     WEEKOFYEAR(period_start_date) AS week_num
# MAGIC FROM gold.dim_date),
# MAGIC cte_date2 as (
# MAGIC select d2.period_id, d1.period_id as period_ya
# MAGIC from cte_date d1
# MAGIC join cte_date d2
# MAGIC on  d1.week_num = d2.week_num
# MAGIC and  d1.year = d2.year + 1)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from gold.dim_geography

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from gold.lkp_store_geography
# MAGIC where store_id = 14

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from silver.sales where store_id in (
# MAGIC select store_id
# MAGIC from silver.stores
# MAGIC where store_district = 'Lucknow')
