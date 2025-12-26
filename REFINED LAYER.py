# Databricks notebook source
# MAGIC %md
# MAGIC ##### FETCHING DATA FROM BRONZE LAYER AND ENSURED CLEANED,DEDUP,CORRECT FORMAT DATA
# MAGIC

# COMMAND ----------

#fetching raw data from bronze catalog 
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

raw_df=df = spark.table("BRONZE.SUPERSTORE.RAW_SUPERSTORE_TABLE")

# COMMAND ----------

df_cleaned=(raw_df
            .withColumn("quantity", regexp_replace(col("quantity"), "[^0-9]", ""))
            .withColumn("order_id",substring_index(col("order_id"),"-",-1))
            .withColumn("customer_id",substring_index(col("customer_id"),"-",-1))
            .withColumn("sales", regexp_replace(col("sales"), "[^0-9]", ""))
            .withColumn("discount", regexp_replace(col("discount"), "[^0-9]", ""))
            .withColumn("product_id",substring_index(col("product_id"),"-",-1))
            .withColumn("order_year",year(col("order_date"))).withColumn("shipped_year",year(col("ship_date")))
            )
                       
df_cast = df_cleaned.withColumn(
    "quantity", expr("try_cast(quantity as float)")
).withColumn(
    "sales", expr("try_cast(sales as float)")
).withColumn(
    "discount", expr("try_cast(discount as float)")
).withColumn(
    "ship_date",
    col("ship_date").cast("timestamp")
).withColumn(
    "order_date",
    col("order_date").cast("timestamp")
)

df_filtered=df_cast.filter(col("quantity")>=0)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### DATA MODELS CREATION AND WRITE INTO DIFFERENT TABLES

# COMMAND ----------

cols=["customer_id","customer_name","country","state","region","city"]
df_customer=df_filtered.select(*cols)
target_table="silver.rfnd_superstore.rfnd_customer_table"

#assigning rank to all rows to remove duplicates
window_spec = Window.partitionBy("customer_id","customer_name","country","state","region","city")  # specify columns
df_customer=df_customer.withColumn('dup_count', count('*').over(window_spec)) \
  .filter(col('dup_count') == 1)
df_customer=df_customer.drop('dup_count')
df_customer.write.mode("overwrite").option("overwriteSchema","true").saveAsTable(target_table)

# COMMAND ----------

prod_cols=["product_id","category","sub_category","product_name","segment"]
df_product=df_filtered.select(*prod_cols)
target_table="silver.rfnd_superstore.rfnd_product_table"
window_spec = Window.partitionBy(prod_cols)  # specify columns
df_product=df_product.withColumn('dup_count', count('*').over(window_spec)) \
  .filter(col('dup_count') == 1).drop(col("dup_count"))
df_product.write.mode("overwrite").options(
  overwriteSchema="True"
).saveAsTable(target_table)


# COMMAND ----------

date_cols=["order_id","order_date","ship_date","order_year","shipped_year"]
df_date=df_filtered.select(*date_cols)
target_table="silver.rfnd_superstore.rfnd_date_table"
window_spec = Window.partitionBy(date_cols)
df_date=df_date.withColumn('dup_count', count('*').over(window_spec)) \
  .filter(col('dup_count') == 1).drop("dup_count")
df_date.write.mode("overwrite").options(
  overwriteSchema="True"
  ).saveAsTable(target_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC desc table silver.rfnd_superstore.rfnd_date_table