# Databricks notebook source
# MAGIC %run "/Shared/Global retail Notebooks/REFINED LAYER"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists gold.consd_superstore
# MAGIC

# COMMAND ----------

target_table="gold.consd_superstore.dim_customer"
df_customer.write.mode("overwrite").options(
  overwriteSchema="True"
  ).saveAsTable(target_table)

# COMMAND ----------

from pyspark.sql.functions import col

target_table = "gold.consd_superstore.dim_order"
df_date.write.mode("overwrite").options(
  overwriteSchema="True"
).saveAsTable(target_table)

# COMMAND ----------

df_orders.display()

# COMMAND ----------

target_table="gold.consd_superstore.dim_product"
df_product.write.mode("overwrite").options(
  overwriteSchema="True"
  ).saveAsTable(target_table)


# COMMAND ----------

target_table="gold.consd_superstore.fact_sales"
drop_cols=['order_year','shipped_year','ship_date','order_date']
df_filter=df_filtered.select('order_id','product_id','customer_id','sales','discount','profit','quantity')
joined_df=final_df.join(df_date,"order_id","inner")
delivery_date_df=joined_df.withColumn('delivery_date',datediff(col('ship_date'),col('order_date')))
joined_df=delivery_date_df.drop(*drop_cols)
df_final=joined_df.groupBy('order_id','product_id','customer_id').agg(sum('sales').alias('sales'),sum('discount').alias('discount'),sum('profit').alias('profit'),sum('quantity').alias('quantity'),first("delivery_date").alias("delivery_date"))
df_final.write.mode('overwrite').options(
    mergeSchema="True",
    overwriteSchema="True"
).saveAsTable(target_table)

# COMMAND ----------

spark.sql("""
CREATE MATERIALIZED VIEW gold.consd_superstore.vw_fact_sales
AS SELECT * FROM gold.consd_superstore.fact_sales
""")