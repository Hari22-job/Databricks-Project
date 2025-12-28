# Databricks notebook source
#extracting the data from volume
from pyspark.sql.functions import*

file_path="/Volumes/bronze/superstore/raw_superstore/Sample - Superstore.csv"

df=spark.read.format('csv').option("header","true").option("inferschema","true").load(file_path)
df=df.withColumn("ingestion_time",current_timestamp())
cleaned_df = (
    df.withColumnRenamed('Customer ID', 'customer_id')
      .withColumnRenamed('Ship Mode', 'ship_mode')
      .withColumnRenamed('Customer Name', 'customer_name')
      .withColumnRenamed('Segment', 'segment')
      .withColumnRenamed('Country', 'country')
      .withColumnRenamed('City', 'city')
      .withColumnRenamed('State', 'state')
      .withColumnRenamed('Postal Code', 'postal_code')
      .withColumnRenamed('Region', 'region')
      .withColumnRenamed('Product ID', 'product_id')
      .withColumnRenamed('Category', 'category')
      .withColumnRenamed('Sub-Category', 'sub_category')
      .withColumnRenamed('Product Name', 'product_name')
      .withColumnRenamed('Sales', 'sales')
      .withColumnRenamed('Quantity', 'quantity')
      .withColumnRenamed('Discount', 'discount')
      .withColumnRenamed('Profit', 'profit')
      .withColumnRenamed('Order Date', 'order_date')
      .withColumnRenamed('Ship Date', 'ship_date')
      .withColumnRenamed('Order ID', 'order_id')
      .withColumnRenamed('Row ID', 'row_id')
)
#converting into delta table
target_table="BRONZE.SUPERSTORE.RAW_SUPERSTORE_TABLE"
cleaned_df.write.format('delta').mode('overwrite').options(
  mergeSchema="True",
  overwriteSchema="True"
).saveAsTable(target_table)


# COMMAND ----------

#I am able to read in the same file without creating duplicate records
#I see both new records and existing records when I upload a new file 

#USE MERGE COMMAND 
from delta.tables import DeltaTable

print(f"starting merge operation on {target_table}")

delta_table = DeltaTable.forName(spark, target_table)
delta_table.alias("target").merge(
    cleaned_df.alias("source"),"target.row_id=source.row_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
print(f"merge operation completed successfully")




# COMMAND ----------

 
df = spark.table("BRONZE.SUPERSTORE.RAW_SUPERSTORE_TABLE")
display(df)