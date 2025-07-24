# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text('init_load_flag', '1')
init_load_flag=int(dbutils.widgets.get('init_load_flag'))


# COMMAND ----------

df=spark.read.table("catalog_project.silver.customers")
df=df.dropDuplicates(subset=["customer_id"])
df.limit(10).display()

# COMMAND ----------

from pyspark.sql.functions import asc, monotonically_increasing_id, current_timestamp, lit, col
from delta.tables import DeltaTable

# === Step 1: Prepare base DataFrame from source ===
df_base = df.orderBy(asc('customer_id')) \
    .withColumn("DimCustomerKey", monotonically_increasing_id())

# === Step 2: Read existing target data or create empty for init load ===
if init_load_flag == 0:
    df_old = spark.read.table("catalog_project.gold.DimCustomers") \
               .select("DimCustomerKey", "customer_id", "create_date", "update_date")
else:
    df_old = spark.read.table("catalog_project.silver.customers") \
               .select("customer_id") \
               .limit(0) \
               .withColumn("DimCustomerKey", lit(None).cast("long")) \
               .withColumn("create_date", lit(None).cast("timestamp")) \
               .withColumn("update_date", lit(None).cast("timestamp"))

# === Step 3: Get max surrogate key if available ===
if init_load_flag == 0:
    df_maxsur = df_old.agg({"DimCustomerKey": "max"})
    max_surrogate_key = df_maxsur.collect()[0]["max(DimCustomerKey)"] or 0
else:
    max_surrogate_key = 0

# === Step 4: Offset surrogate key and add temp timestamps ===
df_base = df_base.withColumn("DimCustomerKey", lit(max_surrogate_key) + col("DimCustomerKey") + lit(1))

# === Step 5: Prepare old for join ===
df_old_renamed = df_old \
    .withColumnRenamed("DimCustomerKey", "old_DimCustomerKey") \
    .withColumnRenamed("customer_id", "old_customer_id") \
    .withColumnRenamed("create_date", "old_create_date") \
    .withColumnRenamed("update_date", "old_update_date")

# === Step 6: Join base data with existing records ===
df_join = df_base.join(df_old_renamed, df_base.customer_id == df_old_renamed.old_customer_id, "left")

# === Step 7: Separate new and existing records ===
df_new = df_join.filter(df_join["old_DimCustomerKey"].isNull()) \
    .withColumn("create_date", current_timestamp()) \
    .withColumn("update_date", current_timestamp())

df_existing = df_join.filter(df_join["old_DimCustomerKey"].isNotNull()) \
    .withColumn("create_date", col("old_create_date")) \
    .withColumn("update_date", current_timestamp()) \
    .withColumn("DimCustomerKey", col("old_DimCustomerKey"))

# === Step 8: Select consistent columns ===
df_new_final = df_new.select("DimCustomerKey", "customer_id", "create_date", "update_date")
df_existing_final = df_existing.select("DimCustomerKey", "customer_id", "create_date", "update_date")

# === Step 9: Union for final result ===
df_final = df_new_final.unionByName(df_existing_final)

# === Step 10: Merge into Delta Table ===
table_path = "abfss://gold@deproject911.dfs.core.windows.net/DimCustomers"
table_name = "catalog_project.gold.DimCustomers"

if spark.catalog.tableExists(table_name):
    delta_table = DeltaTable.forPath(spark, table_path)
    delta_table.alias("trg") \
        .merge(
            df_final.alias("src"),
            "trg.customer_id = src.customer_id"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
else:
    df_final.write.mode("overwrite") \
        .format("delta") \
        .option("path", table_path) \
        .saveAsTable(table_name)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from catalog_project.gold.dimcustomers;