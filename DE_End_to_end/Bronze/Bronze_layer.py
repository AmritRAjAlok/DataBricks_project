# Databricks notebook source
df=spark.read.format("parquet")\
    .load("abfss://source@deproject911.dfs.core.windows.net/orders")
df.display()

# COMMAND ----------

df=spark.read.table('catalog_project.bronze.regions')
df.display()

# COMMAND ----------


import json
config_json=dbutils.notebook.run("dynamic_notebook",10000)
config=json.loads(config_json)

for item in config:
    name=item["file_name"]
    df=spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","parquet")\
        .option("cloudFiles.schemaLocation",f"abfss://source@deproject911.dfs.core.windows.net/checkpoint_{name}")\
        .load(f"abfss://source@deproject911.dfs.core.windows.net/{name}")

    df=df.writeStream.format("parquet")\
        .outputMode("append")\
        .option("checkpointLocation",f"abfss://source@deproject911.dfs.core.windows.net/checkpoint_{name}")\
        .option("path",f"abfss://bronze@deproject911.dfs.core.windows.net/{name}")\
        .trigger(once=True)\
        .start()
    df.awaitTermination()
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS catalog_project.bronze.{name}
        USING parquet
        LOCATION 'abfss://bronze@deproject911.dfs.core.windows.net/{name}'
    """)
    
    




# COMMAND ----------

spark.read.parquet(f"abfss://source@deproject911.dfs.core.windows.net/orders").printSchema()

# COMMAND ----------

df=spark.read.format('parquet').load('abfss://source@deproject911.dfs.core.windows.net/customers')
print(df.count())
