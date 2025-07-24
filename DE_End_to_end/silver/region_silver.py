# Databricks notebook source
df=spark.read.table("catalog_project.bronze.regions")
df.display()

# COMMAND ----------

df.display(100)
df.limit(100)

# COMMAND ----------

df1=df.drop('_rescued_data')


# COMMAND ----------

df1.display()

# COMMAND ----------

df.show()


# COMMAND ----------


df.write.format("delta")\
    .mode("overwrite")\
    .option("path","abfss://silver@deproject911.dfs.core.windows.net/regions")\
    .save()

# COMMAND ----------

df=spark.read.format("delta").load("abfss://silver@deproject911.dfs.core.windows.net/regions")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists catalog_project.silver.regions 
# MAGIC using delta 
# MAGIC location "abfss://silver@deproject911.dfs.core.windows.net/silver/regions";