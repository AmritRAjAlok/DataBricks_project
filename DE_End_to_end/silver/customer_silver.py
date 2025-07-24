# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time

# COMMAND ----------

df=spark.read.format('parquet').load('abfss://bronze@deproject911.dfs.core.windows.net/customers')
df.display()

# COMMAND ----------

df1=df.drop("_rescued_data")
df1.display()

# COMMAND ----------

df1=df1.withColumn("domain",split(col("email"),"@")[1])
df1.show()

# COMMAND ----------

df1=df1.withColumn("full_name",concat(col("first_name"),lit(" "),col("last_name")))
df2=df1.drop("first_name","last_name")
df2.show()

# COMMAND ----------

df2.groupBy("domain").agg(count("domain").alias("total_domain_per")).sort('total_domain_per',ascending=False).display()

# COMMAND ----------

df_gmail =df2.filter(col('domain')=="gmail.com")
df_gmail.display()
time.sleep(5)

df_yahoo = df2.filter(col('domain')=="yahoo.com")
df_yahoo.display()
time.sleep(5)

df_hotmail = df2.filter(col('domain')=="hotmail.com")
df_hotmail.display()
time.sleep(5)

# COMMAND ----------

#write data at specific location
df.write.format("delta").mode("overwrite").save("abfss://silver@deproject911.dfs.core.windows.net/customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Register at metastore level
# MAGIC create table if not exists catalog_project.silver.customers
# MAGIC using delta
# MAGIC location "abfss://silver@deproject911.dfs.core.windows.net/customers";

# COMMAND ----------

# display(dbutils.fs.ls("abfss://silver@deproject911.dfs.core.windows.net/customers"))
# dbutils.fs.rm("abfss://silver@deproject911.dfs.core.windows.net/customers", recurse=True)



# COMMAND ----------

