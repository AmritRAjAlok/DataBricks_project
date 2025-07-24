# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
df = spark.read.format("parquet")\
    .load('abfss://bronze@deproject911.dfs.core.windows.net/orders')
df.display()

# COMMAND ----------



from pyspark.sql.window import Window

df = df.withColumn("order_date", to_date('order_date'))
df = df.withColumn("order_year", year('order_date'))
df1 = df.drop('_rescued_data')
windowspec = Window.partitionBy('order_year').orderBy(desc(col('total_amount')))
df1 = df1.withColumn("flag", row_number().over(windowspec))
display(df1)

# COMMAND ----------

from pyspark.sql.window import Window
class Window:
    def rank(self,df):
        return df.withColumn("rank", row_number().over(windowspec))
    def denseRank(self,df):
        return df.withColumn("dense_rank", dense_rank().over(windowspec))
    def percentRank(self,df):
        return df.withColumn("percent_rank", percent_rank().over(windowspec))
    def cumeDist(self,df):
        return df.withColumn("cume_dist", cume_dist().over(windowspec))
    def ntile(self,df):
        return df.withColumn("ntile", ntile(5).over(windowspec))
    def rowNumber(self,df):
        return df.withColumn("row_number", row_number().over(windowspec))
w = Window()
w.rank(df1).display()
w.denseRank(df1).display()
w.percentRank(df1).display()
w.cumeDist(df1).display()
w.ntile(df1).display()
w.rowNumber(df1).display()



# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@deproject911.dfs.core.windows.net/orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists catalog_project.silver.orders 
# MAGIC using delta 
# MAGIC location "abfss://silver@deproject911.dfs.core.windows.net/orders"