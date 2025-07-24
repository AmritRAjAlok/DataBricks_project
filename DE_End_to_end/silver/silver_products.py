# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df=spark.read.format('parquet').load('abfss://bronze@deproject911.dfs.core.windows.net/products')
df1=df.drop('_rescued_data')
df1.display()

# COMMAND ----------

df1.createOrReplaceTempView("products")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from products;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION catalog_project.silver.dis_price(price DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC RETURN price * 0.90

# COMMAND ----------

df2=df1.withColumn('discounted_price',round(expr('catalog_project.silver.dis_price(price)'),2))
df2.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function catalog_project.silver.Brand(brand string)
# MAGIC returns string
# MAGIC language Python
# MAGIC  as
# MAGIC $$
# MAGIC      return brand.upper()
# MAGIC $$
# MAGIC
# MAGIC

# COMMAND ----------

df3=df2.withColumn('Brand',expr('catalog_project.silver.Brand(brand)'))
df4=df3.drop('brand')
df3.display()

# COMMAND ----------

df.write.format('delta').mode('overwrite').save('abfss://silver@deproject911.dfs.core.windows.net/products')

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table if not exists catalog_project.silver.products
# MAGIC using delta
# MAGIC location 'abfss://silver@deproject911.dfs.core.windows.net/products'