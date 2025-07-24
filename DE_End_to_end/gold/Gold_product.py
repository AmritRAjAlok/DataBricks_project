# Databricks notebook source

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

import dlt
@dlt.table(
    name="DimProducts_Stage",
    table_properties={
        "quality": "gold"
    }
)
def dim_products():
    # Define rules as a dictionary
    my_rules = {
        "rule1": "product_id IS NOT NULL",
        "rule2": "product_name IS NOT NULL"
    }

    # Read the source table
    df = spark.readStream.option("skipChangeCommits", "true").table('catalog_project.silver.products')

    # Apply rules using filter (simulating expect_or_drop behavior)
    for rule in my_rules.values():
        df = df.filter(expr(rule))

    return df

# COMMAND ----------

@dlt.view
def dim_products_view():
    return spark.readStream.table("Live.DimProducts_Stage")

# COMMAND ----------

dlt.create_streaming_table("DimProducts")
dlt.apply_changes(
  target = "DimProducts",
  source = "dim_products_view",
  keys = ["product_id"],
  sequence_by = "product_id",
  stored_as_scd_type = 2
)