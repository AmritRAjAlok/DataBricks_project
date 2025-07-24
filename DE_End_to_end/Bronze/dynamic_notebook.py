# Databricks notebook source
datasets = [
    {
        "file_name" : "orders"
    },
    {
        "file_name" : "customers"
    },
    {
        "file_name" : "products"
    }
]

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps(datasets))