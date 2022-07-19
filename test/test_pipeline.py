# Databricks notebook source
# Time
from time import time

# COMMAND ----------

# Set parameters
dbutils.widgets.text("bucket_name","")

# Get parameters
bucket_name = dbutils.widgets.get("bucket_name")

# COMMAND ----------

for i in range(5):

  # Start time
  start = time()

  for year in ["2016","2017","2018","2019","2020","2021"]:

    # Run silver
    dbutils.notebook.run(path = "../silver/itens_prova", timeout_seconds = 0, arguments = {"bucket_name":bucket_name,"year":year})
    dbutils.notebook.run(path = "../silver/censo_escolar", timeout_seconds = 0, arguments = {"bucket_name":bucket_name,"year":year})
    dbutils.notebook.run(path = "../silver/enem", timeout_seconds = 0, arguments = {"bucket_name":bucket_name,"year":year})

  # Run gold
  dbutils.notebook.run(path = "../gold/censo_escolar", timeout_seconds = 0, arguments = {"bucket_name":bucket_name})
  dbutils.notebook.run(path = "../gold/enem", timeout_seconds = 0, arguments = {"bucket_name":bucket_name})

  # End time
  end = time()

  # Elapsed time (minutes)
  elapsed = round((end-start) / 60,1)

  print(f"Took: {elapsed} min")

  # Clear cache
  spark.catalog.clearCache()