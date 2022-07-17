# Databricks notebook source
# Imports
from os import path

# COMMAND ----------

# MAGIC %run ../lib/delta_lake

# COMMAND ----------

# Set parameters
dbutils.widgets.text("file","")
dbutils.widgets.text("bucket_name","")

# Get input parameters
file = dbutils.widgets.get("file")
bucket_name = dbutils.widgets.get("bucket_name")

# COMMAND ----------

# Filepaths
filepath_transient = get_filepath(bucket_name,"generic+microdados_gov","transient","")
filepath_bronze = get_filepath(bucket_name,"generic+microdados_gov","bronze","")

# COMMAND ----------

# Get files to move
files_to_move = get_files(file,filepath_transient)

# COMMAND ----------

# Move each file to bronze
for file in files_to_move:
  if("enem" in file.lower()):
    folder = "enem"
  elif("itens" in file.lower()):
    folder = "itens-prova"
  elif("ed_basica" in file.lower()):
    folder = "censo-escolar"
  else:
    raise Exception("file '{}' its invalid. We dont recognize the correct folder to store".format(file))
  
  # Move
  move_file(file,path.join(filepath_bronze,folder))
