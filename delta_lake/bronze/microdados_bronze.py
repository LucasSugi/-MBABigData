# Databricks notebook source
# Imports
from os import path

# COMMAND ----------

# MAGIC %run ../lib/delta_lake

# COMMAND ----------

# Set parameters
dbutils.widgets.text("file","")

# Get input parameters
file = dbutils.widgets.get("file")

# COMMAND ----------

# Filepaths
filepath_transient = ""
filepath_bronze = ""

# COMMAND ----------

# Get files to move
files_to_move = get_files(file,filepath_transient)

# COMMAND ----------

# Move each file to bronze
for file in files_to_move:
  if("ENEM" in file):
    folder = "enem"
  elif("ITENS" in file):
    folder = "itens-prova"
  elif("ed_basica" in file):
    folder = "censo-escolar"
  else:
    raise Exception("file '{}' its invalid. We dont recognize the correct folder to store".format(file))
    
  print("Moving file from {} to {}".format(file,folder))
  dbutils.fs.mv(file,path.join(filepath_bronze,folder))