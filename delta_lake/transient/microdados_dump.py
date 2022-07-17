# Databricks notebook source
# Imports
from os import listdir, path

# COMMAND ----------

# MAGIC %run ../lib/delta_lake

# COMMAND ----------

# Urls base
# url_enem = "https://download.inep.gov.br/microdados/microdados_enem_{}.zip"
# url_censo_escolar = "https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_{}.zip"

# Set input parameters
dbutils.widgets.text("url","")
dbutils.widgets.text("year","")
dbutils.widgets.text("filepath","")

# Get input parameters
url = dbutils.widgets.get("url")
year = dbutils.widgets.get("year")
filepath = dbutils.widgets.get("filepath")

# COMMAND ----------

# Create url
url_full = url.format(year)

# Filepaths to store zip files
filepath_transient = "/dbfs/mnt/microdados_gov/transient"
filepath_extract = path.join(filepath_transient,filepath.format(year))

# COMMAND ----------

# Dowload
print("Extracting data from {} to {}".format(url_full,filepath_extract))
download_to_transient(url_full,filepath_extract,False)

# COMMAND ----------

# Unzip
print("Unziping data {} to {}".format(filepath_extract,filepath_transient))
unzip_from_transient_to_bronze(filepath_extract,filepath_transient,"csv")
