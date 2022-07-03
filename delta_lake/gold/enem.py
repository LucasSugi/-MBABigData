# Databricks notebook source
# Pyspark
import pyspark.sql.functions as f

# COMMAND ----------

# MAGIC %run ../lib/delta_lake

# COMMAND ----------

# Set parameters
dbutils.widgets.text("bucket_name","")

# Get parameters
bucket_name = dbutils.widgets.get("bucket_name")

# COMMAND ----------

# Read files
df_enem = read_table(bucket_name,"generic+microdados_gov","silver","enem")

# COMMAND ----------

# Filter Rules
df_enem = (
  df_enem
  .filter(f.col("ESCOLA.NO_MUNICIPIO_ESC").isNotNull())
  .filter(f.col("ESCOLA.SG_UF_ESC").isNotNull())
  .filter(f.col("PRESENCA_PROVA.TP_PRESENCA_CN") == "Presente na prova")
  .filter(f.col("PRESENCA_PROVA.TP_PRESENCA_CH") == "Presente na prova")
  .filter(f.col("PRESENCA_PROVA.TP_PRESENCA_LC") == "Presente na prova")
  .filter(f.col("PRESENCA_PROVA.TP_PRESENCA_MT") == "Presente na prova")
)

# Seeed for id creation
seed_id = "wU0bRB"

# Select right columns
df_enem_base = (
  df_enem
  .select("ANO_PROVA","ESCOLA.NO_MUNICIPIO_ESC","ESCOLA.SG_UF_ESC","NOTA_PROVA","ACERTOS","REDACAO.NU_NOTA_REDACAO","PARTICIPANTE","QUESTIONARIO_SOCIO_ECONOMICO")
)

# COMMAND ----------

# Set id cidade ano
df_enem_base = set_id(df_enem_base,["ANO_PROVA","NO_MUNICIPIO_ESC","SG_UF_ESC"],seed_id,"ID_CIDADE_ANO")

# Set id participante
df_enem_base = set_id(df_enem_base,["PARTICIPANTE"],seed_id,"ID_PARTICIPANTE")

# Set id questionario
df_enem_base = set_id(df_enem_base,["QUESTIONARIO_SOCIO_ECONOMICO"],seed_id,"ID_QUESTIONARIO_SOCIO_ECONOMICO")

# COMMAND ----------

# Split into fact and dimension tables
df_enem_fact = df_enem_base.select("ID_CIDADE_ANO","ID_PARTICIPANTE","ID_QUESTIONARIO_SOCIO_ECONOMICO","NOTA_PROVA.*","ACERTOS.*","NU_NOTA_REDACAO")
df_enem_dimension_participante = df_enem_base.select("ID_PARTICIPANTE","PARTICIPANTE.*").distinct()
df_enem_dimension_questionario = df_enem_base.select("ID_QUESTIONARIO_SOCIO_ECONOMICO","QUESTIONARIO_SOCIO_ECONOMICO.*").distinct()

# COMMAND ----------

# Save (enem-fact)
write_table(
  df_enem_fact,
  bucket_name,
  "generic+microdados_gov",
  "gold",
  "enem-fact",
  options = {"overwriteSchema":"true"}
)

# Save (enem-dimension-participante)
write_table(
  df_enem_dimension_participante,
  bucket_name,
  "generic+microdados_gov",
  "gold",
  "enem-dimension-participante",
  options = {"overwriteSchema":"true"}
)

# Save (enem-dimension-questionario-socio-economico)
write_table(
  df_enem_dimension_questionario,
  bucket_name,
  "generic+microdados_gov",
  "gold",
  "enem-dimension-questionario-socio-economico",
  options = {"overwriteSchema":"true"}
)
