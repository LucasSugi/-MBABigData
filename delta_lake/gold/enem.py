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
df_enem_fact = df_enem_base.select("ID_CIDADE_ANO","ID_PARTICIPANTE","ID_QUESTIONARIO_SOCIO_ECONOMICO","ANO_PROVA","NOTA_PROVA.*","ACERTOS.*","NU_NOTA_REDACAO")
df_enem_dimension_participante = df_enem_base.select("ID_PARTICIPANTE","PARTICIPANTE.*").distinct()
df_enem_dimension_questionario = df_enem_base.select("ID_QUESTIONARIO_SOCIO_ECONOMICO","QUESTIONARIO_SOCIO_ECONOMICO.*").distinct()

# COMMAND ----------

# Columns to rename
notas_columns = {"NU_NOTA_CN":"NOTA_CN","NU_NOTA_CH":"NOTA_CH","NU_NOTA_LC":"NOTA_LC","NU_NOTA_MT":"NOTA_MT","NU_NOTA_REDACAO":"NOTA_REDACAO"}
participante_columns = {"TP_FAIXA_ETARIA": "FAIXA_ETARIA","TP_SEXO": "SEXO","TP_ESTADO_CIVIL": "ESTADO_CIVIL","TP_COR_RACA": "COR_RACA","TP_NACIONALIDADE": "NACIONALIDADE","TP_ST_CONCLUSAO": "CONCLUSAO_ENSINO_MEDIO","TP_ANO_CONCLUIU": "ANO_CONCLUSAO_ENSINO_MEDIO","TP_ESCOLA": "TIPO_ESCOLA","TP_ENSINO": "TIPO_ENSINO","IN_TREINEIRO": "TREINEIRO"}
questionario_columns = {"Q001": "SERIE_PAI_ESTUDOU","Q002": "SERIE_MAE_ESTUDOU","Q003": "OCUPACAO_PAI","Q004": "OCUPACAO_MAE","Q005": "PESSOAS_NA_RESIDENCIA","Q006": "RENDA_FAMILIAR","Q007": "POSSUI_EMPREGADO_DOMESTICO","Q008": "RESIDENCIA_POSSUI_BANHEIRO","Q009": "RESIDENCIA_POSSUI_QUARTOS","Q010": "POSSUI_CARROS","Q011": "POSSUI_MOTOCICLETA","Q012": "POSSUI_GELADEIRA","Q013": "POSSUI_FREEZER","Q014": "POSSUI_MAQUINA_LAVAR","Q015": "POSSUI_MAQUINA_SECAR","Q016": "POSSUI_MICRO_ONDAS","Q017": "POSSUI_LAVAR_LOUCA","Q018": "POSSUI_ASPIRADOR_PO","Q019": "POSSUI_TV","Q020": "POSSUI_DVD","Q021": "POSSUI_TV_ASSINATURA","Q022": "POSSUI_CELULAR","Q023": "POSSUI_TELEFONE_FIXO","Q024": "POSSUI_COMPUTADOR","Q025": "POSSUI_INTERNET","Q026": "CONCLUIU_ENSINO_MEDIO","Q027": "TIPO_ESCOLA_FREQUENTADA"}

# Rename
df_enem_fact = rename_columns(df_enem_fact,notas_columns)
df_enem_dimension_participante = rename_columns(df_enem_dimension_participante,participante_columns)
df_enem_dimension_questionario = rename_columns(df_enem_dimension_questionario,questionario_columns)

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
