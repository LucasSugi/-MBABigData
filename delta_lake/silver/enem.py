# Databricks notebook source
# Pyspark
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, DoubleType
from pyspark.sql.types import StructField, StructType

@udf(returnType=IntegerType())
def get_acertos(resposta,gabarito):

  if((resposta is not None) & (gabarito is not None)):
    
    # Convert to array
    resposta = list(map(lambda x: x,resposta))
    gabarito = list(map(lambda x: x,gabarito))
    
    nota = 0
    for r,g in zip(resposta,gabarito):
      if(r == g):
        nota = nota + 1
    return nota
    
  return None

# COMMAND ----------

# MAGIC %run ../lib/delta_lake

# COMMAND ----------

# Set parameters
dbutils.widgets.text("year","")
dbutils.widgets.text("bucket_name","")

# Get parameters
year = dbutils.widgets.get("year")
bucket_name = dbutils.widgets.get("bucket_name")

# Filepath
filepath_enem = get_filepath(bucket_name,"generic+microdados_gov","bronze","enem")

# Get file to process
files_enem = get_files(".*{}.*".format(year),filepath_enem)
files_enem = "/".join(files_enem[0].split('/')[-2:])
print("Processing file: {}".format(files_enem))

# COMMAND ----------

# Schema ENEM (Novo)
enem_schema_novo = StructType([StructField("NU_INSCRICAO",StringType(),True),StructField("NU_ANO",StringType(),True),StructField("TP_FAIXA_ETARIA",StringType(),True),StructField("TP_SEXO",StringType(),True),StructField("TP_ESTADO_CIVIL",StringType(),True),StructField("TP_COR_RACA",StringType(),True),StructField("TP_NACIONALIDADE",StringType(),True),StructField("TP_ST_CONCLUSAO",StringType(),True),StructField("TP_ANO_CONCLUIU",StringType(),True),StructField("TP_ESCOLA",StringType(),True),StructField("TP_ENSINO",StringType(),True),StructField("IN_TREINEIRO",StringType(),True),StructField("CO_MUNICIPIO_ESC",StringType(),True),StructField("NO_MUNICIPIO_ESC",StringType(),True),StructField("CO_UF_ESC",StringType(),True),StructField("SG_UF_ESC",StringType(),True),StructField("TP_DEPENDENCIA_ADM_ESC",StringType(),True),StructField("TP_LOCALIZACAO_ESC",StringType(),True),StructField("TP_SIT_FUNC_ESC",StringType(),True),StructField("CO_MUNICIPIO_PROVA",StringType(),True),StructField("NO_MUNICIPIO_PROVA",StringType(),True),StructField("CO_UF_PROVA",StringType(),True),StructField("SG_UF_PROVA",StringType(),True),StructField("TP_PRESENCA_CN",StringType(),True),StructField("TP_PRESENCA_CH",StringType(),True),StructField("TP_PRESENCA_LC",StringType(),True),StructField("TP_PRESENCA_MT",StringType(),True),StructField("CO_PROVA_CN",StringType(),True),StructField("CO_PROVA_CH",StringType(),True),StructField("CO_PROVA_LC",StringType(),True),StructField("CO_PROVA_MT",StringType(),True),StructField("NU_NOTA_CN",DoubleType(),True),StructField("NU_NOTA_CH",DoubleType(),True),StructField("NU_NOTA_LC",DoubleType(),True),StructField("NU_NOTA_MT",DoubleType(),True),StructField("TX_RESPOSTAS_CN",StringType(),True),StructField("TX_RESPOSTAS_CH",StringType(),True),StructField("TX_RESPOSTAS_LC",StringType(),True),StructField("TX_RESPOSTAS_MT",StringType(),True),StructField("TP_LINGUA",StringType(),True),StructField("TX_GABARITO_CN",StringType(),True),StructField("TX_GABARITO_CH",StringType(),True),StructField("TX_GABARITO_LC",StringType(),True),StructField("TX_GABARITO_MT",StringType(),True),StructField("TP_STATUS_REDACAO",StringType(),True),StructField("NU_NOTA_COMP1",DoubleType(),True),StructField("NU_NOTA_COMP2",DoubleType(),True),StructField("NU_NOTA_COMP3",DoubleType(),True),StructField("NU_NOTA_COMP4",DoubleType(),True),StructField("NU_NOTA_COMP5",DoubleType(),True),StructField("NU_NOTA_REDACAO",DoubleType(),True),StructField("Q001",StringType(),True),StructField("Q002",StringType(),True),StructField("Q003",StringType(),True),StructField("Q004",StringType(),True),StructField("Q005",IntegerType(),True),StructField("Q006",StringType(),True),StructField("Q007",StringType(),True),StructField("Q008",StringType(),True),StructField("Q009",StringType(),True),StructField("Q010",StringType(),True),StructField("Q011",StringType(),True),StructField("Q012",StringType(),True),StructField("Q013",StringType(),True),StructField("Q014",StringType(),True),StructField("Q015",StringType(),True),StructField("Q016",StringType(),True),StructField("Q017",StringType(),True),StructField("Q018",StringType(),True),StructField("Q019",StringType(),True),StructField("Q020",StringType(),True),StructField("Q021",StringType(),True),StructField("Q022",StringType(),True),StructField("Q023",StringType(),True),StructField("Q024",StringType(),True),StructField("Q025",StringType(),True),StructField("Q026",StringType(),True),StructField("Q027",StringType(),True)])

# Enem schema (Antigo)
enem_schema_antigo = StructType([StructField("NU_INSCRICAO",StringType(),True),StructField("NU_ANO",StringType(),True),StructField("TP_FAIXA_ETARIA",StringType(),True),StructField("TP_SEXO",StringType(),True),StructField("TP_ESTADO_CIVIL",StringType(),True),StructField("TP_COR_RACA",StringType(),True),StructField("TP_NACIONALIDADE",StringType(),True),StructField("TP_ST_CONCLUSAO",StringType(),True),StructField("TP_ANO_CONCLUIU",StringType(),True),StructField("TP_ESCOLA",StringType(),True),StructField("TP_ENSINO",StringType(),True),StructField("IN_TREINEIRO",StringType(),True),StructField("CO_MUNICIPIO_ESC",StringType(),True),StructField("NO_MUNICIPIO_ESC",StringType(),True),StructField("CO_UF_ESC",StringType(),True),StructField("SG_UF_ESC",StringType(),True),StructField("TP_DEPENDENCIA_ADM_ESC",StringType(),True),StructField("TP_LOCALIZACAO_ESC",StringType(),True),StructField("TP_SIT_FUNC_ESC",StringType(),True),StructField("IN_CERTIFICADO",StringType(),True),StructField("NO_ENTIDADE_CERTIFICACAO",StringType(),True),StructField("CO_UF_ENTIDADE_CERTIFICACAO",StringType(),True),StructField("SG_UF_ENTIDADE_CERTIFICACAO",StringType(),True),StructField("CO_MUNICIPIO_PROVA",StringType(),True),StructField("NO_MUNICIPIO_PROVA",StringType(),True),StructField("CO_UF_PROVA",StringType(),True),StructField("SG_UF_PROVA",StringType(),True),StructField("TP_PRESENCA_CN",StringType(),True),StructField("TP_PRESENCA_CH",StringType(),True),StructField("TP_PRESENCA_LC",StringType(),True),StructField("TP_PRESENCA_MT",StringType(),True),StructField("CO_PROVA_CN",StringType(),True),StructField("CO_PROVA_CH",StringType(),True),StructField("CO_PROVA_LC",StringType(),True),StructField("CO_PROVA_MT",StringType(),True),StructField("NU_NOTA_CH",DoubleType(),True),StructField("NU_NOTA_CN",DoubleType(),True),StructField("NU_NOTA_LC",DoubleType(),True),StructField("NU_NOTA_MT",DoubleType(),True),StructField("TX_RESPOSTAS_CN",StringType(),True),StructField("TX_RESPOSTAS_CH",StringType(),True),StructField("TX_RESPOSTAS_LC",StringType(),True),StructField("TX_RESPOSTAS_MT",StringType(),True),StructField("TP_LINGUA",StringType(),True),StructField("TX_GABARITO_CN",StringType(),True),StructField("TX_GABARITO_CH",StringType(),True),StructField("TX_GABARITO_LC",StringType(),True),StructField("TX_GABARITO_MT",StringType(),True),StructField("TP_STATUS_REDACAO",StringType(),True),StructField("NU_NOTA_COMP1",DoubleType(),True),StructField("NU_NOTA_COMP2",DoubleType(),True),StructField("NU_NOTA_COMP3",DoubleType(),True),StructField("NU_NOTA_COMP4",DoubleType(),True),StructField("NU_NOTA_COMP5",DoubleType(),True),StructField("NU_NOTA_REDACAO",DoubleType(),True),StructField("Q001",StringType(),True),StructField("Q002",StringType(),True),StructField("Q003",StringType(),True),StructField("Q004",StringType(),True),StructField("Q005",IntegerType(),True),StructField("Q006",StringType(),True),StructField("Q007",StringType(),True),StructField("Q008",StringType(),True),StructField("Q009",StringType(),True),StructField("Q010",StringType(),True),StructField("Q011",StringType(),True),StructField("Q012",StringType(),True),StructField("Q013",StringType(),True),StructField("Q014",StringType(),True),StructField("Q015",StringType(),True),StructField("Q016",StringType(),True),StructField("Q017",StringType(),True),StructField("Q018",StringType(),True),StructField("Q019",StringType(),True),StructField("Q020",StringType(),True),StructField("Q021",StringType(),True),StructField("Q022",StringType(),True),StructField("Q023",StringType(),True),StructField("Q024",StringType(),True),StructField("Q025",StringType(),True),StructField("Q026",StringType(),True),StructField("Q027",StringType(),True),StructField("Q028",StringType(),True),StructField("Q029",StringType(),True),StructField("Q030",StringType(),True),StructField("Q031",StringType(),True),StructField("Q032",StringType(),True),StructField("Q033",StringType(),True),StructField("Q034",StringType(),True),StructField("Q035",StringType(),True),StructField("Q036",StringType(),True),StructField("Q037",StringType(),True),StructField("Q038",StringType(),True),StructField("Q039",StringType(),True),StructField("Q040",StringType(),True),StructField("Q041",StringType(),True),StructField("Q042",StringType(),True),StructField("Q043",StringType(),True),StructField("Q044",StringType(),True),StructField("Q045",StringType(),True),StructField("Q046",StringType(),True),StructField("Q047",StringType(),True),StructField("Q048",StringType(),True),StructField("Q049",StringType(),True),StructField("Q050",StringType(),True)])

# Set correct schema
if(int(year) >= 2017):
  enem_schema = enem_schema_novo
else:
  enem_schema = enem_schema_antigo

# COMMAND ----------

# Read files
df_enem = (
  read_table(bucket_name,"generic+microdados_gov","bronze",files_enem,options = {"sep":";","encoding":"latin1","header":"True"},schema=enem_schema,table_format="csv")
)

# Repartition
df_enem  = df_enem.repartition(sc.defaultParallelism)

# COMMAND ----------

# Apply string normalization
normalize_columns = ["NO_MUNICIPIO_ESC","NO_MUNICIPIO_PROVA"]
select_normalize_columns = [f.upper(normalize_str(column)).alias(column) for column in normalize_columns]

# Get other columns
select_other_columns = [column for column in df_enem.columns if column not in normalize_columns]

# Apply select
df_enem = df_enem.select(*select_other_columns,*select_normalize_columns)

# COMMAND ----------

# Dicts
faixa_etaria_dict = {"1": "Menor de 17 anos","2": "17 anos","3": "18 anos","4": "19 anos","5": "20 anos","6": "21 anos","7": "22 anos","8": "23 anos","9": "24 anos","10": "25 anos","11": "Entre 26 e 30 anos","12": "Entre 31 e 35 anos","13": "Entre 36 e 40 anos","14": "Entre 41 e 45 anos","15": "Entre 46 e 50 anos","16": "Entre 51 e 55 anos","17": "Entre 56 e 60 anos","18": "Entre 61 e 65 anos","19": "Entre 66 e 70 anos","20": "Maior de 70 anos"}

estado_civil_dict = {"0": "Solteiro(a)","1": "Casado(a)/Mora com companheiro(a)","2": "Divorciado(a)/Desquitado(a)/Separado(a)","3": "Viúvo(a)"}

cor_raca_dict = {"0": "Não declarado","1": "Branca","2": "Preta","3": "Parda","4": "Amarela","5": "Indígena"}

nacionalidade_dict = {"0": "Não informado","1": "Brasileiro(a)","2": "Brasileiro(a) Naturalizado(a)","3": "Estrangeiro(a)","4": "Brasileiro(a) Nato(a), nascido(a) no exterior"}

situacao_ensino_medio_dict = {"1": "Já concluí o Ensino Médio","2": "Estou cursando e concluirei o Ensino Médio em 2018","3": "Estou cursando e concluirei o Ensino Médio após 2018","4": "Não concluí e não estou cursando o Ensino Médio"}

ano_conclusao_ensino_medio_dict = {"0":"Não informado","1":"2017","2":"2016","3":"2015","4":"2014","5":"2013","6":"2012","7":"2011","8":"2010","9":"2009","10":"2008","11":"2007","12":"Antes de 2007"}

tipo_escola_ensino_medio_dict = {"1": "Não Respondeu","2": "Pública","3": "Exterior","4": "Privada"}

tipo_escola_conclusao_ensino_medio_dict = {"1":"Ensino Regular","2":"Educação Especial - Modalidade Substitutiva","3":"Educação de Jovens e Adultos"}

fez_prova_apenas_treinar_conhecimento_dict = {"1":"Sim","0":"Não"}

dependencia_administrativa_escola_dict = {"1":"Federal","2":"Estadual","3":"Municipal","4":"Privada"}

localizacao_escola_dict = {"1":"Urbana","2":"Rural"}

situacao_funcioamento_dict = {"1":"Em atividade","2":"Paralisada","3":"Extinta","4":"Escola extinta em anos anteriores."}

presenca_prova_dict = {"0":"Faltou à prova","1":"Presente na prova","2":"Eliminado na prova"}

lingua_dict = {"0": "Inglês","1": "Espanhol"}

situacao_redacao_dict = {"1":"Sem problemas","2":"Anulada","3":"Cópia Texto Motivador","4":"Em Branco","6":"Fuga ao tema","7":"Não atendimento ao tipo textual","8":"Texto insuficiente","9":"Parte desconectada"}

serie_mae_pai_estudou_dict = {"A":"Nunca estudou.","B":"Não completou a 4ª série/5º ano do Ensino Fundamental.","C":"Completou a 4ª série/5º ano, mas não completou a 8ª série/9º ano do Ensino Fundamental.","D":"Completou a 8ª série/9º ano do Ensino Fundamental, mas não completou o Ensino Médio.","E":"Completou o Ensino Médio, mas não completou a Faculdade.","F":"Completou a Faculdade, mas não completou a Pós-graduação.","G":"Completou a Pós-graduação.","H":"Não sei."}

ocupacao_mae_pai_dict = {"A":"Grupo 1: Lavrador, agricultor sem empregados, bóia fria, criador de animais (gado, porcos, galinhas, ovelhas, cavalos etc.), apicultor, pescador, lenhador, seringueiro, extrativista.","B":"Grupo 2: Diarista, empregado doméstico, cuidador de idosos, babá, cozinheiro (em casas particulares), motorista particular, jardineiro, faxineiro de empresas e prédios, vigilante, porteiro, carteiro, office-boy, vendedor, caixa, atendente de loja, auxiliar administrativo, recepcionista, servente de pedreiro, repositor de mercadoria.","C":"Grupo 3: Padeiro, cozinheiro industrial ou em restaurantes, sapateiro, costureiro, joalheiro, torneiro mecânico, operador de máquinas, soldador, operário de fábrica, trabalhador da mineração, pedreiro, pintor, eletricista, encanador, motorista, caminhoneiro, taxista.","D":"Grupo 4: Professor (de ensino fundamental ou médio, idioma, música, artes etc.), técnico (de enfermagem, contabilidade, eletrônica etc.), policial, militar de baixa patente (soldado, cabo, sargento), corretor de imóveis, supervisor, gerente, mestre de obras, pastor, microempresário (proprietário de empresa com menos de 10 empregados), pequeno comerciante, pequeno proprietário de terras, trabalhador autônomo ou por conta própria.","E":"Grupo 5: Médico, engenheiro, dentista, psicólogo, economista, advogado, juiz, promotor, defensor, delegado, tenente, capitão, coronel, professor universitário, diretor em empresas públicas ou privadas, político, proprietário de empresas com mais de 10 empregados.","F":"Não sei."}

renda_familia_dict = {"A":"Nenhuma renda.","B":"Até R$ 954,00.","C":"De R$ 954,01 até R$ 1.431,00.","D":"De R$ 1.431,01 até R$ 1.908,00.","E":"De R$ 1.908,01 até R$ 2.385,00.","F":"De R$ 2.385,01 até R$ 2.862,00.","G":"De R$ 2.862,01 até R$ 3.816,00.","H":"De R$ 3.816,01 até R$ 4.770,00.","I":"De R$ 4.770,01 até R$ 5.724,00.","J":"De R$ 5.724,01 até R$ 6.678,00.","K":"De R$ 6.678,01 até R$ 7.632,00.","L":"De R$ 7.632,01 até R$ 8.586,00.","M":"De R$ 8.586,01 até R$ 9.540,00.","N":"De R$ 9.540,01 até R$ 11.448,00.","O":"De R$ 11.448,01 até R$ 14.310,00.","P":"De R$ 14.310,01 até R$ 19.080,00.","Q":"Mais de R$ 19.080,00."}


de_para1_dict = {"A":"Não.","B":"Sim, um.","C":"Sim, dois.","D":"Sim, três.","E":"Sim, quatro ou mais."}
de_para2_dict = {"A":"Não.","B":"Sim."}
esta_concluindo_ensino_medio_dict = {"A":"Já concluí o Ensino Médio.","B":"Estou cursando e concluirei o Ensino Médio em 2018.","C":"Estou cursando e concluirei o Ensino Médio após 2018.","D":"Não concluí e não estou cursando o Ensino Médio."}
tipo_escola_frequentou_dict = {"A":"Somente em escola pública.","B":"Parte em escola pública e parte em escola privada SEM bolsa de estudo integral.","C":"Parte em escola pública e parte em escola privada COM bolsa de estudo integral.","D":"Somente em escola privada SEM bolsa de estudo integral.","E":"Somente em escola privada COM bolsa de estudo integral.","F":"Não frequentei a escola"}

# COMMAND ----------

# Selects
select_faixa_etaria = build_case_when(faixa_etaria_dict,"TP_FAIXA_ETARIA","==","TP_FAIXA_ETARIA")
select_estado_civel = build_case_when(estado_civil_dict,"TP_ESTADO_CIVIL","==","TP_ESTADO_CIVIL")
select_cor_raca = build_case_when(cor_raca_dict,"TP_COR_RACA","==","TP_COR_RACA")
select_nacionalidade = build_case_when(nacionalidade_dict,"TP_NACIONALIDADE","==","TP_NACIONALIDADE")
select_situacao_ensino_medio = build_case_when(situacao_ensino_medio_dict,"TP_ST_CONCLUSAO","==","TP_ST_CONCLUSAO")
select_ano_conclusao_ensino_medio = build_case_when(ano_conclusao_ensino_medio_dict,"TP_ANO_CONCLUIU","==","TP_ANO_CONCLUIU")
select_tipo_escola_ensino_medio = build_case_when(tipo_escola_ensino_medio_dict,"TP_ESCOLA","==","TP_ESCOLA")
select_tipo_escola_conclusao_ensino_medio = build_case_when(tipo_escola_conclusao_ensino_medio_dict,"TP_ENSINO","==","TP_ENSINO")
select_fez_prova_apenas_treinar_conhecimento = build_case_when(fez_prova_apenas_treinar_conhecimento_dict,"IN_TREINEIRO","==","IN_TREINEIRO")
select_dependencia_administrativa_escola = build_case_when(dependencia_administrativa_escola_dict,"TP_DEPENDENCIA_ADM_ESC","==","TP_DEPENDENCIA_ADM_ESC")
select_localizacao_escola = build_case_when(localizacao_escola_dict,"TP_LOCALIZACAO_ESC","==","TP_LOCALIZACAO_ESC")
select_situacao_funcioamento = build_case_when(situacao_funcioamento_dict,"TP_SIT_FUNC_ESC","==","TP_SIT_FUNC_ESC")
select_presenca_prova_cn = build_case_when(presenca_prova_dict,"TP_PRESENCA_CN","==","TP_PRESENCA_CN")
select_presenca_prova_ch = build_case_when(presenca_prova_dict,"TP_PRESENCA_CH","==","TP_PRESENCA_CH")
select_presenca_prova_lc = build_case_when(presenca_prova_dict,"TP_PRESENCA_LC","==","TP_PRESENCA_LC")
select_presenca_prova_mt = build_case_when(presenca_prova_dict,"TP_PRESENCA_MT","==","TP_PRESENCA_MT")
select_lingua = build_case_when(situacao_redacao_dict,"TP_LINGUA","==","TP_LINGUA")
select_situacao_redacao = build_case_when(situacao_redacao_dict,"TP_STATUS_REDACAO","==","TP_STATUS_REDACAO")
select_q1 = build_case_when(serie_mae_pai_estudou_dict,"Q001","==","Q001")
select_q2 = build_case_when(serie_mae_pai_estudou_dict,"Q002","==","Q002")
select_q3 = build_case_when(ocupacao_mae_pai_dict,"Q003","==","Q003")
select_q4 = build_case_when(ocupacao_mae_pai_dict,"Q004","==","Q004")
select_q6 = build_case_when(renda_familia_dict,"Q006 ","==","Q006")
select_q7 = build_case_when(de_para1_dict,"Q007","==","Q007")
select_q8 = build_case_when(de_para1_dict,"Q008","==","Q008")
select_q9 = build_case_when(de_para1_dict,"Q009","==","Q009")
select_q10 = build_case_when(de_para1_dict,"Q010","==","Q010")
select_q11 = build_case_when(de_para1_dict,"Q011","==","Q011")
select_q12 = build_case_when(de_para1_dict,"Q012","==","Q012")
select_q13 = build_case_when(de_para1_dict,"Q013","==","Q013")
select_q14 = build_case_when(de_para1_dict,"Q014","==","Q014")
select_q15 = build_case_when(de_para1_dict,"Q015","==","Q015")
select_q16 = build_case_when(de_para1_dict,"Q016","==","Q016")
select_q17 = build_case_when(de_para1_dict,"Q017","==","Q017")
select_q18 = build_case_when(de_para1_dict,"Q018","==","Q018")
select_q19 = build_case_when(de_para1_dict,"Q019","==","Q019")
select_q20 = build_case_when(de_para1_dict,"Q020","==","Q020")
select_q21 = build_case_when(de_para2_dict,"Q021","==","Q021")
select_q22 = build_case_when(de_para1_dict,"Q022","==","Q022")
select_q23 = build_case_when(de_para2_dict,"Q023","==","Q023")
select_q24 = build_case_when(de_para1_dict,"Q024","==","Q024")
select_q25 = build_case_when(de_para2_dict,"Q025","==","Q025")
select_q26 = build_case_when(esta_concluindo_ensino_medio_dict,"Q026","==","Q026")
select_q27 = build_case_when(tipo_escola_frequentou_dict,"Q027","==","Q027")

# Select all
select_depara = [select_faixa_etaria,select_estado_civel,select_cor_raca,select_nacionalidade,select_situacao_ensino_medio,select_ano_conclusao_ensino_medio,select_tipo_escola_ensino_medio,select_tipo_escola_conclusao_ensino_medio,select_fez_prova_apenas_treinar_conhecimento,select_dependencia_administrativa_escola,select_localizacao_escola,select_situacao_funcioamento,select_presenca_prova_cn,select_presenca_prova_ch,select_presenca_prova_lc,select_presenca_prova_mt,select_lingua,select_situacao_redacao,select_q1,select_q2,select_q3,select_q4,select_q6,select_q7,select_q8,select_q9,select_q10,select_q11,select_q12,select_q13,select_q14,select_q15,select_q16,select_q17,select_q18,select_q19,select_q20,select_q21,select_q22,select_q23,select_q24,select_q25,select_q26,select_q27]

# COMMAND ----------

# Get columns that should not apply "case when"
other_columns = [column for column in df_enem.columns if column not in df_enem.select(select_depara).columns]

# Apply case when and select other columns
df_enem = df_enem.select(*other_columns,*select_depara)

# COMMAND ----------

# Create "acertos" column
respostas = ["TX_RESPOSTAS_CN","TX_RESPOSTAS_CH","TX_RESPOSTAS_LC","TX_RESPOSTAS_MT"]
gabaritos = ["TX_GABARITO_CN","TX_GABARITO_CH","TX_GABARITO_LC","TX_GABARITO_MT"]
select_get_acertos = [get_acertos(r,g).alias(r.replace("TX_RESPOSTAS","ACERTOS")) for r, g in zip(respostas,gabaritos)]

# Apply
df_enem = df_enem.select("*",*select_get_acertos).drop(*respostas,*gabaritos)

# COMMAND ----------

# Base columns
select_base_columns = ["NU_INSCRICAO","NU_ANO"]

# Column to transform in struct
struct_dict = {
    "PARTICIPANTE": ["TP_FAIXA_ETARIA","TP_SEXO","TP_ESTADO_CIVIL","TP_COR_RACA","TP_NACIONALIDADE","TP_ST_CONCLUSAO","TP_ANO_CONCLUIU","TP_ESCOLA","TP_ENSINO","IN_TREINEIRO"]
    , "ESCOLA": ["NO_MUNICIPIO_ESC","SG_UF_ESC","TP_DEPENDENCIA_ADM_ESC","TP_LOCALIZACAO_ESC","TP_SIT_FUNC_ESC","TP_LINGUA"]
    , "LOCAL_APLICACAO_PROVA": ["NO_MUNICIPIO_PROVA","SG_UF_PROVA"]
    , "PRESENCA_PROVA": ["TP_PRESENCA_CN","TP_PRESENCA_CH","TP_PRESENCA_LC","TP_PRESENCA_MT"]
    , "NOTA_PROVA": ["NU_NOTA_CN","NU_NOTA_CH","NU_NOTA_LC","NU_NOTA_MT"]
    , "ACERTOS": ["ACERTOS_CN","ACERTOS_CH","ACERTOS_LC","ACERTOS_MT"]
    , "REDACAO": ["TP_STATUS_REDACAO","NU_NOTA_COMP1","NU_NOTA_COMP2","NU_NOTA_COMP3","NU_NOTA_COMP4","NU_NOTA_COMP5","NU_NOTA_REDACAO"]
    , "QUESTIONARIO_SOCIO_ECONOMICO": ["Q001","Q002","Q003","Q004","Q005","Q006","Q007","Q008","Q009","Q010","Q011","Q012","Q013","Q014","Q015","Q016","Q017","Q018","Q019","Q020","Q021","Q022","Q023","Q024","Q025","Q026","Q027"]
}
select_struct = [f.struct(*struct_dict[k]).alias(k) for k in struct_dict]

# Apply
df_enem = df_enem.select(*select_base_columns,*select_struct)

# Rename column
df_enem = df_enem.withColumnRenamed("NU_ANO","ANO_PROVA")

# COMMAND ----------

# Options to write
options = {"replaceWhere":"ANO_PROVA == {}".format(year)}

# Save
write_table(
  df_enem,
  bucket_name,
  "generic+microdados_gov",
  "silver",
  "enem",
  options=options,
  partitionBy="ANO_PROVA"
)
