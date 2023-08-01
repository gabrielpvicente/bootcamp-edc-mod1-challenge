import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
from pyspark.sql import functions as f
from pyspark.sql.functions import lit

  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

rais_vinc_pub_centro_oeste = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("delimiter", ";")\
    .option("encoding", "ISO-8859-1")\
    .load("s3://datalake-gabrielpvicente-352855538020-tf/raw-data/RAIS_VINC_PUB_CENTRO_OESTE/RAIS_VINC_PUB_CENTRO_OESTE.txt")
print("rais_vinc_pub_centro_oeste")
rais_vinc_pub_centro_oeste.printSchema()
rais_vinc_pub_centro_oeste.show(10)

rais_vinc_pub_mg_es_rj = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("delimiter", ";")\
    .option("encoding", "ISO-8859-1")\
    .load("s3://datalake-gabrielpvicente-352855538020-tf/raw-data/RAIS_VINC_PUB_MG_ES_RJ/RAIS_VINC_PUB_MG_ES_RJ.txt")
print("rais_vinc_pub_mg_es_rj")
rais_vinc_pub_mg_es_rj.printSchema()
rais_vinc_pub_mg_es_rj.show(10)

rais_vinc_pub_nordeste = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("delimiter", ";")\
    .option("encoding", "ISO-8859-1")\
    .load("s3://datalake-gabrielpvicente-352855538020-tf/raw-data/RAIS_VINC_PUB_NORDESTE/RAIS_VINC_PUB_NORDESTE.txt")
print("rais_vinc_pub_nordeste")
rais_vinc_pub_nordeste.printSchema()
rais_vinc_pub_nordeste.show(10)

rais_vinc_pub_norte = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("delimiter", ";")\
    .option("encoding", "ISO-8859-1")\
    .load("s3://datalake-gabrielpvicente-352855538020-tf/raw-data/RAIS_VINC_PUB_NORTE/RAIS_VINC_PUB_NORTE.txt")
print("rais_vinc_pub_norte")
rais_vinc_pub_norte.printSchema()
rais_vinc_pub_norte.show(10)

rais_vinc_pub_sp = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("delimiter", ";")\
    .option("encoding", "ISO-8859-1")\
    .load("s3://datalake-gabrielpvicente-352855538020-tf/raw-data/RAIS_VINC_PUB_SP/RAIS_VINC_PUB_SP.txt")
print("rais_vinc_pub_sp")
rais_vinc_pub_sp.printSchema()
rais_vinc_pub_sp.show(10)

rais_vinc_pub_sul = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("delimiter", ";")\
    .option("encoding", "ISO-8859-1")\
    .load("s3://datalake-gabrielpvicente-352855538020-tf/raw-data/RAIS_VINC_PUB_SUL/RAIS_VINC_PUB_SUL.txt")
print("rais_vinc_pub_sul")
rais_vinc_pub_sul.printSchema()
rais_vinc_pub_sul.show(10)

rais = rais_vinc_pub_centro_oeste.union(rais_vinc_pub_mg_es_rj).union(rais_vinc_pub_nordeste).union(rais_vinc_pub_norte).union(rais_vinc_pub_sp).union(rais_vinc_pub_sul)
print("rais after union")
rais.printSchema()
rais.show(10)

rais = (
    rais
    .withColumnRenamed('Bairros SP', 'bairros_sp')
    .withColumnRenamed('Bairros Fortaleza', 'bairros_fortaleza')
    .withColumnRenamed('Bairros RJ', 'bairros_rj')
    .withColumnRenamed('Causa Afastamento 1', 'causa_afastamento_1')
    .withColumnRenamed('Causa Afastamento 2', 'causa_afastamento_2')
    .withColumnRenamed('Causa Afastamento 3', 'causa_afastamento_3')
    .withColumnRenamed('Motivo Desligamento', 'motivo_desligamento')
    .withColumnRenamed('CBO Ocupação 2002', 'cbo_ocupacao_2002')
    .withColumnRenamed('CNAE 2.0 Classe', 'cnae_2_0_classe')
    .withColumnRenamed('CNAE 95 Classe', 'cnae_95_classe')
    .withColumnRenamed('Distritos SP', 'distritos_sp')
    .withColumnRenamed('Vínculo Ativo 31/12', 'vinculo_ativo_31_12')
    .withColumnRenamed('Faixa Etária', 'faixa_etaria')
    .withColumnRenamed('Faixa Hora Contrat', 'faixa_hora_contrat')
    .withColumnRenamed('Faixa Remun Dezem (SM)', 'faixa_remun_dezem_sm')
    .withColumnRenamed('Faixa Remun Média (SM)', 'faixa_remun_media_sm')
    .withColumnRenamed('Faixa Tempo Emprego', 'faixa_tempo_emprego')
    .withColumnRenamed('Escolaridade após 2005', 'escolaridade_apos_2005')
    .withColumnRenamed('Qtd Hora Contr', 'qtd_hora_contr')
    .withColumnRenamed('Idade', 'idade')
    .withColumnRenamed('Ind CEI Vinculado', 'ind_cei_vinculado')
    .withColumnRenamed('Ind Simples', 'ind_simples')
    .withColumnRenamed('Mês Admissão', 'mes_admissao')
    .withColumnRenamed('Mês Desligamento', 'mes_desligamento')
    .withColumnRenamed('Mun Trab', 'mun_trab')
    .withColumnRenamed('Município', 'municipio')
    .withColumnRenamed('Nacionalidade', 'nacionalidade')
    .withColumnRenamed('Natureza Jurídica', 'natureza_juridica')
    .withColumnRenamed('Ind Portador Defic', 'ind_portador_defic')
    .withColumnRenamed('Qtd Dias Afastamento', 'qtd_dias_afastamento')
    .withColumnRenamed('Raça Cor', 'raca_cor')
    .withColumnRenamed('Regiões Adm DF', 'regioes_adm_df')
    .withColumnRenamed('Vl Remun Dezembro Nom', 'vl_remun_dezembro_nom')
    .withColumnRenamed('Vl Remun Dezembro (SM)', 'vl_remun_dezembro_sm')
    .withColumnRenamed('Vl Remun Média Nom', 'vl_remun_media_nom')
    .withColumnRenamed('Vl Remun Média (SM)', 'vl_remun_media_sm')
    .withColumnRenamed('CNAE 2.0 Subclasse', 'cnae_2_0_subclasse')
    .withColumnRenamed('Sexo Trabalhador', 'sexo_trabalhador')
    .withColumnRenamed('Tamanho Estabelecimento', 'tamanho_estabelecimento')
    .withColumnRenamed('Tempo Emprego', 'tempo_emprego')
    .withColumnRenamed('Tipo Admissão', 'tipo_admissao')
    .withColumnRenamed('Tipo Estab41', 'tipo_estab41')
    .withColumnRenamed('Tipo Estab42', 'tipo_estab42')
    .withColumnRenamed('Tipo Defic', 'tipo_defic')
    .withColumnRenamed('Tipo Vínculo', 'tipo_vinculo')
    .withColumnRenamed('IBGE Subsetor', 'ibge_subsetor')
    .withColumnRenamed('Vl Rem Janeiro SC', 'vl_rem_janeiro_sc')
    .withColumnRenamed('Vl Rem Fevereiro SC', 'vl_rem_fevereiro_sc')
    .withColumnRenamed('Vl Rem Março SC', 'vl_rem_marco_sc')
    .withColumnRenamed('Vl Rem Abril SC', 'vl_rem_abril_sc')
    .withColumnRenamed('Vl Rem Maio SC', 'vl_rem_maio_sc')
    .withColumnRenamed('Vl Rem Junho SC', 'vl_rem_junho_sc')
    .withColumnRenamed('Vl Rem Julho SC', 'vl_rem_julho_sc')
    .withColumnRenamed('Vl Rem Agosto SC', 'vl_rem_agosto_sc')
    .withColumnRenamed('Vl Rem Setembro SC', 'vl_rem_setembro_sc')
    .withColumnRenamed('Vl Rem Outubro SC', 'vl_rem_outubro_sc')
    .withColumnRenamed('Vl Rem Novembro SC', 'vl_rem_novembro_sc')
    .withColumnRenamed('Ano Chegada Brasil', 'ano_chegada_brasil')
    .withColumnRenamed('Ind Trab Intermitente', 'ind_trab_intermitente')
    .withColumnRenamed('Ind Trab Parcial', 'ind_trab_parcial')
)


rais = rais.withColumn("ano", lit(2020)).withColumn("uf", f.col("municipio").cast('string').substr(1,2).cast('int'))

rais = (
    rais
    .withColumn("mes_desligamento", f.col('mes_desligamento').cast('int'))
    .withColumn("vl_remun_dezembro_nom", f.regexp_replace("vl_remun_dezembro_nom", ',', '.').cast('double'))
    .withColumn("vl_remun_dezembro_sm", f.regexp_replace("vl_remun_dezembro_sm", ',', '.').cast('double'))
    .withColumn("vl_remun_media_nom", f.regexp_replace("vl_remun_media_nom", ',', '.').cast('double'))
    .withColumn("vl_remun_media_sm", f.regexp_replace("vl_remun_media_sm", ',', '.').cast('double'))
    .withColumn("vl_rem_janeiro_sc", f.regexp_replace("vl_rem_janeiro_sc", ',', '.').cast('double'))
    .withColumn("vl_rem_fevereiro_sc", f.regexp_replace("vl_rem_fevereiro_sc", ',', '.').cast('double'))
    .withColumn("vl_rem_marco_sc", f.regexp_replace("vl_rem_marco_sc", ',', '.').cast('double'))
    .withColumn("vl_rem_abril_sc", f.regexp_replace("vl_rem_abril_sc", ',', '.').cast('double'))
    .withColumn("vl_rem_maio_sc", f.regexp_replace("vl_rem_maio_sc", ',', '.').cast('double'))
    .withColumn("vl_rem_junho_sc", f.regexp_replace("vl_rem_junho_sc", ',', '.').cast('double'))
    .withColumn("vl_rem_julho_sc", f.regexp_replace("vl_rem_julho_sc", ',', '.').cast('double'))
    .withColumn("vl_rem_agosto_sc", f.regexp_replace("vl_rem_agosto_sc", ',', '.').cast('double'))
    .withColumn("vl_rem_setembro_sc", f.regexp_replace("vl_rem_setembro_sc", ',', '.').cast('double'))
    .withColumn("vl_rem_outubro_sc", f.regexp_replace("vl_rem_outubro_sc", ',', '.').cast('double'))
    .withColumn("vl_rem_novembro_sc", f.regexp_replace("vl_rem_novembro_sc", ',', '.').cast('double'))
)

print("rais final schema")
rais.printSchema()
rais.show(10)

(
    rais
    .coalesce(50)
    .write.mode('overwrite')
    .partitionBy('ano','uf')
    .format('parquet')
    .save('s3://datalake-gabrielpvicente-352855538020-tf/spec/rais/')
)
job.commit()