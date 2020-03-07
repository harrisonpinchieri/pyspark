# Databricks notebook source
from pyspark.sql.functions import col,regexp_replace
from pyspark.sql.types import DecimalType



# COMMAND ----------

#Exercício 1
dbutils.fs.mkdirs('/FileStore/output')

data_path = '/FileStore/tables/'
trecho_path = data_path + '2020_Trecho.csv'
viagens_path =  data_path + '2020_Viagem.csv'
output_path = '/FileStore/output/'

viagens_df = spark.read.format("csv").option('header','true').option('encoding','ISO-8859-1').option("sep",";").load(viagens_path)

# COMMAND ----------

new_viagens_df = viagens_df.select(col('Identificador do processo de viagem').alias('identificador_do_processo_de_viagem'),
                                   col('Situação').alias('situacao'),
                                   col('Código do órgão superior').alias('codigo_do_orgao_superior'),
                                   col('Nome do órgão superior').alias('nome_do_orgao_superior'),
                                   col('Código órgão solicitante').alias('codigo_orgao_solicitante'),
                                   col('Nome órgão solicitante').alias('nome_orgao_solicitante'),
                                   col('CPF viajante').alias('cpf_viajante'),
                                   col('Nome').alias('nome'),
                                   col('Cargo').alias('cargo'),
                                   col('Período - Data de início').alias('periodo_data_de_inicio'),
                                   col('Período - Data de fim').alias('periodo_data_de_fim'),
                                   col('Destinos').alias('destinos'),
                                   col('Motivo').alias('motivo'),
                                   regexp_replace(col('Valor diárias'),",","").cast("decimal").alias("valor_diarias"),
                                   regexp_replace(col('Valor passagens'),",","").cast("decimal").alias("valor_passagens"),
                                   regexp_replace(col('Valor outros gastos'),",","").cast("decimal").alias("valor_outros_gastos")).cache()
                                 
                                   
                                   
                                   

# COMMAND ----------

#exercício 1

new_viagens_df.write.mode('overwrite').parquet(output_path + "viagens_parquet")

# COMMAND ----------

#exercício 2
viagens_df.coalesce(1).write.mode('overwrite')\
                .format('xml') \
                .options(rowTag='viagem', rootTag='viagens') \
                .save(output_path + 'viagens_xml')

# COMMAND ----------

#exercício 3
new_viagens_df.orderBy('nome_do_orgao_superior','nome_orgao_solicitante',ascending=False).coalesce(1).write.mode('overwrite').json(output_path + 'viagens_json')

# COMMAND ----------

#exercício 4
df_final_viagens = new_viagens_df.withColumn('valor_total', sum(new_viagens_df.select(["valor_diarias","valor_passagens","valor_outros_gastos"])))
df_final_viagens.show(truncate=False)

# COMMAND ----------

#exercício 5
df_final_viagens.filter(col("situacao") == "Realizada")\
  .groupBy("nome_orgao_solicitante").agg({'valor_total':'sum'})\
  .withColumnRenamed("sum(valor_total)","valor_total")\
  .orderBy(col("valor_total"),ascending=False)\
  .show(truncate=False)
  
                        
                                     
  

# COMMAND ----------

#exercício 6

trecho_df = spark.read.format("csv").option('header','true').option('encoding','ISO-8859-1').option("sep",";").load(trecho_path)

new_trecho_df = trecho_df.select(col('Identificador do processo de viagem ').alias('identificador_do_processo_de_viagem'),
                                   col('Sequência Trecho').alias('sequencia_trecho'),
                                   col('Origem - Data').alias('origem_data'),
                                   col('Origem - País').alias('origem_pais'),
                                   col('Origem - UF').alias('origem_uf'),
                                   col('Origem - Cidade').alias('origem_cidade'),
                                   col('Destino - Data').alias('destino_data'),
                                   col('Destino - País').alias('destino_pais'),
                                   col('Destino - UF').alias('destino_uf'),
                                   col('Destino - Cidade').alias('destino_cidade'),
                                   col('Meio de transporte').alias('meio_de_transporte'),
                                   col('Número Diárias').alias('numero_diarias'),
                                   col('Missao?').alias('missao'))


# COMMAND ----------

#exercício 6

df_join_trecho_viagens = new_trecho_df.join(df_final_viagens,['identificador_do_processo_de_viagem'],how ='inner')\
.select(
  col('identificador_do_processo_de_viagem')
  ,col('situacao')
  ,col('nome_do_orgao_superior')
  ,col('sequencia_trecho')
  ,col('destino_cidade')).cache()

df_join_trecho_viagens.show()

# COMMAND ----------

df_join_trecho_viagens.coalesce(1).write.mode('overwrite').json(output_path + 'join_trecho_viagens_json')

# COMMAND ----------

