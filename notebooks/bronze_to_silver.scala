// Databricks notebook source
// DBTITLE 1,Transformando os dados para a camada silver
// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/bronze")
// MAGIC

// COMMAND ----------

// DBTITLE 1,Lendo os dados na camada bronze
val path = "dbfs:/mnt/dados/bronze/dataset_imoveis/"
val df = spark.read.format("delta").load(path)


// COMMAND ----------

display(df)


// COMMAND ----------

// DBTITLE 1,Transformando os campos do json em coluna
display(df.select("anuncio.*"))


// COMMAND ----------

display(
  df.select("anuncio.*", "anuncio.endereco.*")
)


// COMMAND ----------

val dados_detalhados = df.select("anuncio.*", "anuncio.endereco.*")


// COMMAND ----------

display(dados_detalhados)

// COMMAND ----------

// DBTITLE 1,Removendo colunas
val df_silver = dados_detalhados.drop("caracteristicas", "endereco")
display(df_silver)


// COMMAND ----------

// DBTITLE 1,Salvando na camada silver
val path = "dbfs:/mnt/dados/silver/dataset_imoveis"
df_silver.write.format("delta").mode("overwrite").save(path)


// COMMAND ----------


