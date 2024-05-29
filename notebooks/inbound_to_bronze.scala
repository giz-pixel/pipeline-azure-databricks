// Databricks notebook source
// MAGIC %python 
// MAGIC dbutils.fs.ls("/mnt/dados/inbound")

// COMMAND ----------

val path = "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
val dados = spark.read.json(path)

// COMMAND ----------

dados

// COMMAND ----------

dados.printSchema()

// COMMAND ----------

display(dados)

// COMMAND ----------

val anuncio = dados.drop("imagens","usuario")
display(anuncio)

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

val dfBronze = anuncio.withColumn("id", col("anuncio.id"))
display(dfBronze)

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
dfBronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/inbound")

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/dados/bronze")

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis/"
val df = spark.read.format("delta").load(path)

// COMMAND ----------

display(df)

// COMMAND ----------

display(df.select("anuncio.*"))

// COMMAND ----------

display(
  df.select("anuncio.*", "anuncio.endereco.*")

)

// COMMAND ----------

val dadosDetals = df.select("anuncio.*", "anuncio.endereco.*")

// COMMAND ----------

display(dadosDetals)

// COMMAND ----------

val dataDetals = dadosDetals.drop("caracteristicas")

// COMMAND ----------

dataDetals.printSchema()

// COMMAND ----------

display(dataDetals)

// COMMAND ----------

val dfSilver = dataDetals.drop("endereco")
display(dfSilver)

// COMMAND ----------

val path = "dbfs:/mnt/dados/silver/dataset_imoveis"
dfSilver.write.format("delta").mode("overwrite").save(path)

// COMMAND ----------

dfSilver.columns

// COMMAND ----------

dfSilver.foreach(println)

// COMMAND ----------

val dfteste = dfSilver.columns

// COMMAND ----------

dfteste.foreach(println)

// COMMAND ----------


