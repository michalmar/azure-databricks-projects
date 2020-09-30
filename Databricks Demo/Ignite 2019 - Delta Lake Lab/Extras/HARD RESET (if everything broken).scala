// Databricks notebook source
// MAGIC %md
// MAGIC #THIS NOTEBOOK COULD TAKE LONG TIME TO RUN
// MAGIC Run if everything else is broken and you just need to start a clean slate.

// COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", false)

// COMMAND ----------

dbutils.fs.rm("/delta/", true)

// COMMAND ----------

// MAGIC %sql
// MAGIC DELETE FROM deviceRaw

// COMMAND ----------

// MAGIC %sql
// MAGIC DELETE FROM batteryStatus

// COMMAND ----------

// MAGIC %sql
// MAGIC DELETE FROM cabs

// COMMAND ----------

// MAGIC %sql
// MAGIC VACUUM deviceRaw RETAIN 0 HOURS

// COMMAND ----------

// MAGIC %sql
// MAGIC VACUUM batteryStatus RETAIN 0 HOURS

// COMMAND ----------

// MAGIC %sql
// MAGIC VACUUM cabs RETAIN 0 HOURS

// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS deviceRaw")

// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS batteryStatus")

// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS cabs")