-- Databricks notebook source
-- MAGIC 
-- MAGIC %scala
-- MAGIC val dbNamePrefix = {
-- MAGIC   val tags = com.databricks.logging.AttributionContext.current.tags
-- MAGIC   val name = tags.getOrElse(com.databricks.logging.BaseTagDefinitions.TAG_USER, java.util.UUID.randomUUID.toString.replace("-", ""))
-- MAGIC   val username = if (name != "unknown") name else dbutils.widgets.get("databricksUsername")
-- MAGIC   
-- MAGIC   val module_name = spark.conf.get("com.databricks.training.module_name", "default").toLowerCase()
-- MAGIC 
-- MAGIC   val dbNamePrefix = (username+"_"+module_name).replaceAll("[^a-zA-Z0-9]", "_") + "_db"
-- MAGIC   spark.conf.set("com.databricks.training.spark.dbNamePrefix", dbNamePrefix)
-- MAGIC   dbNamePrefix
-- MAGIC }
-- MAGIC 
-- MAGIC displayHTML(s"Created user-specific database")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC dbNamePrefix = spark.conf.get("com.databricks.training.spark.dbNamePrefix")
-- MAGIC databaseName = dbNamePrefix + "_ilq"
-- MAGIC spark.conf.set("com.databricks.training.spark.databaseName", databaseName)
-- MAGIC 
-- MAGIC spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(databaseName))
-- MAGIC spark.sql("USE {}".format(databaseName))
-- MAGIC 
-- MAGIC displayHTML("""Using the database <b style="color:green">{}</b>.""".format(databaseName))