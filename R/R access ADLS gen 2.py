# Databricks notebook source
# MAGIC %md # R Demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## ADLS Gen 2 mounted to `/mnt/demofs`
# MAGIC 
# MAGIC ```python
# MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
# MAGIC            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC            "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope = "azure-keyvault-mma", key = "tlnr-appid"),
# MAGIC            "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope = "azure-keyvault-mma", key = "tlnr-app-secret"),
# MAGIC            "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token"}
# MAGIC ```
# MAGIC Optionally, you can add <directory-name> to the source URI of your mount point.
# MAGIC ```python
# MAGIC dbutils.fs.mount(
# MAGIC   source = "abfss://demofs@tlnrtestgen2.dfs.core.windows.net/",
# MAGIC   mount_point = "/mnt/demofs",
# MAGIC   extra_configs = configs)
# MAGIC ```

# COMMAND ----------

# MAGIC %md ### Reading from ADLS gen2 using Service Principal - mounted with SP (run on workspace level)

# COMMAND ----------

data_sp = (spark.read
    .format("com.databricks.spark.csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("/mnt/demofs/ages.csv"))

# COMMAND ----------

display(data_sp)

# COMMAND ----------

# MAGIC %md ### Reading from ADLS gen2 using Pass-through or Key (needs to be specified on the cluster level)

# COMMAND ----------

data_pt = (spark.read
    .format('com.databricks.spark.csv')
    .option("inferSchema", "true")
    .option("header", "true")
    .load("abfss://demofs@tlnrtestgen2.dfs.core.windows.net/ages.csv"))

# COMMAND ----------

display(data_pt)

# COMMAND ----------

# MAGIC %md ## Working with data in R
# MAGIC 
# MAGIC There are two major options:
# MAGIC 1. Read in plain R
# MAGIC 1. Read through SparkR
# MAGIC  - read from path
# MAGIC  - read from tempview/table
# MAGIC 
# MAGIC > note: best practice is to differentiate between those dataframes by naming convention (and use `str()` function to see which DF is what)

# COMMAND ----------

# MAGIC %md ### 1. Read from ADLS gen2 in plain R into R's `data.frame`

# COMMAND ----------

# MAGIC %r
# MAGIC rdf <- read.csv("/dbfs/mnt/demofs/ages.csv")
# MAGIC str(rdf)
# MAGIC rdf

# COMMAND ----------

# MAGIC %md ### 2. Read from ADLS gen2 in  SparkR into Spark Data Frame

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR)

# COMMAND ----------

# MAGIC %r 
# MAGIC sdf <- read.df("/mnt/demofs/ages.csv", "csv")
# MAGIC str(sdf)
# MAGIC 
# MAGIC # use Databricks' display() funtion
# MAGIC display(sdf)

# COMMAND ----------

# MAGIC %md You can also create Spark dataframe as Temp View/Table and read it from R (combine Python and R)

# COMMAND ----------

data_sp = (spark.read
    .format("com.databricks.spark.csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("/mnt/demofs/ages.csv"))
data_sp.createOrReplaceTempView("ages_v")

# COMMAND ----------

# MAGIC %r
# MAGIC 
# MAGIC # sql("REFRESH TABLE ages_v")
# MAGIC 
# MAGIC df <- sql("SELECT * FROM ages_v")
# MAGIC 
# MAGIC # dataset<-as.data.frame(df)
# MAGIC 
# MAGIC display(df)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

data_pt = (spark.read
    .format('com.databricks.spark.csv')
    .option("inferSchema", "true")
    .option("header", "true")
    .load("abfss://test@mmaadlsgen2tst.dfs.core.windows.net/pko-stocks-cleansed-w-target.csv"))

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.mmaadlsgen2tst.dfs.core.windows.net","4z/No9iZ6L8SJ6fwEgqCARzw5v4qt1TwQ0nWqTpkFA9jgy0uVlTJgsXb6nDyL4CNsWrDRb88Iu+wb1qbjQoUrA==")

# COMMAND ----------

data_pt = (spark.read
    .format('com.databricks.spark.csv')
    .option("inferSchema", "true")
    .option("header", "true")
    .load("abfss://test@mmaadlsgen2tst.dfs.core.windows.net/pko-stocks-cleansed-w-target.csv"))

# COMMAND ----------

# MAGIC %r
# MAGIC 
# MAGIC dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR)
# MAGIC 
# MAGIC sdf <- read.df("abfss://test@mmaadlsgen2tst.dfs.core.windows.net/pko-stocks-cleansed-w-target.csv", "csv")
# MAGIC str(sdf)

# COMMAND ----------

# MAGIC %r
# MAGIC rdf <- read.csv("abfss://test@mmaadlsgen2tst.dfs.core.windows.net/pko-stocks-cleansed-w-target.csv")
# MAGIC str(rdf)
# MAGIC rdf