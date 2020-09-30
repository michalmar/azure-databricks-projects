# Databricks notebook source
# MAGIC %md # Databricks Spark overview
# MAGIC 
# MAGIC ## intro & setup

# COMMAND ----------

# MAGIC %md Access Azure Key Vault

# COMMAND ----------

dbutils.secrets.get(scope = "azure-keyvault-mma", key = "tlnr-appid")

# COMMAND ----------

# MAGIC %md ### Mounting Storage
# MAGIC 
# MAGIC there are three options:
# MAGIC - **Mount an Azure Data Lake Storage Gen2 account using a service principal and OAuth 2.0**
# MAGIC - Access directly with service principal and OAuth 2.0
# MAGIC - Access directly using the storage account access key
# MAGIC 
# MAGIC ```python
# MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
# MAGIC            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC            "fs.azure.account.oauth2.client.id": "<application-id>",
# MAGIC            "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope = "<scope-name>", key = "<key-name-for-service-credential>"),
# MAGIC            "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}
# MAGIC 
# MAGIC # Optionally, you can add <directory-name> to the source URI of your mount point.
# MAGIC dbutils.fs.mount(
# MAGIC   source = "abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/",
# MAGIC   mount_point = "/mnt/<mount-name>",
# MAGIC   extra_configs = configs)
# MAGIC ```
# MAGIC 
# MAGIC see [documentation](https://docs.databricks.com/data/data-sources/azure/azure-datalake-gen2.html#mount-an-azure-data-lake-storage-gen2-account-using-a-service-principal-and-oauth-20)

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope = "azure-keyvault-mma", key = "tlnr-appid"),
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope = "azure-keyvault-mma", key = "tlnr-app-secret"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://demofs@tlnrtestgen2.dfs.core.windows.net/",
  mount_point = "/mnt/demofs",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# check for the mounted files
# dbutils.fs.ls("/mnt/demofs")
dbutils.fs.ls("/mnt/tlnr")

# COMMAND ----------

# MAGIC %md ## Data Prep

# COMMAND ----------

bts_data = spark.read.parquet(f"/mnt/tlnr/bts_data/")

# COMMAND ----------

sources = ["bts_data","subscriber_data", "probe_data"]

# COMMAND ----------

# read all sources to spark dataframes
sources_dict = {}
for s in sources:
   sources_dict[s] = spark.read.parquet(f"/mnt/tlnr/{s}/")
    
print(f"Sucessfully loaded dataframes:")
for k, _ in sources_dict.items():
  print(f"{k}")

# COMMAND ----------

# display(sources_dict["subscriber_data"])

# COMMAND ----------

# register all dataframes as tables for spark sql

for nm, df in sources_dict.items():
  print(f"creating view for {nm}")
  if (nm == "probe_data"):
    print("{} not chached".format(nm))
  else:
    df.cache()
  cnt = df.count()
  print(f"{nm} rows: {cnt}")
  df.createOrReplaceTempView(nm)

# COMMAND ----------

# MAGIC %md ### it's dataframe after all

# COMMAND ----------

bts_data_df = sources_dict["bts_data"]
bts_data_df.count()

# COMMAND ----------

# MAGIC %md ### use `Pandas`

# COMMAND ----------

bts_data_pdf = bts_data_df.toPandas()
bts_data_pdf

# COMMAND ----------

import pyspark.sql.functions as F
bts_data_df = bts_data_df.withColumn("city_agg", F.when(F.col("city") > 0,"Prague").otherwise(F.lit("Brno")))

# COMMAND ----------

display(bts_data_df.groupby("city_agg").count())

# COMMAND ----------

# MAGIC %sql
# MAGIC select cgi_ecgi, count(1) as row_cnt 
# MAGIC from probe_data 
# MAGIC group by cgi_ecgi
# MAGIC order by row_cnt DESC
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select  count(distinct http_host) from probe_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select http_host , count(1) as row_cnt 
# MAGIC from probe_data 
# MAGIC group by http_host 
# MAGIC order by row_cnt DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bts_data limit 6

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## QUERY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) 
# MAGIC from (
# MAGIC   SELECT
# MAGIC       msisdn
# MAGIC       , http_host
# MAGIC       , count(*)
# MAGIC       , sum(total_volume)
# MAGIC   FROM
# MAGIC       probe_data
# MAGIC   WHERE
# MAGIC       http_host IN (
# MAGIC           "bancaintesa.rs"
# MAGIC           , "raiffeisenbank.rs"
# MAGIC           , "erstebank.rs"
# MAGIC           , "unicreditbank.rs"
# MAGIC           , "kombank.com"
# MAGIC           , "societegenerale.rs"
# MAGIC           , "voban.rs"
# MAGIC           , "sberbank.rs")
# MAGIC   GROUP BY
# MAGIC       msisdn
# MAGIC       , http_host
# MAGIC )

# COMMAND ----------

# MAGIC %md ## QUERY_3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     p.msisdn
# MAGIC     , p.http_host
# MAGIC     , count(*)
# MAGIC     , sum(p.total_volume)
# MAGIC FROM
# MAGIC     probe_data p
# MAGIC LEFT JOIN
# MAGIC     bts_data b ON b.cgi_ecgi = p.cgi_ecgi
# MAGIC WHERE
# MAGIC     http_host IN (
# MAGIC         "bancaintesa.rs"
# MAGIC         , "raiffeisenbank.rs"
# MAGIC         , "erstebank.rs"
# MAGIC         , "unicreditbank.rs"
# MAGIC         , "kombank.com"
# MAGIC         , "societegenerale.rs"
# MAGIC         , "voban.rs"
# MAGIC         , "sberbank.rs")
# MAGIC     AND b.city = (select city from bts_data limit 1)
# MAGIC GROUP BY
# MAGIC     p.msisdn
# MAGIC     , p.http_host