# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Databricks COVID-19 Data Analysis
# MAGIC ### Full notebook available at: https://aka.ms/DatabricksBuild2020
# MAGIC #### For purposes of demo, please don't take any of this information as accurate or scientific fact...

# COMMAND ----------

# DBTITLE 1,This Demo leverages the 2019-nCoV dataset by Johns Hopkins CSSE
# MAGIC %fs ls "/databricks-datasets/COVID/"

# COMMAND ----------

# DBTITLE 1,Ingest, merge, clean variety of data from January through May
# MAGIC %run ./COVID-ETL

# COMMAND ----------

# DBTITLE 1,Inspect the data available 
# MAGIC %sql 
# MAGIC select * from jhu_daily_covid

# COMMAND ----------

# DBTITLE 1,Total confirmed cases and deaths in King County Washington
# MAGIC %sql
# MAGIC select process_date, Admin2, Confirmed, Deaths, Recovered, Active from jhu_daily_pop where Province_State in ('Washington') and Admin2 in ('King') order by process_date

# COMMAND ----------

# DBTITLE 1,Calculate relative cases/deaths per 100k people
df_usa = spark.sql("""
  select fips, 
         cast(100000.*Confirmed/POPESTIMATE2019 as int) as confirmed_per100K, 
         cast(100000.*Deaths/POPESTIMATE2019 as int) as deaths_per100K, 
         recovered, 
         active, 
         lat, 
         long_, 
         admin2 as county, 
         province_state as state, 
         process_date, 
         cast(replace(process_date, '-', '') as integer) as process_date_num 
 from jhu_daily_pop 
 where lat is not null and long_ is not null and fips is not null and (lat <> 0 and long_ <> 0)
""")

df_usa.createOrReplaceTempView("df_usa")

# COMMAND ----------

# DBTITLE 1,Normalize the time series
process_date_zero = spark.sql("select min(process_date) from df_usa where fips is not null").collect()[0][0]
df_usa_conf = spark.sql("""
select fips, 100 + datediff(process_date, '""" + process_date_zero + """') as day_num, confirmed_per100K
  from (
     select fips, process_date, max(confirmed_per100K) as confirmed_per100K
       from df_usa
      group by fips, process_date
) x """)
df_usa_conf.createOrReplaceTempView("df_usa_conf")

df_usa_deaths = spark.sql("""
select lat, long_, 100 + datediff(process_date, '""" + process_date_zero + """') as day_num, deaths_per100K
  from (
     select lat, long_, process_date, max(deaths_per100K) as deaths_per100K
       from df_usa
      group by lat, long_, process_date
) x """)
df_usa_deaths.createOrReplaceTempView("df_usa_deaths")

# COMMAND ----------

# DBTITLE 1,Create per US County heatmap of cases and deaths relative per capita   -    (full code available at https://aka.ms/DatabricksBuild2020)
# MAGIC %run ./prepare-chart

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notice the time it took to run these queries (~7.7sec and ~18sec)
# MAGIC ### Let's see if we can do better with Delta Lake

# COMMAND ----------

# DBTITLE 1,Save same dataset as Delta format
spark.sql("select * from jhu_daily_pop").write.format("delta").mode("overwrite").partitionBy("Province_State").save("/tmp/kyweller/COVID_DeltaLake/jhu_daily_pop_delta/")

# COMMAND ----------

spark.sql("select * from jhu_daily_pop").write.format("parquet").mode("overwrite").partitionBy("Province_State").save("/tmp/kyweller/COVID_DeltaLake/jhu_daily_pop_parquet/")

# COMMAND ----------

# DBTITLE 1,Optimize and Z-ORDER
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS jhu_daily_pop_deltalake;
# MAGIC CREATE TABLE jhu_daily_pop_deltalake USING DELTA LOCATION '/tmp/kyweller/COVID_DeltaLake/jhu_daily_pop_delta/';
# MAGIC OPTIMIZE jhu_daily_pop_deltalake ZORDER BY (process_date);

# COMMAND ----------

# DBTITLE 1,Same exact query runs ~0.3sec (~20x speed up)
# MAGIC %sql
# MAGIC select process_date, Admin2, Confirmed, Deaths, Recovered, Active from jhu_daily_pop_deltalake where Province_State in ('Washington') and Admin2 in ('King') order by process_date

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS jhu_daily_pop_parquet;
# MAGIC CREATE TABLE jhu_daily_pop_parquet USING PARQUET LOCATION '/tmp/kyweller/COVID_DeltaLake/jhu_daily_pop_parquet/';
# MAGIC -- OPTIMIZE jhu_daily_pop_deltalake ZORDER BY (process_date);

# COMMAND ----------

# DBTITLE 1,Same exact query runs ~0.3sec (~20x speed up)
# MAGIC %sql
# MAGIC select process_date, Admin2, Confirmed, Deaths, Recovered, Active from jhu_daily_pop_parquet where Province_State in ('Washington') and Admin2 in ('King') order by process_date

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Old query for reference
# MAGIC %sql
# MAGIC select process_date, Admin2, Confirmed, Deaths, Recovered, Active from jhu_daily_pop where Province_State in ('Washington') and Admin2 in ('King') order by process_date

# COMMAND ----------

# MAGIC %md
# MAGIC # Appendix
# MAGIC # -
# MAGIC # -
# MAGIC # -
# MAGIC # -
# MAGIC # -
# MAGIC # -
# MAGIC # -
# MAGIC # -
# MAGIC # -
# MAGIC # -

# COMMAND ----------

df_usa_conf.write.format("delta").mode("overwrite").save("/tmp/kyweller/COVID_DeltaLake/df_usa_conf/")
df_usa_deaths.write.format("delta").mode("overwrite").save("/tmp/kyweller/COVID_DeltaLake/df_usa_deaths/")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS df_usa_conf_delta;
# MAGIC CREATE TABLE df_usa_conf_delta USING DELTA LOCATION '/tmp/kyweller/COVID_DeltaLake/df_usa_conf/';
# MAGIC OPTIMIZE df_usa_conf_delta ZORDER BY (day_num);
# MAGIC 
# MAGIC DROP TABLE IF EXISTS df_usa_deaths_delta;
# MAGIC CREATE TABLE df_usa_deaths_delta USING DELTA LOCATION '/tmp/kyweller/COVID_DeltaLake/df_usa_deaths/';
# MAGIC OPTIMIZE df_usa_deaths_delta ZORDER BY (day_num);

# COMMAND ----------

# MAGIC %run ./prepare-chart-DeltaLake