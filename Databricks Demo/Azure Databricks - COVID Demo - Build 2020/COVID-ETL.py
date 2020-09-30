# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, TimestampType
schema = StructType([
  StructField('FIPS', IntegerType(), True), 
  StructField('Admin2', StringType(), True),
  StructField('Province_State', StringType(), True),  
  StructField('Country_Region', StringType(), True),  
  StructField('Last_Update', TimestampType(), True),  
  StructField('Lat', DoubleType(), True),  
  StructField('Long_', DoubleType(), True),
  StructField('Confirmed', IntegerType(), True), 
  StructField('Deaths', IntegerType(), True), 
  StructField('Recovered', IntegerType(), True), 
  StructField('Active', IntegerType(), True),   
  StructField('Combined_Key', StringType(), True),  
  StructField('process_date', DateType(), True),    
])

# Create initial empty Spark DataFrame based on preceding schema
jhu_daily = spark.createDataFrame([], schema)

# COMMAND ----------

import os
import pandas as pd
import glob
from pyspark.sql.functions import input_file_name, lit, col

# Creates a list of all csv files
globbed_files = glob.glob("/dbfs/databricks-datasets/COVID/CSSEGISandData/csse_covid_19_data/csse_covid_19_daily_reports/*.csv") 
#globbed_files = glob.glob("/dbfs/databricks-datasets/COVID/CSSEGISandData/csse_covid_19_data/csse_covid_19_daily_reports/04*.csv")

i = 0
for csv in globbed_files:
  # Filename
  source_file = csv[5:200]
  process_date = csv[100:104] + "-" + csv[94:96] + "-" + csv[97:99]
  
  # Read data into temporary dataframe
  df_tmp = spark.read.option("inferSchema", True).option("header", True).csv(source_file)
  df_tmp.createOrReplaceTempView("df_tmp")

  # Obtain schema
  schema_txt = ' '.join(map(str, df_tmp.columns)) 
  
  # Three schema types (as of 2020-04-08) 
  schema_01 = "Province/State Country/Region Last Update Confirmed Deaths Recovered" # 01-22-2020 to 02-29-2020
  schema_02 = "Province/State Country/Region Last Update Confirmed Deaths Recovered Latitude Longitude" # 03-01-2020 to 03-21-2020
  schema_03 = "FIPS Admin2 Province_State Country_Region Last_Update Lat Long_ Confirmed Deaths Recovered Active Combined_Key" # 03-22-2020 to
  
  # Insert data based on schema type
  if (schema_txt == schema_01):
    df_tmp = (df_tmp
                .withColumn("FIPS", lit(None).cast(IntegerType()))
                .withColumn("Admin2", lit(None).cast(StringType()))
                .withColumn("Province_State", col("Province/State"))
                .withColumn("Country_Region", col("Country/Region"))
                .withColumn("Last_Update", col("Last Update"))
                .withColumn("Lat", lit(None).cast(DoubleType()))
                .withColumn("Long_", lit(None).cast(DoubleType()))
                .withColumn("Active", lit(None).cast(IntegerType()))
                .withColumn("Combined_Key", lit(None).cast(StringType()))
                .withColumn("process_date", lit(process_date))
                .select("FIPS", 
                        "Admin2", 
                        "Province_State", 
                        "Country_Region", 
                        "Last_Update", 
                        "Lat", 
                        "Long_", 
                        "Confirmed", 
                        "Deaths", 
                        "Recovered", 
                        "Active", 
                        "Combined_Key", 
                        "process_date")
               )
    jhu_daily = jhu_daily.union(df_tmp)
  elif (schema_txt == schema_02):
    df_tmp = (df_tmp
                .withColumn("FIPS", lit(None).cast(IntegerType()))
                .withColumn("Admin2", lit(None).cast(StringType()))
                .withColumn("Province_State", col("Province/State"))
                .withColumn("Country_Region", col("Country/Region"))
                .withColumn("Last_Update", col("Last Update"))
                .withColumn("Lat", col("Latitude"))
                .withColumn("Long_", col("Longitude"))
                .withColumn("Active", lit(None).cast(IntegerType()))
                .withColumn("Combined_Key", lit(None).cast(StringType()))
                .withColumn("process_date", lit(process_date))
                .select("FIPS", 
                        "Admin2", 
                        "Province_State", 
                        "Country_Region", 
                        "Last_Update", 
                        "Lat", 
                        "Long_", 
                        "Confirmed", 
                        "Deaths", 
                        "Recovered", 
                        "Active", 
                        "Combined_Key", 
                        "process_date")
               )
    jhu_daily = jhu_daily.union(df_tmp)

  elif (schema_txt == schema_03):
    df_tmp = df_tmp.withColumn("process_date", lit(process_date))
    jhu_daily = jhu_daily.union(df_tmp)
  else:
    print("Schema may have changed")
    print(schema_txt)
#     raise
    next
  
  # print out the schema being processed by date
  print("%s | %s" % (process_date, schema_txt))

# COMMAND ----------

jhu_daily.createOrReplaceTempView("jhu_daily_covid")

# COMMAND ----------

# MAGIC %sh mkdir -p /dbfs/tmp/kyweller/COVID/population_estimates_by_county/ && wget -O /dbfs/tmp/kyweller/COVID/population_estimates_by_county/co-est2019-alldata.csv https://raw.githubusercontent.com/databricks/tech-talks/master/datasets/co-est2019-alldata.csv && ls -al /dbfs/tmp/kyweller/COVID/population_estimates_by_county/

# COMMAND ----------

map_popest_county = spark.read.option("header", True).option("inferSchema", True).csv("/tmp/kyweller/COVID/population_estimates_by_county/co-est2019-alldata.csv")
map_popest_county.createOrReplaceTempView("map_popest_county")
fips_popest_county = spark.sql("select State * 1000 + substring(cast(1000 + County as string), 2, 3) as fips, STNAME, CTYNAME, census2010pop, POPESTIMATE2019 from map_popest_county")
fips_popest_county.createOrReplaceTempView("fips_popest_county")

# COMMAND ----------

jhu_daily_pop = spark.sql("""
SELECT f.FIPS, f.Admin2, f.Province_State, f.Country_Region, f.Last_Update, f.Lat, f.Long_, f.Confirmed, f.Deaths, f.Recovered, f.Active, f.Combined_Key, f.process_date, p.POPESTIMATE2019 
  FROM jhu_daily_covid f
    JOIN fips_popest_county p
      ON p.fips = f.FIPS
""")
jhu_daily_pop.createOrReplaceTempView("jhu_daily_pop")