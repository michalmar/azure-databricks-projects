# Databricks notebook source
# MAGIC %run "./Course-Name"

# COMMAND ----------

# MAGIC %run "./Dataset-Mounts"

# COMMAND ----------

# MAGIC %run "./Test-Library"

# COMMAND ----------

# MAGIC %run "./Create-User-DB"

# COMMAND ----------

#SETUP

from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import col, to_date, weekofyear
from pyspark.sql import DataFrame

basePath = userhome + "/capstone"
outputPathBronzeTest = basePath + "/gaming/bronzeTest"
outputPathSilverTest = basePath + "/gaming/silverTest"
outputPathGoldTest = basePath + "/gaming/goldTest"

eventSchema = ( StructType()
  .add('eventName', StringType()) 
  .add('eventParams', StructType() 
    .add('game_keyword', StringType()) 
    .add('app_name', StringType()) 
    .add('scoreAdjustment', IntegerType()) 
    .add('platform', StringType()) 
    .add('app_version', StringType()) 
    .add('device_id', StringType()) 
    .add('client_event_time', TimestampType()) 
    .add('amount', DoubleType()) 
  )     
)

singleFilePath = "/mnt/training/enb/capstone-data/single"

class Key:
  singleStreamDF = (spark
    .readStream
    .schema(eventSchema) 
    .option('streamName','mobilestreaming_test') 
    .option("maxFilesPerTrigger", 1)                # treat each file as Trigger event                    
    .json(singleFilePath) 
  )

  bronzeDF  = (spark
    .read
    .format("delta")
    .load("/mnt/training/enb/capstone-data/bronze") 
  )

  correctLookupDF = (spark.read
    .format("delta")
    .load("/mnt/training/enb/capstone-data/lookup"))

  silverDF = (spark
    .read
    .format("delta")
    .load("/mnt/training/enb/capstone-data/silver") 
  )

  goldDF = (spark
    .read
    .format("delta") 
    .load("/mnt/training/enb/capstone-data/gold") 
  )

None

# COMMAND ----------

# BRONZE

from pyspark.sql import DataFrame
import time

def realityCheckBronze(writeToBronze, display = True):

  dbutils.fs.rm(outputPathBronzeTest, True)
  
  bronze_tests = TestSuite()

  try:
    writeToBronze(Key.singleStreamDF, outputPathBronzeTest, "bronze_test")

    def groupAndCount(df: DataFrame):
      return df.select('eventName').groupBy('eventName').count()

    for s in spark.streams.active:
        if s.name == "bronze_test":
          first = True
          while (len(s.recentProgress) == 0): 
            if first:
              print("waiting for stream to start...")
              first = False
            time.sleep(5)

    try:
      testDF = (spark
          .read
          .format("delta")
          .load(outputPathBronzeTest))
    except Exception as e:
      print(e)
      testDF = (spark
        .read
        .load(outputPathBronzeTest))

    test_dtype = findColumnDatatype(testDF, 'eventDate')

    historyDF = spark.sql("DESCRIBE HISTORY delta.`{}`".format(outputPathBronzeTest))

    bronze_tests.test(id = "rc_bronze_delta_format", points = 2, description = "Is in Delta format", 
        testFunction = lambda: isDelta(outputPathBronzeTest))
    bronze_tests.test(id = "rc_bronze_contains_columns", points = 2, description = "Dataframe contains eventDate column",              
            testFunction = lambda: verifyColumnsExists(testDF, ['eventDate']))
    bronze_tests.test(id = "rc_bronze_correct_schema", points = 2, description = "Returns correct schema", 
            testFunction = lambda: checkSchema(testDF.schema, Key.bronzeDF.schema))
    bronze_tests.test(id = "rc_bronze_column_check", points = 2, description = "eventDate column is correct data type",              
            testFunction = lambda: test_dtype == "date")
    bronze_tests.test(id = "rc_bronze_null_check", points = 2, description = "Does not contain nulls",              
            testFunction = lambda: checkForNulls(testDF, 'eventParams'))
    bronze_tests.test(id = "rc_bronze_is_streaming", points = 2, description = "Is streaming DataFrame",              
            testFunction = lambda: isStreamingDataframe(historyDF))
    bronze_tests.test(id = "rc_bronze_output_mode", points = 2, description = "Output mode is Append",              
            testFunction = lambda: checkOutputMode(historyDF, "Append"))
    bronze_tests.test(id = "rc_bronze_correct_rows", points = 2, description = "Returns a Dataframe with the correct number of rows",      
            testFunction = lambda: testDF.count() == Key.bronzeDF.count())
    bronze_tests.test(id = "rc_bronze_correct_df", points = 2, description = "Returns the correct Dataframe",             
            testFunction = lambda: compareDataFrames(groupAndCount(testDF), groupAndCount(Key.bronzeDF)))

    # get bronze_tests results using bronze_tests.passed, bronze_tests.percentage
    
    if (display):
      bronze_tests.displayResults()

  finally:
    for s in spark.streams.active:
      if s.name == 'bronze_test':
        try:
          s.stop()
        except Exception as e:
          print('!!', e)

displayHTML("""
Declared the following function:
  <li><span style="color:green; font-weight:bold">realityCheckBronze</span></li>
""")

# COMMAND ----------

# STATIC

def realityCheckStatic(loadStaticData, display = True):
  
  static_tests = TestSuite()
  
  testDF = loadStaticData("/mnt/training/enb/capstone-data/lookup")
  
  static_tests.test(id = "rc_static_count", points = 2, description = "Has the correct number of rows", 
             testFunction = lambda: testDF.count() == 475)
  static_tests.test(id = "rc_static_schema", points = 2, description = "Returns correct schema", 
               testFunction = lambda: checkSchema(testDF.schema, Key.correctLookupDF.schema))

  if (display):
    static_tests.displayResults()
  
displayHTML("""
Declared the following function:
  <li><span style="color:green; font-weight:bold">realityCheckStatic</span></li>
""")

# COMMAND ----------

# SILVER

def realityCheckSilver(bronzeToSilver, display = True):
  
  dbutils.fs.rm(outputPathSilverTest, True)
  
  silver_tests = TestSuite()
  
  try:

    bronzeToSilver(outputPathBronzeTest, outputPathSilverTest, "silver_test", Key.correctLookupDF)
    
    def groupAndCount(df: DataFrame):
      try:
        return df.select('deviceType').groupBy('deviceType').count()
      except:
        print("deviceType not found")
    
    for s in spark.streams.active:
        first = True
        while (len(s.recentProgress) == 0): 
          if first:
            print("waiting for stream to start...")
            first = False
          time.sleep(5)

    try:
      testDF = (spark
          .read
          .format("delta")
          .load(outputPathSilverTest))
    except Exception as e:
      testDF = (spark
        .read
        .load(outputPathSilverTest))

    historyDF = spark.sql("DESCRIBE HISTORY delta.`{}`".format(outputPathSilverTest))
    
    silver_tests.test(id = "rc_silver_delta_format", points = 2, description = "Is in Delta format", 
            testFunction = lambda: isDelta(outputPathSilverTest))
    silver_tests.test(id = "rc_silver_contains_columns", points = 2, description = "Dataframe contains device_id, client_event_time, deviceType columns",              
            testFunction = lambda: verifyColumnsExists(testDF, ["device_id", "client_event_time", "deviceType"]))
    silver_tests.test(id = "rc_silver_correct_schema", points = 2, description = "Returns correct schema", 
            testFunction = lambda: checkSchema(testDF.schema, Key.silverDF.schema))
    silver_tests.test(id = "rc_silver_null_check", points = 2, description = "Does not contain nulls",              
            testFunction = lambda: checkForNulls(testDF, "eventName"))
    silver_tests.test(id = "rc_silver_is_streaming", points = 2, description = "Is streaming DataFrame",              
            testFunction = lambda: isStreamingDataframe(historyDF))
    silver_tests.test(id = "rc_silver_output_mode", points = 2, description = "Output mode is Append",              
            testFunction = lambda: checkOutputMode(historyDF, "Append"))
    silver_tests.test(id = "rc_silver_correct_rows", points = 2, description = "Returns a Dataframe with the correct number of rows",              
            testFunction = lambda: testDF.count() == Key.silverDF.count())
    silver_tests.test(id = "rc_silver_correct_df", points = 2, description = "Returns the correct Dataframe",              
            testFunction = lambda: compareDataFrames(groupAndCount(testDF), groupAndCount(Key.silverDF)))

    if (display):
      silver_tests.displayResults()

  finally:
    for s in spark.streams.active:
      if s.name == 'silver_test':
        s.stop()
  
displayHTML("""
Declared the following function:
  <li><span style="color:green; font-weight:bold">realityCheckSilver</span></li>
""")

# COMMAND ----------

# GOLD

def realityCheckGold(silverToGold, display = True):
  
  dbutils.fs.rm(outputPathGoldTest, True)
  
  gold_tests = TestSuite()
  
  try:
  
    silverToGold(outputPathSilverTest, outputPathGoldTest, "gold_test")

    for s in spark.streams.active:
        first = True
        while (len(s.recentProgress) == 0): 
          if first:
            print("waiting for stream to start...")
            first = False
          time.sleep(5)
          
    try:
      testDF = (spark
        .read
        .format("delta")
        .load(outputPathGoldTest))
    except Exception as e:
      testDF = (spark
        .read
        .load(outputPathGoldTest))

    historyDF = spark.sql("DESCRIBE HISTORY delta.`{}`".format(outputPathGoldTest))

    gold_tests.test(id = "rc_gold_delta_format", points = 2, description = "Is in Delta format", 
             testFunction = lambda: isDelta(outputPathGoldTest))
    gold_tests.test(id = "rc_gold_contains_columns", points = 2, description = "Dataframe contains week and WAU columns",              
             testFunction = lambda: verifyColumnsExists(testDF, ["week", "WAU"]))
    gold_tests.test(id = "rc_gold_correct_schema", points = 2, description = "Returns correct schema", 
             testFunction = lambda: checkSchema(testDF.schema, Key.goldDF.schema))
    gold_tests.test(id = "rc_gold_null_check", points = 2, description = "Does not contain nulls",              
             testFunction = lambda: checkForNulls(testDF, "eventName"))
    gold_tests.test(id = "rc_gold_is_streaming", points = 2, description = "Is streaming DataFrame",              
             testFunction = lambda: isStreamingDataframe(historyDF))
    gold_tests.test(id = "rc_gold_output_mode", points = 2, description = "Output mode is Complete",              
             testFunction = lambda: checkOutputMode(historyDF, "Complete"))
    gold_tests.test(id = "rc_gold_correct_rows", points = 2, description = "Returns a Dataframe with the correct number of rows",              
             testFunction = lambda: testDF.count() == Key.goldDF.count())
    gold_tests.test(id = "rc_gold_correct_df", points = 2, description = "Returns the correct Dataframe",              
             testFunction = lambda: compareDataFrames(testDF.sort("week"), Key.goldDF.sort("week")))

    if (display):
      gold_tests.displayResults()

  finally:
    for s in spark.streams.active:
      if s.name == 'gold_test':
        s.stop()
  
displayHTML("""
Declared the following function:
  <li><span style="color:green; font-weight:bold">realityCheckGold</span></li>
""")
