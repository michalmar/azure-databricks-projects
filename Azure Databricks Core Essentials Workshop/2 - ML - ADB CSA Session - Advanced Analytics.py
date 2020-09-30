# Databricks notebook source
# MAGIC %md
# MAGIC # MLflow and AzureML Integration
# MAGIC ### Build, train and test a simple SparkML model using MLflow and then deploy to AzureML
# MAGIC #### Use Databrick Runtime 6.x (Spark 2.4)
# MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/Ignite/intro.jpg" alt="Better Together" width="800">

# COMMAND ----------

# Install MLflow and AzureML packages
dbutils.library.installPyPI("mlflow")
dbutils.library.installPyPI("azureml-sdk")
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch Data

# COMMAND ----------

dataPath = "dbfs:/databricks-datasets/Rdatasets/data-001/csv/datasets/iris.csv"

irisDF = (spark.read               # The DataFrameReader
   .option("header", "true")       # Use first line of all files as header
   .option("inferSchema", "true")  # Automatically infer data types
   .csv(dataPath)                  # Creates a DataFrame from CSV after reading in the file
)

# COMMAND ----------

irisDF = irisDF.drop("_c0") \
               .withColumnRenamed("Sepal.Length", "Sepal_Length") \
               .withColumnRenamed("Sepal.Width", "Sepal_Width") \
               .withColumnRenamed("Petal.Length", "Petal_Length") \
               .withColumnRenamed("Petal.Width", "Petal_Width").cache() \


# COMMAND ----------

display(irisDF)

# COMMAND ----------

display(irisDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import SparkML Library

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import DecisionTreeClassifier

# split the dataset into train and test sets
trainDF, testDF = irisDF.randomSplit([0.8, 0.2], seed=11)

# map categorical labels to numerical
indexer = StringIndexer(inputCol="Species", outputCol="label").fit(irisDF)

assembler = VectorAssembler(inputCols=irisDF.columns[:-1], outputCol="features")

dtc = DecisionTreeClassifier(featuresCol="features", labelCol="label")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC `ParamGridBuilder()` allows us to string together all of the different possible hyperparameters we would like to test.  In this case, we can test the maximum number of iterations, whether we want to use an intercept with the y axis, and whether we want to standardize our features.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> Since grid search works through exhaustively building a model for each combination of parameters, it quickly becomes a lot of different unique combinations of parameters.

# COMMAND ----------

from pyspark.ml.tuning import ParamGridBuilder

paramGrid = (ParamGridBuilder()
  .addGrid(dtc.maxDepth, [2, 3, 4, 5])
  .addGrid(dtc.maxBins,  [8, 16, 24, 32])
  .build()
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Cross-Validation
# MAGIC 
# MAGIC There are a number of different ways of conducting cross-validation, allowing us to trade off between computational expense and model performance.  An exhaustive approach to cross-validation would include every possible split of the training set.  More commonly, _k_-fold cross-validation is used where the training dataset is divided into _k_ smaller sets, or folds.  A model is then trained on _k_-1 folds of the training data and the last fold is used to evaluate its performance.

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator

evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="label", metricName="accuracy")

cv = CrossValidator(
  estimator = dtc,                  # Estimator (individual model or pipeline)
  estimatorParamMaps = paramGrid,   # Grid of parameters to try (grid search)
  evaluator = evaluator,            # Evaluator
  numFolds = 3,                     # Set k to 3
  seed = 10                         # Seed to sure our results are the same if ran again
)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC Add `VectorAssembler()` and `CrossValidator()` to a `Pipeline()` and fit it to the training dataset. 
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> This will train a large number of models.  If your cluster size is too small, it could take a while.

# COMMAND ----------

from pyspark.ml import Pipeline

pipeline = Pipeline(stages = [indexer, assembler, cv])

cvModel = pipeline.fit(trainDF)

# COMMAND ----------

# MAGIC %md
# MAGIC You can then access the best model using the `.bestModel` attribute.

# COMMAND ----------

bestModel = cvModel.stages[-1].bestModel
print(bestModel)

# get the best value for maxDepth parameter
bestDepth = bestModel.getOrDefault("maxDepth")
bestBins = bestModel.getOrDefault("maxBins")

print("bestDepth =", bestDepth, ", bestBins", bestBins)

# COMMAND ----------

# MAGIC %md
# MAGIC Build final model using the entire training dataset and evaluate its performance using the test set
# MAGIC 
# MAGIC Log parameters, metrics, and the model itself in MLflow

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train Final Model

# COMMAND ----------

import mlflow.spark

with mlflow.start_run(run_name="final_model") as run:
  runID = run.info.run_uuid
  
  # train model
  dtc = DecisionTreeClassifier(featuresCol="features", labelCol="label", maxDepth=bestDepth, maxBins=bestBins)
  pipeline = Pipeline(stages = [indexer, assembler, dtc])
  finalModel = pipeline.fit(trainDF)
  
  # log parameters and model
  mlflow.log_param("maxDepth", bestDepth)
  mlflow.log_param("maxBins", bestBins)
  mlflow.spark.log_model(finalModel, "model")
  
  # generate and log metrics
  testPredictionDF = finalModel.transform(testDF)
  accuracy = evaluator.evaluate(testPredictionDF)
  mlflow.log_metric("accuracy", accuracy)
  print("Accuracy on the test set for the decision tree model: {}".format(accuracy))

# COMMAND ----------

model = finalModel.stages[-1]
display(model)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evaluate Feature Importance

# COMMAND ----------

# zip the list of features with their scores
scores = zip(assembler.getInputCols(), model.featureImportances)

# and pretty print theem
for x in scores: print("%-15s = %s" % x)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register Model
# MAGIC 
# MAGIC #### Create a new registered model using the API
# MAGIC 
# MAGIC The following cells use the `mlflow.register_model()` function to create a new registered model named `IrisModel`. This also creates a new model version (e.g., `Version 1` of `IrisModel`).

# COMMAND ----------

import time

model_name = "IrisModel"

artifactPath = "model"
model_URI = "runs:/{run_id}/{artifact_path}".format(run_id=runID, artifact_path=artifactPath)

modelDetails = mlflow.register_model(model_uri=model_URI, name=model_name)
time.sleep(5)

# COMMAND ----------

# MAGIC %md ### Perform a model stage transition
# MAGIC 
# MAGIC The MLflow Model Registry defines several model stages: **None**, **Staging**, **Production**, and **Archived**. Each stage has a unique meaning. For example, **Staging** is meant for model testing, while **Production** is for models that have completed the testing or review processes and have been deployed to applications.

# COMMAND ----------

from mlflow.tracking.client import MlflowClient
client = MlflowClient()

client.transition_model_version_stage(
  name = modelDetails.name,
  version = modelDetails.version,
  stage='Production',
)

# COMMAND ----------

# MAGIC %md The MLflow Model Registry allows multiple model versions to share the same stage. When referencing a model by stage, the Model Registry will use the latest model version (the model version with the largest version ID). The `MlflowClient.get_latest_versions()` function fetches the latest model version for a given stage or set of stages. The following cell uses this function to print the latest version of the power forecasting model that is in the `Production` stage.

# COMMAND ----------

latestVersionInfo = client.get_latest_versions(model_name, stages=["Production"])
latestVersion = latestVersionInfo[0].version
print("The latest production version of the model '%s' is '%s'." % (model_name, latestVersion))

model_URI = latestVersionInfo[0].source
modelPipeline = mlflow.spark.load_model(model_URI)
print("Loading registered model version from URI: '{model_uri}'".format(model_uri=model_URI))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deploy the model to Azure Container Instance (using Azure ML SDK)
# MAGIC 
# MAGIC Note: You must fill in the variables for your AML workspace

# COMMAND ----------

# DBTITLE 1,Fill in the variables for your AML workspace & authenticate
from azureml.core import Workspace
from azureml.core.webservice import AciWebservice, Webservice

# Azure Subscription ID
subscription_id = 'FILL-IN-WITH-SUB-ID'
# Azure Machine Learning resource group NOT the managed resource group
resource_group = 'FILL-IN-WITH-RG-NAME' 
# Azure Machine Learning workspace name, NOT Azure Databricks workspace
workspace_name = 'FILL-IN-WITH-WORKSPACE-NAME'  
# Azure ML workspace location 
location = "FILL-IN-WITH-WORKSPACE-REGION"

# Load or create an Azure ML Workspace
azure_workspace = Workspace.create(name=workspace_name,
                                   subscription_id=subscription_id,
                                   resource_group=resource_group,
                                   location=location,
                                   create_resource_group=True,
                                   exist_ok=True)

# COMMAND ----------

import os
print("copy model from DBFS to local")

model_local = "file:" + os.getcwd() + "/" + model_name
dbutils.fs.cp(model_URI + "/sparkml/", model_local, True)

# COMMAND ----------

#Register the model in Azure ML
from azureml.core.model import Model
mymodel = Model.register(model_path = model_name, # this points to a local file
                         model_name = model_name, # this is the name the model is registered as
                         description = "ADB trained model - iris classification",
                         workspace = azure_workspace)

print(mymodel.name, mymodel.description, mymodel.version)

# COMMAND ----------

# Write Inference Script file >> score_sparkml.py

score_sparkml = """
import json
 
def init():
    # One-time initialization of PySpark and predictive model
    import pyspark
    import os
    from azureml.core.model import Model
    from pyspark.ml import PipelineModel
 
    global trainedModel
    global spark
 
    spark = pyspark.sql.SparkSession.builder.appName("Iris Classifier").getOrCreate()
    model_name = "{model_name}" #interpolated
    # AZUREML_MODEL_DIR is an environment variable created during deployment.
    # It is the path to the model folder (./azureml-models/$MODEL_NAME/$VERSION)
    model_path = os.path.join(os.getenv('AZUREML_MODEL_DIR'), model_name)
    trainedModel = PipelineModel.load(model_path)
    
def run(raw_data):
  try:
    input_list = json.loads(raw_data)["data"]
    sc = spark.sparkContext
    input_rdd = sc.parallelize(input_list)
    input_df = input_rdd.toDF()
    pred_df = trainedModel.transform(input_df)
    pred_list = pred_df.collect()
    pred_array = [int(x["prediction"]) for x in pred_list]
    return pred_array
  except Exception as e:
    result = str(e)
    return "Internal Exception : " + result
    
""".format(model_name=model_name)
 
exec(score_sparkml)
 
with open("score_sparkml.py", "w") as file:
    file.write(score_sparkml)

# COMMAND ----------

from azureml.core.webservice import AciWebservice, Webservice
from azureml.core.environment import Environment
from azureml.core.model import InferenceConfig
  
# Create deploy config object
aci_conf = AciWebservice.deploy_configuration(cpu_cores=2,
                                              memory_gb=2,
                                              description="This is for ADB and AML - Iris model")
 
# Create yaml to set azure-ml dependency
myenv = Environment(name="myenv")
myenv.python.conda_dependencies.save("./myenv.yml")
 
# Create inference config with score.py
inf_conf = InferenceConfig(entry_script="score_sparkml.py",
                           conda_file="myenv.yml",
                           runtime="spark-py")
 
# Deploy and Publish (start your service) !
svc = Model.deploy(name="sparkml-service",
                   deployment_config=aci_conf,
                   models=[mymodel],
                   inference_config=inf_conf,
                   workspace=azure_workspace)

# This will take a few minutes
svc.wait_for_deployment()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the REST Endpoint

# COMMAND ----------

import requests
 
headers = {"Content-Type":"application/json"}

input_data = """
{
  "data": [
    {
      "Sepal_Length" : 4.2,
      "Sepal_Width"  : 3.0,
      "Petal_Length" : 1.1,
      "Petal_Width"  : 0.2
    },
    {
      "Sepal_Length" : 6.2,
      "Sepal_Width"  : 3.0,
      "Petal_Length" : 4.1,
      "Petal_Width"  : 1.2
    }
  ]
}
"""

demo_scoring_uri = "http://688f205d-c75d-429d-a0d5-c625e95f1dfe.southeastasia.azurecontainer.io/score"

http_res = requests.post(demo_scoring_uri,   # svc.scoring_uri,
                         input_data,
                         headers = headers)

print("Predicted : ", http_res.text)

# COMMAND ----------

# MAGIC %md ### Cleanup

# COMMAND ----------

# delete AML webservice
svc.delete()


# loop over registered models in MLflow 
models = client.search_model_versions("name='{}'".format(model_name))
for model in models:
  try:
    # set model stage to Archive
    client.transition_model_version_stage(name=model_name, version=model.version, stage='Archived')
  except:
    pass
  # delete version of model
  client.delete_model_version(model_name, model.version)

# delete model
client.delete_registered_model(model_name)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>