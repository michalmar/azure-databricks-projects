# Databricks notebook source
# MAGIC %md # Training Machine Learning Models on Tabular Data: An End-to-End Example
# MAGIC 
# MAGIC This tutorial covers the following steps:
# MAGIC - Import data from your local machine into the Databricks File System (DBFS)
# MAGIC - Visualize the data using Seaborn and matplotlib
# MAGIC - Run a parallel hyperparameter sweep to train machine learning models on the dataset
# MAGIC - Explore the results of the hyperparameter sweep with MLflow
# MAGIC - Register the best performing model in MLflow
# MAGIC - Apply the registered model to another dataset using a Spark UDF
# MAGIC 
# MAGIC In this example, you build a model to predict the quality of Portugese "Vinho Verde" wine based on the wine's physicochemical properties. 
# MAGIC 
# MAGIC The example uses a dataset from the UCI Machine Learning Repository, presented in [*
# MAGIC Modeling wine preferences by data mining from physicochemical properties*](https://www.sciencedirect.com/science/article/pii/S0167923609001377?via%3Dihub) [Cortez et al., 2009].
# MAGIC 
# MAGIC ### Setup
# MAGIC - Launch a Python 3 cluster running the Databricks Runtime 6.5 ML or above

# COMMAND ----------

# MAGIC %md ### Setup AML - install necessary libraries
# MAGIC 
# MAGIC `%pip install azureml-sdk[databricks]`
# MAGIC 
# MAGIC `%pip install azureml-mlflow`

# COMMAND ----------

# MAGIC %md ## Importing Data
# MAGIC   
# MAGIC In this section, you download a dataset from the web and upload it to Databricks File System (DBFS).
# MAGIC 
# MAGIC 1. Navigate to https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/ and download both `winequality-red.csv` and `winequality-white.csv` to your local machine.
# MAGIC 
# MAGIC 1. From this Databricks notebook, select *File* > *Upload Data*, and drag these files to the drag-and-drop target to upload them to the Databricks File System (DBFS). 
# MAGIC 
# MAGIC     **Note**: if you don't have the *File* > *Upload Data* option, you can load the dataset from the Databricks example datasets. Uncomment and run the last two lines in the following cell.
# MAGIC 
# MAGIC 1. Click *Next*. Some auto-generated code to load the data appears. Select *pandas*, and copy the example code. 
# MAGIC 
# MAGIC 1. Create a new cell, then paste in the sample code. It will look similar to the code shown in the following cell. Make these changes:
# MAGIC   - Pass `.sep=';'` to `pd.read_csv`
# MAGIC   - Change the variable names from `df1` and `df2` to `white_wine` and `red_wine`, as shown in the following cell.

# COMMAND ----------

print(1)

# COMMAND ----------

# If you have the File > Upload Data menu option, follow the instructions in the previous cell to upload the data from your local machine.
# The generated code, including the required edits described in the previous cell, is shown here for reference.

import pandas as pd

# In the following lines, replace <username> with your username.
white_wine = pd.read_csv("/dbfs/FileStore/shared_uploads/mimarusa@microsoft.com/winequality_white.csv", sep=';')
red_wine = pd.read_csv("/dbfs/FileStore/shared_uploads/mimarusa@microsoft.com/winequality_red.csv", sep=';')

# If you do not have the File > Upload Data menu option, uncomment and run these lines to load the dataset.

#white_wine = pd.read_csv("/dbfs/databricks-datasets/wine-quality/winequality-white.csv", sep=";")
#red_wine = pd.read_csv("/dbfs/databricks-datasets/wine-quality/winequality-red.csv", sep=";")

# COMMAND ----------

# MAGIC %md Merge the two DataFrames into a single dataset, with a new binary feature "is_red" that indicates whether the wine is red or white.

# COMMAND ----------

red_wine['is_red'] = 1
white_wine['is_red'] = 0

data = pd.concat([red_wine, white_wine], axis=0)

# Remove spaces from column names
data.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)

# COMMAND ----------

data.head()

# COMMAND ----------

# MAGIC %md ##Data Visualization
# MAGIC 
# MAGIC Before training a model, explore the dataset using Seaborn and Matplotlib.

# COMMAND ----------

# MAGIC %md Plot a histogram of the dependent variable, quality.

# COMMAND ----------

import seaborn as sns
sns.distplot(data.quality, kde=False)

# COMMAND ----------

# MAGIC %md Looks like quality scores are normally distributed between 3 and 9. 
# MAGIC 
# MAGIC Define a wine as high quality if it has quality >= 7.

# COMMAND ----------

high_quality = (data.quality >= 7).astype(int)
data.quality = high_quality

# COMMAND ----------

# MAGIC %md Box plots are useful in noticing correlations between features and a binary label.

# COMMAND ----------

import matplotlib.pyplot as plt

dims = (3, 4)

f, axes = plt.subplots(dims[0], dims[1], figsize=(25, 15))
axis_i, axis_j = 0, 0
for col in data.columns:
  if col == 'is_red' or col == 'quality':
    continue # Box plots cannot be used on indicator variables
  sns.boxplot(x=high_quality, y=data[col], ax=axes[axis_i, axis_j])
  axis_j += 1
  if axis_j == dims[1]:
    axis_i += 1
    axis_j = 0

# COMMAND ----------

# MAGIC %md In the above box plots, a few variables stand out as good univariate predictors of quality. 
# MAGIC 
# MAGIC - In the alcohol box plot, the median alcohol content of high quality wines is greater than even the 75th quantile of low quality wines. High alcohol content is correlated with quality.
# MAGIC - In the density box plot, low quality wines have a greater density than high quality wines. Density is inversely correlated with quality.

# COMMAND ----------

# MAGIC %md ## Preprocessing Data
# MAGIC Prior to training a model, check for missing values and split the data into training and validation sets.

# COMMAND ----------

data.isna().any()

# COMMAND ----------

# MAGIC %md There are no missing values.

# COMMAND ----------

from sklearn.model_selection import train_test_split

train, test = train_test_split(data, random_state=123)
X_train = train.drop(["quality"], axis=1)
X_test = test.drop(["quality"], axis=1)
y_train = train.quality
y_test = test.quality

# COMMAND ----------

# MAGIC %md ## Setup MLFlow backend to Azure Machine Learning

# COMMAND ----------

import azureml
from azureml.core import Workspace

workspace_name = "mlops-demo"
workspace_location="westeurope"
resource_group = "mlops-rg"
subscription_id = "6ee947fa-0d77-4915-bf68-4a83a8bec2a4"

workspace = Workspace(subscription_id, resource_group, workspace_name)

ws_details = workspace.get_details()
print(f'Logged into {ws_details["name"]}')

# COMMAND ----------

# ws_details = workspace.get_details()
# print(f'Logged into {ws_details["name"]}')

# COMMAND ----------

import mlflow

print(f"original MLFlow tracing url: {mlflow.get_tracking_uri()}")
# change to AML
mlflow.set_tracking_uri(workspace.get_mlflow_tracking_uri())
print(f"new MLFlow tracing url: {mlflow.get_tracking_uri()}")

# COMMAND ----------

experiment_name = 'experiment_with_mlflow_from_databricks'
mlflow.set_experiment(experiment_name)

# COMMAND ----------

# MAGIC %md ## Building a Baseline Model
# MAGIC 
# MAGIC This task seems well suited to a random forest classifier, since the output is binary and there may be interactions between multiple variables.
# MAGIC 
# MAGIC The following code builds a simple classifier using scikit-learn. It uses MLflow to keep track of the model accuracy, and to save the model for later use.

# COMMAND ----------

import mlflow
import mlflow.pyfunc
import mlflow.sklearn
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score

# The predict method of sklearn's RandomForestClassifier returns a binary classification (0 or 1). 
# The following code creates a wrapper function, SklearnModelWrapper, that uses 
# the predict_proba method to return the probability that the observation belongs to each class. 

class SklearnModelWrapper(mlflow.pyfunc.PythonModel):
  def __init__(self, model):
    self.model = model
    
  def predict(self, context, model_input):
    return self.model.predict_proba(model_input)[:,1]

# mlflow.start_run creates a new MLflow run to track the performance of this model. 
# Within the context, you call mlflow.log_param to keep track of the parameters used, and
# mlflow.log_metric to record metrics like accuracy.
with mlflow.start_run(run_name='untuned_random_forest'):
  n_estimators = 10
  model = RandomForestClassifier(n_estimators=n_estimators, random_state=np.random.RandomState(123))
  model.fit(X_train, y_train)

  # predict_proba returns [prob_negative, prob_positive], so slice the output with [:, 1]
  predictions_test = model.predict_proba(X_test)[:,1]
  auc_score = roc_auc_score(y_test, predictions_test)
  mlflow.log_param('n_estimators', n_estimators)
  # Use the area under the ROC curve as a metric.
  mlflow.log_metric('auc', auc_score)
  wrappedModel = SklearnModelWrapper(model)
#   mlflow.pyfunc.log_model("random_forest_model", python_model=wrappedModel)
  mlflow.sklearn.log_model(model, "random_forest_model")

# COMMAND ----------

from azureml.core import Experiment, Run
experiment = Experiment(workspace, experiment_name)

# COMMAND ----------

import os

last = True
run_id = None
for run in experiment.get_runs():
  print(f"Run ID: {run._run_id}, Run Number: {run._run_number}")
  if (last):
    run_id = run._run_id
    last = False
  

print()
print(f"Last Run ID: {run_id}")

# register last run_id as environment variable so it can be used in %sh commands
os.environ['RUN_ID_LAST']=run_id
# print(os.getenv('RUN_ID_LAST'))

# COMMAND ----------

import mlflow.sklearn
# run_id = "434c0e56-307a-4e40-a170-343530c77386"
run = Run(experiment, run_id = run_id)
run.get_file_names()

# COMMAND ----------

run.download_files(output_directory=f"/tmp/{run_id}")

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l /tmp/$RUN_ID_LAST/random_forest_model

# COMMAND ----------

model_uri_aml = f"/tmp/{run_id}/random_forest_model"
model = mlflow.sklearn.load_model(model_uri_aml)

# COMMAND ----------

model

# COMMAND ----------

# MAGIC %md Examine the learned feature importances output by the model as a sanity-check.

# COMMAND ----------

feature_importances = pd.DataFrame(model.feature_importances_, index=X_train.columns.tolist(), columns=['importance'])
feature_importances.sort_values('importance', ascending=False)

# COMMAND ----------

# MAGIC %md As illustrated by the boxplots shown previously, both alcohol and density are important in predicting quality.

# COMMAND ----------

# MAGIC %md You logged the Area Under the ROC Curve (AUC) to MLflow. Click **Runs** at the upper right to display the Runs sidebar. 
# MAGIC 
# MAGIC The model achieved an AUC of 0.89. 
# MAGIC 
# MAGIC A random classifier would have an AUC of 0.5, and higher AUC values are better. For more information, see [Receiver Operating Characteristic Curve](https://en.wikipedia.org/wiki/Receiver_operating_characteristic#Area_under_the_curve).

# COMMAND ----------

# MAGIC %md ## Deploy model via Azure ML

# COMMAND ----------

import mlflow.azureml
# from azureml.core import Workspace
# from azureml.core.webservice import AciWebservice, Webservice

# Build an Azure ML Container Image for an MLflow model
azure_image, azure_model = mlflow.azureml.build_image(model_uri=model_uri_aml,
                                                      workspace=workspace,
                                                      synchronous=True)

# If your image build failed, you can access build logs at the following URI:
print("Access the following URI for build logs: {}".format(azure_image.image_build_log_uri))

# # Deploy the image to Azure Container Instances (ACI) for real-time serving
# webservice_deployment_config = AciWebservice.deploy_configuration()
# webservice = Webservice.deploy_from_image(
#                     image=azure_image, workspace=azure_workspace, name="<deployment-name>")
# webservice.wait_for_deployment()

# COMMAND ----------

from azureml.core.webservice import AciWebservice, Webservice

# Deploy the image to Azure Container Instances (ACI) for real-time serving
webservice_deployment_config = AciWebservice.deploy_configuration()
webservice = Webservice.deploy_from_image(
                    image=azure_image, workspace=workspace, name="mlflow-wine-quality")
webservice.wait_for_deployment()

# COMMAND ----------

train[train["quality"]==1].drop(["quality"], axis=1)[0:10].values

# COMMAND ----------

import json

test_sample = json.dumps({'data': [  
  [7.5000e+00, 1.8000e-01, 3.1000e-01, 1.1700e+01, 5.1000e-02,
        2.4000e+01, 9.4000e+01, 9.9700e-01, 3.1900e+00, 4.4000e-01,
        9.5000e+00, 0.0000e+00],
       [8.0000e+00, 4.0000e-01, 3.3000e-01, 7.7000e+00, 3.4000e-02,
        2.7000e+01, 9.8000e+01, 9.9350e-01, 3.1800e+00, 4.1000e-01,
        1.2200e+01, 0.0000e+00],
       [7.3000e+00, 1.3000e-01, 3.1000e-01, 2.3000e+00, 5.4000e-02,
        2.2000e+01, 1.0400e+02, 9.9240e-01, 3.2400e+00, 9.2000e-01,
        1.1500e+01, 0.0000e+00],
       [6.4000e+00, 2.3000e-01, 3.2000e-01, 1.9000e+00, 3.8000e-02,
        4.0000e+01, 1.1800e+02, 9.9074e-01, 3.3200e+00, 5.3000e-01,
        1.1800e+01, 0.0000e+00],
       [6.8000e+00, 2.6000e-01, 4.2000e-01, 1.7000e+00, 4.9000e-02,
        4.1000e+01, 1.2200e+02, 9.9300e-01, 3.4700e+00, 4.8000e-01,
        1.0500e+01, 0.0000e+00]
]})
test_sample = bytes(test_sample,encoding = 'utf8')

output = webservice.run(input_data = test_sample)

print(output)


# COMMAND ----------

# construct raw HTTP request and send to the service
# %%time

# import requests

# import json

# test_sample = json.dumps({'data': [
#     [1,2,3,4,5,6,7,8,9,10], 
#     [10,9,8,7,6,5,4,3,2,1]
# ]})
# test_sample = bytes(test_sample,encoding = 'utf8')

# # If (key) auth is enabled, don't forget to add key to the HTTP header.
# headers = {'Content-Type':'application/json', 'Authorization': 'Bearer ' + key1}

# # If token auth is enabled, don't forget to add token to the HTTP header.
# headers = {'Content-Type':'application/json', 'Authorization': 'Bearer ' + access_token}

# resp = requests.post(aks_service.scoring_uri, test_sample, headers=headers)


# print("prediction:", resp.text)

# COMMAND ----------

# this output copy to Postman (AML - MLFlow - Deploy - test) 
test_sample

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md #### Registering the model in the MLflow Model Registry
# MAGIC 
# MAGIC By registering this model in the Model Registry, you can easily reference the model from anywhere within Databricks.
# MAGIC 
# MAGIC The following section shows how to do this programmatically, but you can also register a model using the UI by following the steps in [Register a model in the Model Registry
# MAGIC ](https://docs.microsoft.com/azure/databricks/applications/mlflow/model-registry.html#register-a-model-in-the-model-registry).

# COMMAND ----------

run_id = mlflow.search_runs(filter_string='tags.mlflow.runName = "untuned_random_forest"').iloc[0].run_id
print(f"runid is: {run_id}")

# COMMAND ----------

model_version = mlflow.register_model(f"runs:/{run_id}/random_forest_model", "wine_quality")

# COMMAND ----------

model_version = mlflow.register_model(f"/tmp/{run_id}/random_forest_model", "wine_quality")

# COMMAND ----------

# MAGIC %md You should now see the wine-quality model in the Models page. To display the Models page, click the Models icon in the left sidebar. 
# MAGIC 
# MAGIC Next, transition this model to production and load it into this notebook from the model registry.

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient()
client.transition_model_version_stage(
  name="wine_quality",
  version=model_version.version,
  stage="Production"
)

# COMMAND ----------

# MAGIC %md The Models page now shows the model version in stage "Production".
# MAGIC 
# MAGIC You can now refer to the model using the path "models:/wine-quality/production".

# COMMAND ----------

model = mlflow.pyfunc.load_model("models:/wine_quality/production")

# Sanity-check: This should match the AUC logged by MLflow
print(f'AUC: {roc_auc_score(y_test, model.predict(X_test))}')

# COMMAND ----------

# MAGIC %md ##Experimenting with a new model
# MAGIC 
# MAGIC The random forest model performed well even without hyperparameter tuning.
# MAGIC 
# MAGIC The following code uses the xgboost library to train a more accurate model. It runs a parallel hyperparameter sweep to train multiple
# MAGIC models in parallel, using Hyperopt and SparkTrials. As before, the code tracks the performance of each parameter configuration with MLflow.

# COMMAND ----------

from hyperopt import fmin, tpe, hp, SparkTrials, Trials, STATUS_OK
from hyperopt.pyll import scope
from math import exp
import mlflow.xgboost
import numpy as np
import xgboost as xgb

search_space = {
  'max_depth': scope.int(hp.quniform('max_depth', 4, 100, 1)),
  'learning_rate': hp.loguniform('learning_rate', -3, 0),
  'reg_alpha': hp.loguniform('reg_alpha', -5, -1),
  'reg_lambda': hp.loguniform('reg_lambda', -6, -1),
  'min_child_weight': hp.loguniform('min_child_weight', -1, 3),
  'objective': 'binary:logistic',
  'seed': 123, # Set a seed for deterministic training
}

def train_model(params):
  # With MLflow autologging, hyperparameters and the trained model are automatically logged to MLflow.
  mlflow.xgboost.autolog()
  with mlflow.start_run(nested=True):
    train = xgb.DMatrix(data=X_train, label=y_train)
    test = xgb.DMatrix(data=X_test, label=y_test)
    # Pass in the test set so xgb can track an evaluation metric. XGBoost terminates training when the evaluation metric
    # is no longer improving.
    booster = xgb.train(params=params, dtrain=train, num_boost_round=1000,\
                        evals=[(test, "test")], early_stopping_rounds=50)
    predictions_test = booster.predict(test)
    auc_score = roc_auc_score(y_test, predictions_test)
    mlflow.log_metric('auc', auc_score)

    # Set the loss to -1*auc_score so fmin maximizes the auc_score
    return {'status': STATUS_OK, 'loss': -1*auc_score, 'booster': booster.attributes()}

# Greater parallelism will lead to speedups, but a less optimal hyperparameter sweep. 
# A reasonable value for parallelism is the square root of max_evals.
spark_trials = SparkTrials(parallelism=10)

# Run fmin within an MLflow run context so that each hyperparameter configuration is logged as a child run of a parent
# run called "xgboost_models" .
with mlflow.start_run(run_name='xgboost_models'):
  best_params = fmin(
    fn=train_model, 
    space=search_space, 
    algo=tpe.suggest, 
    max_evals=96,
    trials=spark_trials, 
    rstate=np.random.RandomState(123)
  )

# COMMAND ----------

# MAGIC %md  #### Use MLflow to view the results
# MAGIC Open up the Runs sidebar to see the MLflow runs. Click on Date next to the down arrow to display a menu, and select 'auc' to display the runs sorted by the auc metric. The highest auc value is 0.91. You beat the baseline!
# MAGIC 
# MAGIC MLflow tracks the parameters and performance metrics of each run. Click the External Link icon <img src="https://docs.microsoft.com/azure/databricks/_static/images/external-link.png"/> at the top of the Runs Sidebar to navigate to the MLflow Runs Table.

# COMMAND ----------

# MAGIC %md Now investigate how the hyperparameter choice correlates with AUC. Click the "+" icon to expand the parent run, then select all runs except the parent, and click "Compare". Select the Parallel Coordinates Plot.
# MAGIC 
# MAGIC The Parallel Coordinates Plot is useful in understanding the impact of parameters on a metric. You can drag the pink slider bar at the upper right corner of the plot to highlight a subset of AUC values and the corresponding parameter values. The plot below highlights the highest AUC values:
# MAGIC 
# MAGIC <img src="https://docs.microsoft.com/azure/databricks/_static/images/mlflow/end-to-end-example/parallel-coordinates-plot.png"/>
# MAGIC 
# MAGIC Notice that all of the top performing runs have a low value for reg_lambda and learning_rate. 
# MAGIC 
# MAGIC You could run another hyperparameter sweep to explore even lower values for these parameters. For simplicity, that step is not included in this example.

# COMMAND ----------

# MAGIC %md 
# MAGIC You used MLflow to log the model produced by each hyperparameter configuration. The following code finds the best performing run and saves the model to the model registry.

# COMMAND ----------

best_run = mlflow.search_runs(order_by=['metrics.auc DESC']).iloc[0]
print(f'AUC of Best Run: {best_run["metrics.auc"]}')

# COMMAND ----------

# MAGIC %md #### Updating the production wine_quality model in the MLflow Model Registry
# MAGIC 
# MAGIC Earlier, you saved the baseline model to the Model Registry under "wine_quality". Now that you have a created a more accurate model, update wine_quality.

# COMMAND ----------

new_model_version = mlflow.register_model(f"runs:/{best_run.run_id}/model", "wine_quality")

# COMMAND ----------

# MAGIC %md Click **Models** in the left sidebar to see that the wine_quality model now has two versions. 
# MAGIC 
# MAGIC The following code promotes the new version to production.

# COMMAND ----------

# Archive the old model version
client.transition_model_version_stage(
  name="wine_quality",
  version=model_version.version,
  stage="Archived",
)

# Promote the new model version to Production
client.transition_model_version_stage(
  name="wine_quality",
  version=new_model_version.version,
  stage="Production",
)

# COMMAND ----------

# MAGIC %md Clients that call load_model now receive the new model.

# COMMAND ----------

# This code is the same as the last block of "Building a Baseline Model". No change is required for clients to get the new model!
model = mlflow.pyfunc.load_model("models:/wine_quality/production")
print(f'AUC: {roc_auc_score(y_test, model.predict(X_test))}')

# COMMAND ----------

# MAGIC %md ##Batch Inference
# MAGIC 
# MAGIC There are many scenarios where you might want to evaluate a model on a corpus of new data. For example, you may have a fresh batch of data, or may need to compare the performance of two models on the same corpus of data.
# MAGIC 
# MAGIC The following code evaluates the model on data stored in a Delta table, using Spark to run the computation in parallel.

# COMMAND ----------

# To simulate a new corpus of data, save the existing X_train data to a Delta table. 
# In the real world, this would be a new batch of data.
spark_df = spark.createDataFrame(X_train)
# Replace mimarusa@microsoft.com with your username before running this cell.
table_path = "dbfs:/mimarusa@microsoft.com/delta/wine_data"
# Delete the contents of this path in case this cell has already been run
dbutils.fs.rm(table_path, True)
spark_df.write.format("delta").save(table_path)

# COMMAND ----------

# MAGIC %md Load the model into a Spark UDF, so it can be applied to the Delta table.

# COMMAND ----------

import mlflow.pyfunc

apply_model_udf = mlflow.pyfunc.spark_udf(spark, "models:/wine_quality/production")

# COMMAND ----------

# Read the "new data" from Delta
new_data = spark.read.format("delta").load(table_path)

# COMMAND ----------

display(new_data)

# COMMAND ----------

from pyspark.sql.functions import struct

# Apply the model to the new data
udf_inputs = struct(*(X_train.columns.tolist()))

new_data = new_data.withColumn(
  "prediction",
  apply_model_udf(udf_inputs)
)

# COMMAND ----------

# Each row now has an associated prediction. Note that the xgboost function does not output probabilities by default, so the predictions are not limited to the range [0, 1].
display(new_data)