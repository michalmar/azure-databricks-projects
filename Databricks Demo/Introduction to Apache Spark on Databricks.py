# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # A Gentle Introduction to Apache Spark
# MAGIC 
# MAGIC To read more about some of the other options that are available to users please see the [Databricks Guide on Clusters](https://docs.cloud.databricks.com/docs/latest/databricks_guide/index.html#02%20Product%20Overview/01%20Clusters.html).

# COMMAND ----------

# MAGIC %md first let's explore the previously mentioned `SparkSession`. We can access it via the `spark` variable. As explained, the Spark Session is the core location for where Apache Spark related information is stored. For Spark 1.X the variables are `sqlContext` and `sc`.
# MAGIC 
# MAGIC Cells can be executed by hitting `shift+enter` while the cell is selected.

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md We can use the Spark Context to access information but we can also use it to parallelize a collection as well. Here we'll parallelize a small python range that will provide a return type of `DataFrame`.

# COMMAND ----------

firstDataFrame = sqlContext.range(1000000)

# The code for 2.X is
# spark.range(1000000)
print (firstDataFrame)

# COMMAND ----------

# MAGIC %md Now one might think that this would actually print out the values of the `DataFrame` that we just parallelized, however that's not quite how Apache Spark works. Spark allows two distinct kinds of operations by the user. There are **transformations** and there are **actions**.
# MAGIC 
# MAGIC ### Transformations
# MAGIC 
# MAGIC Transformations are operations that will not be completed at the time you write and execute the code in a cell - they will only get executed once you have called a **action**. An example of a transformation might be to convert an integer into a float or to filter a set of values.
# MAGIC 
# MAGIC ### Actions
# MAGIC 
# MAGIC Actions are commands that are computed by Spark right at the time of their execution. They consist of running all of the previous transformations in order to get back an actual result. An action is composed of one or more jobs which consists of tasks that will be executed by the workers in parallel where possible
# MAGIC 
# MAGIC Here are some simple examples of transformations and actions. Remember, these **are not all** the transformations and actions - this is just a short sample of them. We'll get to why Apache Spark is designed this way shortly!
# MAGIC 
# MAGIC ![transformations and actions](https://training.databricks.com/databricks_guide/gentle_introduction/trans_and_actions.png)

# COMMAND ----------

# An example of a transformation
# select the ID column values and multiply them by 2
secondDataFrame = firstDataFrame.selectExpr("(id * 2) as value")

# COMMAND ----------

# an example of an action
# take the first 5 values that we have in our firstDataFrame
print (firstDataFrame.take(5))
# take the first 5 values that we have in our secondDataFrame
print (secondDataFrame.take(5))

# COMMAND ----------

display(firstDataFrame.take(5))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Now we've seen that Spark consists of actions and transformations. Let's talk about why that's the case. The reason for this is that it gives a simple way to optimize the entire pipeline of computations as opposed to the individual pieces. This makes it exceptionally fast for certain types of computation because it can perform all relevant computations at once. Technically speaking, Spark `pipelines` this computation which we can see in the image below. This means that certain computations can all be performed at once (like a map and a filter) rather than having to do one operation for all pieces of data then the following operation.
# MAGIC 
# MAGIC ![transformations and actions](https://training.databricks.com/databricks_guide/gentle_introduction/pipeline.png)
# MAGIC 
# MAGIC Apache Spark can also keep results in memory as opposed to other frameworks that immediately write to disk after each task.
# MAGIC 
# MAGIC ## Apache Spark Architecture
# MAGIC 
# MAGIC Before proceeding with our example, let's see an overview of the Apache Spark architecture. As mentioned before, Apache Spark allows you to treat many machines as one machine and this is done via a master-worker type architecture where there is a `driver` or master node in the cluster, accompanied by `worker` nodes. The master sends work to the workers and either instructs them to pull to data from memory or from disk (or from another data source).
# MAGIC 
# MAGIC The diagram below shows an example Apache Spark cluster, basically there exists a Driver node that communicates with executor nodes. Each of these executor nodes have slots which are logically like execution cores. 
# MAGIC 
# MAGIC ![spark-architecture](https://training.databricks.com/databricks_guide/gentle_introduction/videoss_logo.png)
# MAGIC 
# MAGIC The Driver sends Tasks to the empty slots on the Executors when work has to be done:
# MAGIC 
# MAGIC ![spark-architecture](https://training.databricks.com/databricks_guide/gentle_introduction/spark_cluster_tasks.png)
# MAGIC 
# MAGIC Note: In the case of the Community Edition there is no Worker, and the Master, not shown in the figure, executes the entire code.
# MAGIC 
# MAGIC ![spark-architecture](https://docs.databricks.com/_static/images/notebooks/notebook-microcluster-agnostic.png)
# MAGIC 
# MAGIC You can view the details of your Apache Spark application in the Apache Spark web UI.  The web UI is accessible in Databricks by going to "Clusters" and then clicking on the "View Spark UI" link for your cluster, it is also available by clicking at the top left of this notebook where you would select the cluster to attach this notebook to. In this option will be a link to the Apache Spark Web UI.
# MAGIC 
# MAGIC At a high level, every Apache Spark application consists of a driver program that launches various parallel operations on executor Java Virtual Machines (JVMs) running either in a cluster or locally on the same machine. In Databricks, the notebook interface is the driver program.  This driver program contains the main loop for the program and creates distributed datasets on the cluster, then applies operations (transformations & actions) to those datasets.
# MAGIC Driver programs access Apache Spark through a `SparkSession` object regardless of deployment location.
# MAGIC 
# MAGIC ## A Worked Example of Transformations and Actions
# MAGIC 
# MAGIC To illustrate all of these architectural and most relevantly **transformations** and **actions** - let's go through a more thorough example, this time using `DataFrames` and a csv file. 
# MAGIC 
# MAGIC The DataFrame and SparkSQL work almost exactly as we have described above, we're going to build up a plan for how we're going to access the data and then finally execute that plan with an action. We'll see this process in the diagram below. We go through a process of analyzing the query, building up a plan, comparing them and then finally executing it.
# MAGIC 
# MAGIC ![Spark Query Plan](https://training.databricks.com/databricks_guide/gentle_introduction/query-plan-generation.png)
# MAGIC 
# MAGIC While we won't go too deep into the details for how this process works, you can read a lot more about this process on the [Databricks blog](https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html). For those that want a more information about how Apache Spark goes through this process, I would definitely recommend that post!
# MAGIC 
# MAGIC Going forward, we're going to access a set of public datasets that Databricks makes available. Databricks datasets are a small curated group that we've pulled together from across the web. We make these available using the Databricks filesystem. Let's load the popular diamonds dataset in as a spark  `DataFrame`. Now let's go through the dataset that we'll be working with.

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/Rdatasets/data-001/datasets.csv

# COMMAND ----------

dataPath = "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"
diamonds = spark.read.format("com.databricks.spark.csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load(dataPath)
  

# COMMAND ----------

diamonds.cache()
diamonds.count()



# COMMAND ----------

# MAGIC %md Now that we've loaded in the data, we're going to perform computations on it. This provide us a convenient tour of some of the basic functionality and some of the nice features that makes running Spark on Databricks the simplest! In order to be able to perform our computations, we need to understand more about the data. We can do this with the `display` function.

# COMMAND ----------

display(diamonds)

# COMMAND ----------

# MAGIC %md what makes `display` exceptional is the fact that we can very easily create some more sophisticated graphs by clicking the graphing icon that you can see below. Here's a plot that allows us to compare price, color, and cut.

# COMMAND ----------

dataPath = "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"
diamonds = spark.read.format("com.databricks.spark.csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load(dataPath)
  
# inferSchema means we will automatically figure out column types 
# at a cost of reading the data more than once

display(diamonds)

# COMMAND ----------

# MAGIC %md Now that we've explored the data, let's return to understanding **transformations** and **actions**. I'm going to create several transformations and then an action. After that we will inspect exactly what's happening under the hood.
# MAGIC 
# MAGIC These transformations are simple, first we group by two variables, cut and color and then compute the average price. Then we're going to inner join that to the original dataset on the column `color`. Then we'll select the average price as well as the carat from that new dataset.

# COMMAND ----------

df1 = diamonds.groupBy("cut", "color").avg("price") # a simple grouping

df2 = df1\
  .join(diamonds, on='color', how='inner')\
  .select("`avg(price)`", "carat")
# a simple join and selecting some columns

# COMMAND ----------

df2.createOrReplaceTempView("diamonds2")

# COMMAND ----------

# MAGIC %md
# MAGIC # DEMO GRAPH
# MAGIC This...

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from diamonds2

# COMMAND ----------

# MAGIC %md These transformations are now complete in a sense but nothing has happened. As you'll see above we don't get any results back! 
# MAGIC 
# MAGIC The reason for that is these computations are *lazy* in order to build up the entire flow of data from start to finish required by the user. This is a intelligent optimization for two key reasons. Any calculation can be recomputed from the very source data allowing Apache Spark to handle any failures that occur along the way, successfully handle stragglers. Secondly, Apache Spark can optimize computation so that data and computation can be `pipelined` as we mentioned above. Therefore, with each transformation Apache Spark creates a plan for how it will perform this work.
# MAGIC 
# MAGIC To get a sense for what this plan consists of, we can use the `explain` method. Remember that none of our computations have been executed yet, so all this explain method does is tells us the lineage for how to compute this exact dataset.

# COMMAND ----------

df2.explain()

# COMMAND ----------

# MAGIC %md Now explaining the above results is outside of this introductory tutorial, but please feel free to read through it. What you should deduce from this is that Spark has generated a plan for how it hopes to execute the given query. Let's now run an action in order to execute the above plan.

# COMMAND ----------

df2.count()

# COMMAND ----------

# MAGIC %md This will execute the plan that Apache Spark built up previously. Click the little arrow next to where it says `(2) Spark Jobs` after that cell finishes executing and then click the `View` link. This brings up the Apache Spark Web UI right inside of your notebook. This can also be accessed from the cluster attach button at the top of this notebook. In the Spark UI, you should see something that includes a diagram something like this.
# MAGIC 
# MAGIC ![img](https://training.databricks.com/databricks_guide/gentle_introduction/spark-dag-ui-before-2-0.png)
# MAGIC 
# MAGIC or
# MAGIC 
# MAGIC ![img](https://training.databricks.com/databricks_guide/gentle_introduction/spark-dag-ui.png)
# MAGIC 
# MAGIC These are significant visualizations. The top one is using Apache Spark 1.6 while the lower one is using Apache Spark 2.0, we'll be focusing on the 2.0 version. These are Directed Acyclic Graphs (DAG)s of all the computations that have to be performed in order to get to that result. It's easy to see that the second DAG visualization is much cleaner than the one before but both visualizations show us all the steps that Spark has to get our data into the final form. 
# MAGIC 
# MAGIC Again, this DAG is generated because transformations are *lazy* - while generating this series of steps Spark will optimize lots of things along the way and will even generate code to do so. This is one of the core reasons that users should be focusing on using DataFrames and Datasets instead of the legacy RDD API. With DataFrames and Datasets, Apache Spark will work under the hood to optimize the entire query plan and pipeline entire steps together. You'll see instances of `WholeStageCodeGen` as well as `tungsten` in the plans and these are apart of the improvements [in SparkSQL which you can read more about on the Databricks blog.](https://databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html)
# MAGIC 
# MAGIC In this diagram you can see that we start with a CSV all the way on the left side, perform some changes, merge it with another CSV file (that we created from the original DataFrame), then join those together and finally perform some aggregations until we get our final result!

# COMMAND ----------

# MAGIC %md ### Caching
# MAGIC 
# MAGIC One of the significant parts of Apache Spark is its ability to store things in memory during computation. This is a neat trick that you can use as a way to speed up access to commonly queried tables or pieces of data. This is also great for iterative algorithms that work over and over again on the same data. While many see this as a panacea for all speed issues, think of it much more like a tool that you can use. Other important concepts like data partitioning, clustering and bucketing can end up having a much greater effect on the execution of your job than caching however remember - these are all tools in your tool kit!
# MAGIC 
# MAGIC To cache a DataFrame or RDD, simply use the cache method.

# COMMAND ----------

df2.cache()

# COMMAND ----------

# MAGIC %md Caching, like a transformation, is performed lazily. That means that it won't store the data in memory until you call an action on that dataset. 
# MAGIC 
# MAGIC Here's a simple example. We've created our df2 DataFrame which is essentially a logical plan that tells us how to compute that exact DataFrame. We've told Apache Spark to cache that data after we compute it for the first time. So let's call a full scan of the data with a count twice. The first time, this will create the DataFrame, cache it in memory, then return the result. The second time, rather than recomputing that whole DataFrame, it will just hit the version that it has in memory.
# MAGIC 
# MAGIC Let's take a look at how we can discover this.

# COMMAND ----------

df2.count()

# COMMAND ----------

# MAGIC %md However after we've now counted the data. We'll see that the explain ends up being quite different.

# COMMAND ----------

df2.count()

# COMMAND ----------

# MAGIC %md In the above example, we can see that this cuts down on the time needed to generate this data immensely - often by at least an order of magnitude. With much larger and more complex data analysis, the gains that we get from caching can be even greater!

# COMMAND ----------

# MAGIC %md ## Conclusion
# MAGIC 
# MAGIC In this notebook we've covered a ton of material! But you're now well on your way to understanding Spark and Databricks! Now that you've completed this notebook, you should hopefully be more familiar with the core concepts of Spark on Databricks. Be sure to subscribe to our blog to get the latest updates about Apache Spark 2.0 and the next notebooks in this series!