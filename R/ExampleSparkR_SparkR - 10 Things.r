# Databricks notebook source
# MAGIC %md
# MAGIC ### 10 things I wish someone had told me before I started using Apache SparkR
# MAGIC 
# MAGIC Link: https://databricks.com/blog/2016/12/28/10-things-i-wish-i-knew-before-using-apache-sparkr.html
# MAGIC 
# MAGIC *Written by Neil Dewar, a senior data science manager at a global asset management firm*
# MAGIC 
# MAGIC I'm an R user, with a reasonable level of skills, but not a super-user.  Certainly not an object oriented programmer, and no experience of distributed computing. As my team starts to explore options for distributed processing of big data, I took the task to evaluate SparkR.  Fortunately, I've been able to combine this work with some academic studies at Syracuse University's I-School.  Over the last couple of months I have been researching ways of deploying Spark R, what tasks SparkR can be used for, and how to train R users to adopt SparkR.
# MAGIC 
# MAGIC Databricks and Apache Spark provide some great materials to learn  SparkR --  good factual material.  But I felt I was learning SparkR really slowly, and making some big mistakes along the way.  I looked for help on Stack Overflow, but found little by way of community with answers (with one exception - @Zero323 ... please keep answering newbies' questions - the community appreciates you!).  I eventually figured out that what's missing is the contextual advice for people who already know R, to help them understand what's different about SparkR and how to adapt your thinking to make best use of it.
# MAGIC 
# MAGIC That's the purpose of this notebook - to document the "aha!" moments in a journey from R to SparkR.  I hope my hard-earned discovery helps you get there faster!  I've peppered the narrative with code examples that will hopefully clarify the points.

# COMMAND ----------

# MAGIC %md ## For the full SparkR documentation, see the Spark site documentation at:
# MAGIC https://spark.apache.org/docs/latest/sparkr.html

# COMMAND ----------

# MAGIC %md
# MAGIC #### Some changes needed due to changes in Databricks Notebooks interaction with Spark.  Bryan Cafferky

# COMMAND ----------

# MAGIC %md
# MAGIC #### #1. Apache Spark Building Blocks
# MAGIC If you read a great deal, you can reverse-engineer the Spark documentation into a simple picture that helps you understand what's going on.  
# MAGIC 
# MAGIC I found Nitin Bandugula's post "The 5-Minute Guide to Spark" on Mapr.com useful in building up my understanding: <https://www.mapr.com/blog/5-minute-guide-understanding-significance-apache-spark>
# MAGIC 
# MAGIC ![Spark Core Stack](https://www.mapr.com/sites/default/files/blogimages/Spark-core-stack-DB.jpg)
# MAGIC 
# MAGIC From that diagram I still struggled with how some of the pieces work together, so I redrafted the diagram at the end of Nitin's post in a way that helped me explain the different APIs:
# MAGIC 
# MAGIC ![Spark APIs and Data Abstractions](https://www.dropbox.com/s/jhw7hvq3ikfrtsn/Spark%20architecture%20diagram.png?raw=1)
# MAGIC 
# MAGIC What I'm trying to convey with this diagram is:  
# MAGIC - Spark core provides all the core functions used by all APIs.  
# MAGIC - Spark core also contains the RDD data abstraction
# MAGIC - MLLib, GraphX, Spark Streaming and Spark SQL APIs are all based on Spark Core
# MAGIC - Spark SQL contains the DataFrame data abstraction
# MAGIC - The ML API uses DataFrames
# MAGIC - The SparkR language API uses DataFrames only, and so it is built on top of Spark SQL.  
# MAGIC    + Because of this, SparkR cannot directly access the RDD-based MLLib, GraphX and Spark Streaming.  
# MAGIC    + Some SparkR functions have been built that are able to access ML as both use DataFrames, but it's not automatic - ML functions only become available in SparkR as quickly as someone writes the SparkR code to access the ML API.
# MAGIC - The other language APIs use both DataFrames and RDDs, and therefore can access functionality in all APIs.

# COMMAND ----------

# MAGIC %md
# MAGIC #### #2.  Spark Context, SQL Context, and Spark Session
# MAGIC 
# MAGIC You interact with Spark Core through Spark Context.  Without it there is no interaction with Spark APIs.
# MAGIC 
# MAGIC Many texts, blogs, and even the Spark website encourage you to create Spark Context using:<br>
# MAGIC    `sc <- sparkR.init() # initiate Spark context`  
# MAGIC 
# MAGIC If you are working on a non-hosted Spark installation, and you have not previously initiated Spark Context, then that's right.  But if you are working on Databricks `sc` has already been created for you.  The same thing may apply in other notebook apps - the kernel that connects the notebook to Spark is likely to have created `sc` for you.  
# MAGIC 
# MAGIC If you are working on a local installation, and submitting jobs from the command line ... do you really know if `sc` exists or if you are inadvertently trying to create a new Spark Context.  Why's this important?  In the words of Highlander ... "There can be only one!". Multiple contexts create problems.
# MAGIC 
# MAGIC On platforms other than Databricks you can validate the Spark context with `print(sc)`.
# MAGIC 
# MAGIC The same thing applies for sc's offspring sqlContext.  To interact with Spark DataFrames (which is the only Spark data object in SparkR), you need sqlContext. Again, our friends at Databricks do that for you.  Always, before you create one, check it's there:

# COMMAND ----------

# DBTITLE 1,You Now Need to Import the SparkR package...
# print(sqlContext)
library(SparkR)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### For masked functions, you can still reference the underlying open source package using the packagename::function format such as 
# MAGIC #### base::myfunction

# COMMAND ----------

# MAGIC %md
# MAGIC One final point on using Spark outside of Databricks.  Databricks does a great job of cleaning things up when you stop working.  If you are using Spark elsewhere (eg in R Studio) you need to remember to shut things down when you are done, or Spark will keep on filling up your console with messages.  You can do this with:  
# MAGIC `# DO NOT DO THIS IN DATABRICKS`  
# MAGIC `sparkR.stop()`  
# MAGIC `detach("package:SparkR", unload=TRUE)`  
# MAGIC 
# MAGIC Key Takeaways:
# MAGIC - Don't initiate sc and sqlContext in Databricks - they are already there
# MAGIC - Always check if sc or sqlContext are present, particularly before initiating them
# MAGIC - If you are working on SparkR outside of Databricks, close down Spark when you are done

# COMMAND ----------

# DBTITLE 1,Prefix rdf = R Dataframe and sdf = Spark Dataframe
rdf <- data.frame(c(1,2,3,4))
colnames(rdf) <- c("myCol")
sdf <- createDataFrame(rdf)  
withColumn(sdf, "newCol", sdf$myCol * 2.0)
str(rdf)
str(sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC #### #3. A DataFrame or a data.frame?
# MAGIC 
# MAGIC When I see SparkR questions on Stackoverflow, one of the most common causes that underly questions is a confusion over data objects.  On reflection, this was the same for me too ... the first 3 weeks of learning SparkR was spent looking at confusing error messages because I had done something wrong.  Most of that was due to data frame confusion.
# MAGIC 
# MAGIC The SparkR API presents a full R interface, supplemented with the {SparkR} package.  As an experienced R user, you will be familiar with the R data.frame object.  Here's the critical point - SparkR has its own DataFrame object, which is not the same thing as an R data.frame.  You can convert between them easily (sometimes too easily), but you  must respect which is which.

# COMMAND ----------

# hopefully all R users are familiar with mtcars, or should I have used iris?
str(mtcars)

# Note the first line of the result "data.frame" - that's an R data.frame!

# COMMAND ----------

# DBTITLE 1,Spark functions like createDataFrame return Spark Data Frames...
sdfCars <- createDataFrame(mtcars)
str(sdfCars)
# Note the first line of the result "DataFrame" - that a Spark DataFrame!

# COMMAND ----------

# DBTITLE 1,Bring the data back to a local R Dataframe...
rdfGuzzler <- collect(filter(sdfCars, sdfCars$mpg > 25))
str(rdfGuzzler)
# note - the collect() function creates an R data.frame from a Spark DataFrame!

# COMMAND ----------

# MAGIC %md
# MAGIC One very useful difference to remember is what happens when you run a function that returns a data.frame / DataFrame, and you do not return it into a variable.
# MAGIC 
# MAGIC If you return an R data.frame, it returns the contents of the data.frame.

# COMMAND ----------

mtcars

# COMMAND ----------

# MAGIC %md 
# MAGIC But if you return a SparkR DataFrame you only get the structure back.  You have to use another command to display the contents.

# COMMAND ----------

sdfCars

# COMMAND ----------

# DBTITLE 1,display() only works in Databricks, not open source Spark
# display() only works in Databricks, not open source Spark
display(sdfCars)

# COMMAND ----------

# MAGIC %md
# MAGIC Probably the most useful thing I learned along the way is to pedantically name R data.frames with the prefix 'rdf' and SparkR DataFrames with the prefix 'sdf'.  It has saved me a lot of pain.
# MAGIC 
# MAGIC Key Takeaways:
# MAGIC - A Spark DataFrame is not the same as an R data.frame  
# MAGIC - DataFrame is the only data abstraction supported by SparkR  
# MAGIC - R functions work on R data abstractions, SparkR functions work on Spark Dataframes.  
# MAGIC - Carefully and consistently name your data.frames and DataFrames with an appropriate prefix.  
# MAGIC - If you get an undecipherable error message, start by checking on the data object type and whether your command is SparkR or R  

# COMMAND ----------

# MAGIC %md **Update for Spark 2.0**
# MAGIC 
# MAGIC SparkR DataFrames are named `SparkDataFrame` in Spark 2.0

# COMMAND ----------

# MAGIC %md
# MAGIC #### #4.  Distributed Processing 101
# MAGIC 
# MAGIC Having a basic understanding of what's going on in distributed processing is critically important.  Unfortunately, you have to open the hood to understand what's going on, but I don't believe you need to dismantle the engine.  I've tried to describe things in a high-level way here without getting into all the details.  This is a Data Scientist's view of the world, not a Computer Scientist's view - no offense intended if this trivializes someone's life work.
# MAGIC 
# MAGIC **Traditional R**
# MAGIC 
# MAGIC Traditional R was designed to work on a single computer.  The underlying processing is performed in a single "thread" on a single processor core of that computer, though there are R packages which support multithreaded processing.
# MAGIC 
# MAGIC **SparkR**
# MAGIC 
# MAGIC Spark is designed to work across multiple computers, with multiple threads on each computer.  To understand that, it's maybe helpful to think about how distributed processing works.
# MAGIC 
# MAGIC _Clusters_
# MAGIC - A cluster is a collection of servers, all connected into the same chassis in the data center.  The multiple computers have high-speed networking connecting them through the chassis backplane.  Each node in the cluster is a computer with multiple cores, and a lot of memory.  For example, a small real world cluster might consist of 12 nodes (each is a Linux server), each node has 24 cores, each node has 36GB of memory.  So the total cluster has 288 cores, and 432GB of memory.  
# MAGIC - Spark is installed on each computer in the cluster.  
# MAGIC - The term "cluster" is used in two ways.  The physical network of computers is called a cluster.  The logical collection of Spark node installations is called a Spark cluster.
# MAGIC - One computer in the Spark cluster is appointed as Spark master.  All Spark jobs submitted by users are directed to the Spark master.  The Spark master is responsible for distributing both the data and processing tasks to the "worker nodes" in the cluster.
# MAGIC 
# MAGIC _Spark DataFrames and jobs_
# MAGIC - Any DataFrame (under the hood, a DataFrame is implemented as RDDs) is physically sliced up between nodes in the cluster, but logically is presented to the user as a single DataFrame.  
# MAGIC - At this point, its worth reminding ourselves that RDD stands for Resilient, Distributed, Data Set.  The distributed part should now be clear.  The resilience comes from Spark's approach to distributing the data -- if one worker node in the cluster crashes, its data can be recovered on other nodes, and the Spark master can instruct these other nodes to pick up processing in its place.
# MAGIC - When a Spark job is executed, the Spark master builds an execution plan for how to divide the work up between the different worker nodes.  Each node that is involved in the processing will load its slice of the RDD into its local memory.  Each node will perform a subset of the processing.  For example, if I run the code: `sdfNew <- sample( x=sdfOld, withReplacement = FALSE , fraction = 0.1 , seed=42)`  ... the Spark master instructs each node to sample its slice of the data in the physically distributed sdfOld.  Each node samples its slice, then places the result into a new "RDD slice" in its memory.  All those result RDD slices distributed across the cluster collectively form the logical DataFrame sdfNew.  When all worker nodes have told the master that they have completed processing, the master presents the user with the new DataFrame as a logical abstraction of the distributed results.
# MAGIC - The data is only physically reassembled into a single data object on a single computer when you use a command like collect() to export the data from the Spark DataFrame into an R data.frame.  When this happens, because R only works on a single computer, the data is "collected" together onto a single computer in the cluster.  Any commands run using traditional R on the collected data.frame will be executed only on that single computer.
# MAGIC 
# MAGIC That's probably enough on distributed processing to get you started with using SparkR ... if you want to dig further then this Spark documentation is a good place to begin: <http://spark.apache.org/docs/latest/cluster-overview.html>  
# MAGIC 
# MAGIC Key Take-aways:  
# MAGIC - The data scientist working in SparkR needs to develop strategies on how to organize their work in a way that gets the benefits of Spark to crunch big data, but gets the benefits of R's wealth of functionality.  What I've come up with so far includes:  
# MAGIC   + Load Big data, directly from file into a SparkR DataFrame.  Do not go via an R data.frame because it's inefficient!
# MAGIC   + Do not try to collect the full Big data set into an R data.frame   
# MAGIC   + Do any processing of the Big data set in Spark  
# MAGIC   + Create smaller data sets with "triple-S" __Sample, Subset, Summary__ in Spark DataFrames, and collect() them for subsequent processing and visualization in R  
# MAGIC - If you are learning SparkR on small data, don't get into the bad habit of jumping backwards and forward between SparkR and R for the convenience of familiar R functions.  They are not available in SparkR because they don't work with big data.  If you don't learn how to do it using SparkR functions, you will hit a wall when you meet your first big data set!

# COMMAND ----------

# MAGIC %md
# MAGIC #### #5. Function Masking  
# MAGIC 
# MAGIC Most R users are used to the idea of function masking.  There are functions in base R.  Someone writes a library for a specific purpose and wants a similarly named function.  When you load the library, the new function masks the old one.  If you need to explicity use the old function you can access it using its library name such as: `base::summary()`.
# MAGIC 
# MAGIC No surprises here, SparkR contains functions that mask the R versions.  I would group the SparkR functions into four classes:  
# MAGIC - SparkR functions that are entirely new, and are unavailable in R for use on R objects.
# MAGIC - SparkR functions that have the same parameters as the R function, and they work fine on both R objects and SparkR objects.  Good examples here are `str()` and `summary()` functions.
# MAGIC - SparkR functions that have the same parameters as the R function, but which only work on SparkR objects.  An example of this type is the `glm()` function.
# MAGIC - SparkR functions that have different parameters to the R function, and only work on SparkR objects.  A good example of this is the `sample()` function.
# MAGIC 
# MAGIC Key Take-aways:
# MAGIC - Know your functions, and which of the four classes they fall into
# MAGIC - Get comfortable with looking up R libraries so that you can over-ride masked functions.  I keep R Studio open as I'm working in Spark and use the search feature to look up R functions.

# COMMAND ----------

# MAGIC %md
# MAGIC #### #6. Specifying Rows
# MAGIC In R, row indexing is permitted.  For example:  
# MAGIC   `df[3,]`  
# MAGIC returns the third row of data.frame called df.  Similarly a group of rows can be specified:  
# MAGIC   `df[c(3,4,6,19),]`  
# MAGIC Conversely in Spark there is no row indexing.  When the data is distributed across multiple nodes, each with a different subset of the data, what constitutes the third record?  That would require the controller to maintain a sort order across all records.  Data can however be specified by query, i.e. by specifying features of the data that identify certain records.  Just as in R we can specify:  
# MAGIC   `df[df$Name=='Joe']`  
# MAGIC In Spark we can also filter a DataFrame using commands like:  
# MAGIC   `filter(df, df$Name=='Joe')`  
# MAGIC 
# MAGIC Key Take-aways:
# MAGIC - Big data's not about specific records
# MAGIC - Sample, Subset and Summary to get smaller data sets
# MAGIC - If you cheat and try to identify rows by filtering for a specific key value - expect it to be painfully slow

# COMMAND ----------

# MAGIC %md
# MAGIC #### #7. Sampling
# MAGIC The structure of the function sample() is different between R and SparkR.  In R the function is:  
# MAGIC   `sample(x, size, replace = FALSE, prob = NULL)`  
# MAGIC Whereas in SparkR the function is  
# MAGIC   `sample(x, withReplacement, fraction, seed)`  
# MAGIC 
# MAGIC The differences in sample() lead us to have to think differently about partitioning a data set for Training and Testing.  The normal approach in R would be to do something like this:  
# MAGIC 
# MAGIC `# This is all in R`  
# MAGIC `set.seed(42)`  
# MAGIC `train_ind <- base::sample(mtcars, size = 10, replace = FALSE)`  
# MAGIC `display(train_ind)`  
# MAGIC `# note: train_ind is a vector of row numbers`  
# MAGIC `#rdfTrain <- mtcars[train_ind, ]`  
# MAGIC `#rdfTest <- mtcars[-train_ind, ]`  
# MAGIC `nrow(rdfTest)`  
# MAGIC 
# MAGIC But with a Spark DataFrame you can't index specific rows, so `mtcars[train_ind,]` doesn't fly.  
# MAGIC 
# MAGIC There are two ways to achieve the data set partition in Spark:

# COMMAND ----------

# METHOD 1: Use randomSplit to portion the data into disjoint subsets.

# Split the data into 2 roughly equal subsets.
splitData <- randomSplit(sdfCars, weights = c(0.5, 0.5), seed = 42)
sdfTrain <- splitData[[1]]
sdfTest <- splitData[[2]]
# note - sdfTrain is a DataFrame, and contains all columns of sdfCars

nrow(sdfTest)/nrow(mtcars) # proportions are approximate!

# COMMAND ----------

# randomSplit can also split into more than 2 subsets.
# If `weights` contains values which do not add up to 1, these proportions are normalized to proper ratios.
splitData <- randomSplit(sdfCars, weights = c(1, 2, 4), seed = 42)
sdfOne <- splitData[[1]]
sdfTwo <- splitData[[2]]
sdfThree <- splitData[[3]]

c(nrow(sdfOne)/nrow(mtcars),
  nrow(sdfTwo)/nrow(mtcars),
  nrow(sdfThree)/nrow(mtcars))

# COMMAND ----------

# METHOD 2: Add a column containing a random number between 0 and 1
sdfCars <- withColumn(sdfCars, "random", rand(42))
sdfTrain <- where(sdfCars, sdfCars$random <= 0.5)
sdfTest  <- where(sdfCars, sdfCars$random > 0.5)

nrow(sdfTest)/nrow(mtcars) # proportions are approximate!

# COMMAND ----------

# DBTITLE 1,Good to know but don't do this at home!
# METHOD 3: The except() function (DON'T USE THIS!)
# This method is inefficient on large data sets since it requires comparing 2 Big datasets.  Use METHOD 1 or 2 instead.

sdfTrain <- sample(sdfCars, withReplacement = FALSE, fraction = 0.5, seed = 42)
# note - sdfTrain is a DataFrame, and contains all columns of sdfCars
sdfTest  <- except(sdfCars, sdfTrain)
# note - sdfTest is a DataFrame, and contains all columns of sdfCars

nrow(sdfTest)/nrow(mtcars) # proportions are approximate!

# COMMAND ----------

# MAGIC %md
# MAGIC Another thought here is BIG Data.  If you have 10 Billion records, do you need test and train data sets that make up the full data set?  Maybe not --- but it depends on how complex or expressive your model is.  A linear regression on 10 features may not require 10 Billion records.  But a random forest with 1000 features may benefit from more data!
# MAGIC 
# MAGIC Key Take-aways:
# MAGIC - Change your approach to sampling on Spark R - it's designed for BIG data!
# MAGIC - If in doubt, refer to #4 or #5

# COMMAND ----------

# MAGIC %md
# MAGIC #### #8.  Machine Learning
# MAGIC 
# MAGIC In the official SparkR documentation <https://spark.apache.org/docs/latest/sparkr.html>, in the overview paragraph it says:
# MAGIC > "SparkR also supports distributed machine learning using MLlib."
# MAGIC 
# MAGIC That's an extremely generous interpretation of the situation.  What would be more accurate would be:  
# MAGIC > "ML functions are exposed to SparkR, and SparkR will support them as and when SparkR functions are written".  
# MAGIC 
# MAGIC Refer back to my diagram in section #1.  SparkR has access to the DataFrame based functions in ML.  BUT ... they only appear in R when someone takes the time out of their busy schedule to write a SparkR function to implement them, and work it through the Apache project to be included in SparkR.  For reference, here is the current trajectory of ML algorithms in SparkR:
# MAGIC * Spark 1.6: only one algorithm ... `glm`
# MAGIC * Spark 2.0: 3 more algorithms ... `naiveBayes`, `survreg` (AFT survival regression), `kmeans`
# MAGIC * Spark 2.1: 9 more algorithms ... `randomForest`, `gbt` (gradient-boosted trees), `als` (Alternating least squares), `logit` (multiclass logistic regression), `gaussianMixture`, `lda` (Latent Dirichlet Allocation), `mlp` (Multilayer perceptron), `kstest` (Kolmogorov-Smirnov test), `isoreg` (Isotonic regression)
# MAGIC 
# MAGIC Another approach you can take is to use SparkR for preparation of big data for modeling, create a sample of the big data that is manageable in R, and `collect()` it into an R data.frame.  You can then do your modeling in R.
# MAGIC 
# MAGIC If that doesn't work for you, and you really need to use ML on big data I can offer two options:  
# MAGIC (a) learn one of the other Spark languages such as Python  
# MAGIC (b) wait for the next Spark release which will contain more SparkR ML functions.
# MAGIC 
# MAGIC Key Take-aways:
# MAGIC - SparkR Machine Learning is a work in progress
# MAGIC - Do you need to execute your ML job in Spark?  Consider sampling and doing ML in R.
# MAGIC - More features are on the way - sign up for Spark product updates
# MAGIC - Motivate an R user to learn Scala and contribute to the project

# COMMAND ----------

# MAGIC %md
# MAGIC #### #9.  Visualization
# MAGIC 
# MAGIC Databricks provides a great `display()` function for viewing Spark DataFrames.  It provides tabular and graphical views of the data:  

# COMMAND ----------

display(sdfCars)

# COMMAND ----------

# MAGIC %md
# MAGIC If you run `display()` on a huge data set, it will only display a subset of the data.
# MAGIC 
# MAGIC To just display a few records (and save Spark from having to collect extra records from across the cluster), you can use the `head()` or `take()` functions.
# MAGIC 
# MAGIC If you want to visualize a billion row data set, maybe it's best to think about other strategies.  
# MAGIC - Can you filter the outliers and visualize them?
# MAGIC - Can you sample and visualize?
# MAGIC - Can you group and aggregate to visualize?  (Some Databricks built-in visualizations will aggregate across the entire dataset.)
# MAGIC 
# MAGIC It's also useful to remember that R's graphical libraries do not work against SparkR DataFrames.  So that's another reason to Sample, Subset or Summary before collecting into an R data.frame for visualization.  That said, I did have a good conversation with the Databricks product management team about integrating plot.ly directly against DataFrames in SparkR.
# MAGIC 
# MAGIC One last thought ... most of the big BI tools like Tableau and Microstrategy have ways of pulling data from Spark.  This presents another valuable way of visualizing your Spark data.  But remember - it's probably not very efficient to try to visualize a billion rows of data by pulling it through a web service to hold in memory on your workstation!
# MAGIC 
# MAGIC Key Take-aways
# MAGIC - It's probably not smart to try to visualize raw big data
# MAGIC - More visualization functionality is available in R than in SparkR.  
# MAGIC - Think about #4 before converting Spark DataFrames to R data.frames: __Sample, Subset, Summary__ so you don't move Big Data into R
# MAGIC - Consider using 3rd party BI tools like Excel, Power BI, or Tableau, and Microstrategy to visualize your not-so-big data

# COMMAND ----------

# DBTITLE 1,Bringing a SQL Table into R...
# MAGIC %sql select * from birthwt_table

# COMMAND ----------

# DBTITLE 1,Using the SparkR sql function we can convert the table into a SparkR data frame...
sdfBirthweight <- sql('select age, lwt, race from birthwt_table')
display(sdfBirthweight)

# COMMAND ----------

# MAGIC %md ### Create a SQL Table from a SparkR Data Frame...

# COMMAND ----------

# DBTITLE 1,Newer Method...
library(SparkR)
createOrReplaceTempView(sdfBirthweight, "birthwightTemp1")

# COMMAND ----------

# MAGIC %sql select * from birthwightTemp1

# COMMAND ----------

# DBTITLE 1,Save a Spark Data Frame as a Table...
# Register earlier df as temp table.  Older method...
registerTempTable(sdfBirthweight, "birthwightTemp2")

# COMMAND ----------

# DBTITLE 1,Let's try our new temporary table...
# MAGIC %sql select * from birthwightTemp2 limit 10

# COMMAND ----------

# MAGIC %md ## Converting from a Spark data frame to a local R Dataframe...

# COMMAND ----------

rdfBirthLocal <- collect(sql('select avg(age), avg(lwt), race from birthwightTemp2 group by race'))
str(rdfBirthLocal)

# COMMAND ----------

rdfBirthLocal

# COMMAND ----------

barplot(rdfBirthLocal$race, col='blue')

# COMMAND ----------

