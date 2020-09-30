-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Analytics and Data Science On All Of Your Data: Demo
-- MAGIC 
-- MAGIC ## Big data challenge #2: BI is limited to a fraction of your total data
-- MAGIC 
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/2 - Enable BI directly on all of your source data.png" width=1000/>

-- COMMAND ----------

-- MAGIC %md **Note:** Run notebook #1 (Build & Manage Your Data Lake With Delta Lake) before running this notebook. Notebook #1 generates the `loans` database that we use throughout this notebook.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### <img src="https://pages.databricks.com/rs/094-YMS-629/images/dbsquare.png" width=30/> Using magic commands to switch between languages in Databricks Notebooks
-- MAGIC 
-- MAGIC Databricks Notebooks feel familiar to Jupyter but possess several enhancements. They make it easy for data analysts, data engineers, and data scientists to use whatever language they are most comfortable with. By simply entering the percentage sign along with the name of a language, you can turn any cell into a cell that runs code in your language of choice.
-- MAGIC 
-- MAGIC <sp>
-- MAGIC - `%sql` - Query your data using SQL commands, or use DDL/DML to define and modify tables
-- MAGIC - `%python` - run Python code
-- MAGIC - `%scala` - run Scala code
-- MAGIC - `%r` - run R code
-- MAGIC   
-- MAGIC #### Other magic commands
-- MAGIC 
-- MAGIC - `%md` - format cell contents as Markdown
-- MAGIC - `%fs` - access DBFS (the Databricks File System)
-- MAGIC - `%sh` - access the shell
-- MAGIC 
-- MAGIC   
-- MAGIC This notebook is a SQL notebook, so we can run commands in SQL without needing to specify the `%sql` magic command, but these commands come in handy when working in Python notebooks or other environments.

-- COMMAND ----------

-- MAGIC %md SQL:

-- COMMAND ----------

USE loans;
SELECT *
FROM bronze_loan_stats
LIMIT 2

-- COMMAND ----------

-- MAGIC %md Scala:

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val myVar = "Hello, World!"

-- COMMAND ----------

-- MAGIC %md R:

-- COMMAND ----------

-- MAGIC %r
-- MAGIC x <- c(2, 3, 4, 5)
-- MAGIC x

-- COMMAND ----------

-- MAGIC %md Python:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table('silver_loan_stats')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Markdown:
-- MAGIC ## Heading level 2
-- MAGIC #### Heading level 4
-- MAGIC 
-- MAGIC Markdown comes in _**really**_ handy sometimes, like when you want to make [links](https://www.databricks.com),
-- MAGIC > or *emphasize* what you're saying with a blockquote,
-- MAGIC 
-- MAGIC or embed an image. <img src="https://pages.databricks.com/rs/094-YMS-629/images/dbsquare.png" width=30/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Databricks file system:

-- COMMAND ----------

-- MAGIC %fs ls

-- COMMAND ----------

-- MAGIC %md Shell (with built-in Conda support!):

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC conda --version
-- MAGIC # or try "conda install matplotlib -y"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Working with databases and tables
-- MAGIC 
-- MAGIC **[Link to documentation](https://docs.databricks.com/data/tables.html#databases-and-tables)**
-- MAGIC 
-- MAGIC In the previous notebook, we stored our Silver table `silver_loan_stats` in a managed Databricks database so it would be easily accessible for our data analysts. Data analysts can find and retrieve databases and tables 2 different ways:
-- MAGIC <sp>
-- MAGIC 1. **Using SQL commands**, to access the data programatically
-- MAGIC 2. **Using the `Data` tab** on the left hand side of the Databricks workspace, to access the data through the visual interface
-- MAGIC 
-- MAGIC  Let's use SQL to find this table by showing the managed databases and tables in Databricks programmatically using SQL as shown below.

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

-- Use the `loans` database, and show the tables within it
USE loans; 
SHOW TABLES IN loans

-- COMMAND ----------

-- Select data from the table
SELECT *
FROM silver_loan_stats
LIMIT 2

-- COMMAND ----------

SELECT *
FROM gold_loan_stats
LIMIT 2

-- COMMAND ----------

-- Get details on the table
DESCRIBE DETAIL silver_loan_stats

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Working with the Data tab
-- MAGIC 
-- MAGIC We just demonstrated how to access managed Databricks databases and tables using SQL, but you can also use the **`Data`** tab on the left hand side of the Databricks workspace to visually find data sets and tables to query, or add new ones (from S3/Azure Blob, DBFS, a file, or otherwise) through the upload interface.
-- MAGIC 
-- MAGIC Clicking into a table, you can see information about it including its schema, column names and data types, and some sample data. When working with Delta Lake tables, clicking the `History` tab brings you to the Delta Lake Transaction Log, allowing you to see any changes to the table over time.
-- MAGIC 
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/data-tab.gif" width=800/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## <img src="https://pages.databricks.com/rs/094-YMS-629/images/dbsquare.png" width=30/> Exploratory data analysis and data visualization using SQL and Datatabricks built-in visualizations
-- MAGIC 
-- MAGIC In Databricks notebooks, when the result of a cell is a DataFrame, you can click the Chart button in the bottom left hand corner of the cell to plot your DataFrame using Databricks' built-in data visualization tool.
-- MAGIC 
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/viz-button.png" width=180/>
-- MAGIC 
-- MAGIC 
-- MAGIC When you click on the button, you will see that it allows you to group and aggregate the data in many different ways. Play around with the visualizations toolbar at the bottom of the cell until you find a way to display the data that you like.
-- MAGIC 
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/Screen Shot 2020-03-05 at 12.24.43 PM.png" width=300/>
-- MAGIC 
-- MAGIC The `Plot Options` button allows you to customize your visualization in greater detail.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To demonstrate, in this next cell, we'll load the `silver_loan_stats` table from the `loans` database that we created in the previous notebook.

-- COMMAND ----------

SELECT *
FROM silver_loan_stats
WHERE int_rate > 6.24
ORDER BY int_rate ASC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Anytime the result of a SQL query or Spark command returns a DataFrame, the result will automatically be displayed in a table that can be easily converted into a visualization using Databricks built-in visualizations. The visualization engine can perform aggregations, counts, and group bys on the back end, so analysts can explore the data, and easily play with different visualizations quickly, without knowing exactly what they are looking for right away.

-- COMMAND ----------

SELECT *
FROM silver_loan_stats

-- COMMAND ----------

-- MAGIC %python
-- MAGIC displayHTML('<iframe width="800" height="450" src="https://www.youtube.com/embed/YMrFnqSGD2s" frameborder="0" allow="autoplay; encrypted-media; picture-in-picture" allowfullscreen></iframe>')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Built-in Databricks visualizations are interactive. Mouseover each bar to view the exact values, click the "false" or "true" group in the legend to isolate each group, or use the tools in the upper right to download the plot, resize it, zoom, or choose several other options.

-- COMMAND ----------

SELECT grade, bad_loan, COUNT(*)
FROM silver_loan_stats
GROUP BY grade, bad_loan
ORDER BY grade

-- COMMAND ----------

SELECT bad_loan, grade, SUM(net)
FROM silver_loan_stats
GROUP BY bad_loan, grade
ORDER BY bad_loan, grade

-- COMMAND ----------

SELECT issue_year, bad_loan, COUNT(*)
FROM silver_loan_stats
GROUP BY issue_year, bad_loan
ORDER BY issue_year, bad_loan

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###<img src="https://pages.databricks.com/rs/094-YMS-629/images/dbsquare.png" width=30/> Combine notebook visualizations into an Databricks integrated custom dashboard

-- COMMAND ----------

-- MAGIC %python
-- MAGIC displayHTML('<iframe width="800" height="450" src="https://www.youtube.com/embed/DQjATxSicmY" frameborder="0" allow="autoplay; encrypted-media; picture-in-picture" allowfullscreen></iframe>')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC [YouTube link](https://www.youtube.com/embed/DQjATxSicmY)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Connecting Delta Lake tables to BI tools for analytics, reporting, and dashboarding
-- MAGIC 
-- MAGIC Delta Lake provides an excellent source of clean, reliable data that can easily be connected to many popular BI, reporting, and dashboarding tools through Databricks. Documentation can be found [here](https://docs.databricks.com/integrations/bi/jdbc-odbc-bi.html). In this demonstration, we'll connect to **Tableau**. Once your data is clean and conforming in a Delta Lake Silver table, follow the steps outlined below to create a [JDBC/ODBC connection](https://docs.databricks.com/integrations/bi/tableau.html#tableau) between your Delta Lake data in Databricks and Tableau Desktop.
-- MAGIC 
-- MAGIC <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/4/4b/Tableau_Logo.png/320px-Tableau_Logo.png" width=320/>
-- MAGIC 
-- MAGIC ### 1. Getting connection credentials from Databricks cluster
-- MAGIC 
-- MAGIC - In Databricks, click **Clusters** in the left menu and select the cluster from the list. Get the **hostname** and **HTTP path** of your Databricks cluster on the Cluster details page, under **Advanced settings** => **JDBC/ODBC**.
-- MAGIC 
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/JDBC%20connection%20GIF.gif" width=800/>
-- MAGIC 
-- MAGIC ### 2. Connecting to Tableau Desktop and selecting Delta Lake table
-- MAGIC - Launch Tableau Desktop, go to the **Connect** => **To a Server** menu, and select the Databricks connector.
-- MAGIC - On the Databricks dialog, enter the **Server Hostname** and **HTTP Path** of the Databricks cluster that you copied down previously.
-- MAGIC - **Sign in** to Databricks using your Databricks username and password. You can also use token as the username and a [personal access token](https://docs.databricks.com/dev-tools/api/latest/authentication.html#token-management) as the password.
-- MAGIC 
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/Tableau Login.gif" width=800/>
-- MAGIC 
-- MAGIC ### 3. Viewing Delta Lake tables in the Tableau Dashboard
-- MAGIC Once the connection to Tableau is set up, create charts and dashboards to interactively analyze your data in Delta Lake.
-- MAGIC 
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/Tableau Dashboard.gif" width=800/>
-- MAGIC 
-- MAGIC You can find an [updated version of this dashboard online here](https://public.tableau.com/profile/brenner.h.#!/vizhome/TableauDashboardwithDataExtracted/Dashboard1?publish=yes).

-- COMMAND ----------

-- MAGIC %python
-- MAGIC displayHTML('<iframe width="750" height="450" src="https://www.youtube.com/embed/2MhvN1ycS3s" vq="hd1080" frameborder="0"; autoplay; encrypted-media; gyroscope" allowfullscreen></iframe>')