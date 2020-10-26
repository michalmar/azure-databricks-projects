# Databricks notebook source
candidates <- c( Sys.getenv("R_PROFILE"),
                 file.path(Sys.getenv("R_HOME"), "etc", "Rprofile.site"),
                 Sys.getenv("R_PROFILE_USER"),
                 file.path(getwd(), ".Rprofile") )

Filter(file.exists, candidates)

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /usr/lib/R/etc/Rprofile.site

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls -l /dbfs/

# COMMAND ----------

save.image(file = "/dbfs/my_work_space.RData")

# load("/dbfs/my_work_space.RData")


# COMMAND ----------

load("/dbfs/my_work_space.RData")

# COMMAND ----------

my_var

# COMMAND ----------



# COMMAND ----------

# MAGIC %md # In RStudio

# COMMAND ----------

candidates <- c( Sys.getenv("R_PROFILE"),
                 file.path(Sys.getenv("R_HOME"), "etc", "Rprofile.site"),
                 Sys.getenv("R_PROFILE_USER"),
                 file.path(getwd(), ".Rprofile") )

Filter(file.exists, candidates)


my_var = "123"

save.image(file = "/dbfs/my_work_space.RData")

load("/dbfs/my_work_space.RData")
