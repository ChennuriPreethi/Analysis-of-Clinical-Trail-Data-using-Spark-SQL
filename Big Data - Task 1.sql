-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Task 1: Analysis of Clinical Trial Data Using Spark SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Introduction
-- MAGIC The objective of this initiative is to analyse a comprehensive dataset of clinical trials registered in the United States, which has been obtained from ClinicalTrials.gov. The dataset contains over 500,000 records and includes valuable fields such as study type, study status, funding type, conditions studied, and various timeline-related attributes. The primary objective of this analysis is to investigate patterns in clinical trial types, identify medical conditions that are frequently investigated, estimate the average trial duration, and analyse temporal trends in diabetes-related trials. Because of its scalability and aptitude for structured data processing on huge datasets, Spark SQL is used throughout the project. This document provides a step-by-step description of the tasks done, the logic behind each SQL query, and insights obtained from the findings. This analysis illustrates a potent pipeline for extracting business and clinical insights from healthcare data by integrating Spark SQL with data transformation techniques.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # This command lists all files and directories in the /FileStore/tables path in Databricks.
-- MAGIC dbutils.fs.ls("/FileStore/tables")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dataset Overview and Tools
-- MAGIC
-- MAGIC The `Clinicaltrial_16012025.csv` dataset was used for this assignment. It encompasses a wide range of study designs, circumstances, and research phases and covers clinical trials carried out and registered in the United States. 
-- MAGIC A distinct clinical trial is represented by each row, which contains details on the trial's name, sponsor, study type, conditions under investigation, start and end dates, and current status. 
-- MAGIC Preprocessing is necessary for precise querying in certain fields, such as `Conditions`, which contain several values concatenated in a single string.
-- MAGIC
-- MAGIC The main framework for data processing was Apache Spark, and the main engine for information querying and aggregation was Spark SQL. Spark SQL enables integration with PySpark routines for preprocessing and offers robust support for SQL-like queries. Spark was the perfect option for carrying out scalable analytics on this huge dataset because of its combination of distributed computing with SQL.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import DateType
-- MAGIC
-- MAGIC # Start Spark Session
-- MAGIC spark = SparkSession.builder.appName("ClinicalTrialAnalysis").getOrCreate()
-- MAGIC
-- MAGIC # Load CSV data
-- MAGIC df = spark.read.csv("/FileStore/tables/Clinicaltrial_16012025.csv", header=True, inferSchema=True)
-- MAGIC
-- MAGIC # Register the DataFrame as a SQL temporary view
-- MAGIC df.createOrReplaceTempView("clinical_trials")
-- MAGIC
-- MAGIC # Display schema
-- MAGIC df.printSchema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Listing all the databases

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Listing the tables in the database

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Displaying top 10 rows from clinical_trail table

-- COMMAND ----------

SELECT * FROM clinical_trials LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Frequency of Clinical Trial Types
-- MAGIC
-- MAGIC ###  List all the clinical trial types (as contained in the Type column of the data) along with their frequency, sorting the results from most to least frequent

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To calculate the frequency of various clinical trial kinds in the dataset. Clinical trial structure is classified by values like 'Expanded Access', 'Observational', and 'Interventional' in the `Study Type` field. 
-- MAGIC
-- MAGIC In order to perform this analysis, we categorise the records by `Study Type` and count the number of instances of each type. The most common kinds of clinical trials are then revealed by sorting the results in descending order. 
-- MAGIC
-- MAGIC This quest reveals the most common study format and aids in our understanding of methodological preferences in clinical research.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("""
-- MAGIC     SELECT `Study Type`, COUNT(*) AS Frequency
-- MAGIC     FROM clinical_trials
-- MAGIC     GROUP BY `Study Type`
-- MAGIC     ORDER BY Frequency DESC
-- MAGIC """).show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For instance, more interventional research can indicate a focus on evaluating new medicines or treatments. Knowing this distribution is also helpful for finding opportunities in under-represented trial types or coordinating future research with industry standards.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Top 10 Most Common Conditions
-- MAGIC
-- MAGIC ### The top 10 conditions along with their frequency (note, that the Condition column can contain multiple conditions in each row, so you will need to separate these out and count each occurrence separately) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To determining the dataset's ten most researched medical conditions. The format of the `Conditions` column causes an issue in this case since it may include several conditions in a single string, usually divided by semicolons or commas. We divide these strings into distinct rows, each of which represents a particular condition, using `split` and `explode` functions to allow for precise frequency analysis.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Transformation to split multiple conditions
-- MAGIC condition_df = df.select(explode(split(col("Conditions"), "\\|")).alias("Condition"))
-- MAGIC
-- MAGIC # Register SQL view for exploded conditions
-- MAGIC condition_df.createOrReplaceTempView("exploded_conditions")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC After transformation, we mark this DataFrame as a view and use Spark SQL to count the occurrences of each condition group. This enables us to determine which conditions have been studied the most in all clinical studies.
-- MAGIC
-- MAGIC Understanding current research trends, medical problem areas, and prospective markets for pharmaceutical innovation all depend on this information. High-frequency diseases like "diabetes," "cancer," or "hypertension" frequently indicate serious public health problems or substantial institutional and corporate research expenditures.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.sql("""
-- MAGIC     SELECT Condition, COUNT(*) AS Frequency
-- MAGIC     FROM exploded_conditions
-- MAGIC     GROUP BY Condition
-- MAGIC     ORDER BY Frequency DESC
-- MAGIC     LIMIT 20
-- MAGIC """).show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Mean Length of Clinical Trials (in Months)
-- MAGIC
-- MAGIC ### For studies with an end date, calculate the mean clinical trial length in months.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To calculate the average length of clinical trials with a non-null `Completion Date`. The `months_between` function, which calculates the number of months between the `Start Date` and `Completion Date` for each study results, is used to assess duration in months. To ensure precise computations, both columns are transformed from string to date format using the `to_date` function before this function is applied.The outcome is a column with numbers that shows how many months each trial lasted. The mean clinical trial time is then determined by averaging this column.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Converting date and add duration
-- MAGIC duration_df = df.withColumn("Start Date", to_date(col("Start Date"), "yyyy-MM-dd")) \
-- MAGIC                 .withColumn("Completion Date", to_date(col("Completion Date"), "yyyy-MM-dd")) \
-- MAGIC                 .withColumn("Trial Length Months", months_between(col("Completion Date"), col("Start Date")))
-- MAGIC
-- MAGIC # Register SQL view
-- MAGIC duration_df.createOrReplaceTempView("trial_durations")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # SQL to calculate mean
-- MAGIC spark.sql("""
-- MAGIC     SELECT ROUND(AVG(`Trial Length Months`), 2) AS Mean_Trial_Length_In_Months
-- MAGIC     FROM trial_durations
-- MAGIC     WHERE `Completion Date` IS NOT NULL
-- MAGIC """).show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC When comparing timetables from various study kinds or funding agencies, this statistic can be used as a standard. It is especially helpful for understanding the time commitment needed for different medical research initiatives and for project planning. We make sure that unfinished or continuing research doesn't distort the results by eliminating null values.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Completed Diabetes Studies per Year 
-- MAGIC
-- MAGIC ### From the studies with a non-null completion date and a status of ‘Completed’ in the Study Status, calculate how many of these related to Diabetes each year. Display the trend over time in an appropriate visualisation. (For this you can assume all relevant studies will contain an exact match for ‘Diabetes’ or ‘diabetes’ in the Conditions column.) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To examine the pattern of diabetes-related clinical trials that have been performed throughout time. Evaluating whether research activity in this field is increasing, decreasing, or staying the same is the aim. 
-- MAGIC
-- MAGIC In order to accomplish this, we filter the dataset to only include records with the following criteria: the Conditions field contain either 'Diabetes' or 'diabetes' (to allow for case-insensitive matches), the Completion Date is not null, and the 'Study Status is 'Completed'.
-- MAGIC
-- MAGIC To determine the number of diabetes-related trials completed each year, we next use the `year()` method to extract the year from the `Completion Date` and group the results by this year.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Preprocess: filter status, convert date, extract year
-- MAGIC completed_df = df.withColumn("Completion Date", to_date(col("Completion Date"), "yyyy-MM-dd")) \
-- MAGIC                  .filter(col("Completion Date").isNotNull())
-- MAGIC                  
-- MAGIC # Register as view
-- MAGIC completed_df.createOrReplaceTempView("completed_diabetes")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # SQL to count per year
-- MAGIC spark.sql("""
-- MAGIC     SELECT 
-- MAGIC         YEAR(`Completion Date`) AS Year,
-- MAGIC         COUNT(*) AS DiabetesStudies
-- MAGIC     FROM completed_diabetes
-- MAGIC     WHERE lower(Conditions) LIKE '%diabetes%'
-- MAGIC       AND lower(`Study Status`) = 'completed'
-- MAGIC     GROUP BY YEAR(`Completion Date`)
-- MAGIC     ORDER BY Year
-- MAGIC """).show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Visualization: Trend of Completed Diabetes Clinical Trials Per Year

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The annual trend of finished clinical studies pertaining to diabetes is displayed in this graph.
-- MAGIC We used Spark SQL to filter the dataset for trials that had the condition "Diabetes" in the conditions field and a "Completed" status.  We sorted the results by year and totalled them after removing the year from the completion date. 
-- MAGIC The quantity of relevant studies.  The graphic that results makes it easier to determine if research attention and interest in diabetes have grown, decreased, or stayed the same during the course of the analysis.  Such visual trends are useful for determining future funding allocations or health policy, as well as for recognising research objectives.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC
-- MAGIC # Convert to Pandas for visualization
-- MAGIC diabetes_trend_df = spark.sql("""
-- MAGIC     SELECT 
-- MAGIC         YEAR(`Completion Date`) AS Year,
-- MAGIC         COUNT(*) AS DiabetesStudies
-- MAGIC     FROM completed_diabetes
-- MAGIC     WHERE lower(Conditions) LIKE '%diabetes%'
-- MAGIC       AND lower(`Study Status`) = 'completed'
-- MAGIC     GROUP BY YEAR(`Completion Date`)
-- MAGIC     ORDER BY Year
-- MAGIC """).toPandas()
-- MAGIC
-- MAGIC # Plot the trend
-- MAGIC plt.figure(figsize=(10, 6))
-- MAGIC plt.plot(diabetes_trend_df['Year'], diabetes_trend_df['DiabetesStudies'], marker='o')
-- MAGIC plt.title('Completed Diabetes Studies Per Year')
-- MAGIC plt.xlabel('Year')
-- MAGIC plt.ylabel('Number of Studies')
-- MAGIC plt.grid(True)
-- MAGIC plt.tight_layout()
-- MAGIC plt.show()
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Learning Outcomes
-- MAGIC
-- MAGIC Finishing this task gave me invaluable practical experience using big data tools, especially Apache Spark. The assignment highlighted how crucial it is to preprocess and query huge datasets, particularly when working with complex fields like date calculations or multi-valued conditions. One important lesson learnt was how to combine SQL queries with PySpark DataFrame operations to attain flexibility and speed. Finding insights from raw data through exploratory analysis was another crucial learning objective. This research illustrated how data analysis may guide strategic decision-making in pharmaceutical and healthcare environments by addressing problems about frequency, temporal patterns, and duration. Additionally, the research gained an interpretive layer from the use of visualisations to convey trends, highlighting the significance of narrative in data science.
-- MAGIC