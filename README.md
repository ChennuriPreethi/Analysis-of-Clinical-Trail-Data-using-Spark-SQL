# Analysis-of-Clinical-Trail-Data-using-Spark-SQL

This project explores large-scale clinical trial data using Apache Spark SQL, hosted on the Databricks platform. The dataset includes over 500,000 clinical trial records from ClinicalTrials.gov, focusing on metadata such as study types, conditions studied, funding sources, and timeline-related attributes. The goal is to demonstrate scalable data analysis techniques using Spark SQL to extract actionable insights from healthcare data.

## Objective
The primary aim is to investigate trends and patterns in clinical trials to:

- Identify the most common types of trials.

- Determine frequently studied medical conditions.

- Estimate average durations of clinical trials.

- Analyze temporal trends in clinical trial registrations.

- Discover geographical and organizational trial distributions.

## Tools and Technologies
- Apache Spark SQL: Used for querying and transforming large datasets efficiently.

- Databricks Community Edition: A collaborative platform for running Spark jobs.

- CSV and JSON data files: Contain clinical trial metadata used for analysis.

## Dataset Overview
The dataset was sourced from ClinicalTrials.gov and includes structured information such as:

- Study type (Interventional, Observational, etc.)

- Study status (Completed, Recruiting, etc.)

- Conditions studied (e.g., Diabetes, Cancer)

- Sponsors and funding types (Industry, NIH, etc.)

- Dates of registration and completion

## Analysis Workflow
1. Data Loading: Data was read from the Databricks File System (DBFS) into Spark DataFrames.

2. Data Cleaning: Null values were handled, columns renamed for clarity, and date formats standardized.

3. Exploratory Analysis:

- Most frequent conditions and study types.

- Trends over time in new registrations.

- Duration distribution of completed trials.

- Frequency of studies by phase (e.g., Phase 1, Phase 3).

- Top organizations and countries involved in trials.

4. Advanced Queries: Complex Spark SQL queries were used to derive metrics and identify patterns, such as:

- Median trial length.

- Year-over-year comparison of trial counts.

- Distribution by gender eligibility.

## Key Insights
- Interventional trials are the most prevalent type.

- Common conditions include cancer-related diseases, diabetes, and cardiovascular conditions.

- The average duration for completed trials varies by study type.

- A significant rise in trial registrations was observed in the past decade.

- U.S.-based institutions lead in trial sponsorship.

## Benefits of Using Spark SQL
- Handles massive volumes of data efficiently.

- SQL syntax enables ease of query formulation for users with SQL background.

- Scalable and integrable with cloud platforms like Databricks.

## Conclusion
This project highlights how Spark SQL can transform raw, large-scale clinical data into meaningful business and research insights. It can support healthcare analytics initiatives and drive data-driven decision-making in biomedical research.

## Future Work
- Integrate natural language processing to analyze trial descriptions.

- Automate anomaly detection in trial registration patterns.

- Expand analysis to include outcome data where available.
