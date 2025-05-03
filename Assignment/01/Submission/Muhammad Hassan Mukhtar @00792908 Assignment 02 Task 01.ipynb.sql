-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Code**
-- MAGIC - Create the directory "assignment_2" to store all files for this assignment.
-- MAGIC - After uploading, confirm the successful upload at the location "/FileStore/tables/assignment_2".

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.mkdirs("/FileStore/tables/assignment_2")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.ls("/FileStore/tables/assignment_2")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Code**
-- MAGIC - from pyspark.sql.functions import col, split, explode, datediff, month, year, lower: Brings in tools to work with columns, split text, separate lists, calculate date differences, and handle years, months, or lowercase text in Spark.
-- MAGIC - import matplotlib.pyplot as plt: Brings in a tool to draw graphs and charts.
-- MAGIC - df = spark.read.csv(...): Loads the file "Clinicaltrial_16012025.csv" from a folder into a Spark table called df.
-- MAGIC - "/FileStore/tables/assignment_2/Clinicaltrial_16012025.csv": Tells Spark where to find the CSV file.
-- MAGIC header=True: Says the first row of the file has column names.
-- MAGIC - inferSchema=True: Lets Spark guess the data types (like dates or numbers) for each column.
-- MAGIC - df.createOrReplaceTempView("clinical_trials"): Names the loaded data "clinical_trials" so it can be used in SQL queries.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.functions import col, split, explode, datediff, month, year, lower
-- MAGIC
-- MAGIC df = spark.read.csv(
-- MAGIC     "/FileStore/tables/assignment_2/Clinicaltrial_16012025.csv",
-- MAGIC     header=True, 
-- MAGIC     inferSchema=True, 
-- MAGIC     quote='"', 
-- MAGIC     escape='"', 
-- MAGIC     multiLine=True 
-- MAGIC )
-- MAGIC df.createOrReplaceTempView("clinical_trials")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Code**
-- MAGIC - Verify if the temporary view table exists.
-- MAGIC - Get a list of all column names in the temporary view table.
-- MAGIC - Take a glimpse of the data to understand its structure and content.

-- COMMAND ----------


SHOW TABLES

-- COMMAND ----------


SHOW COLUMNS FROM clinical_trials;

-- COMMAND ----------


SELECT * FROM clinical_trials LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1. List all the clinical trial types (as contained in the Type column of the data) along with their frequency, sorting the results from most to least frequent?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Study Type**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Code**
-- MAGIC - SELECT Study Type, COUNT(*) as Frequency: Picks each study type and counts how many times it shows up, calling it "Frequency."
-- MAGIC - FROM clinical_trials: Looks at the clinical_trials table for the data.
-- MAGIC - GROUP BY Study Type``: Puts all the same study types together to count them.
-- MAGIC - ORDER BY Frequency DESC: Lists them from most common to least common.

-- COMMAND ----------


SELECT `Study Type`, COUNT(*) as Frequency
FROM clinical_trials
WHERE `Study Type` IS NOT NULL
GROUP BY `Study Type`
ORDER BY Frequency DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Result**
-- MAGIC - Interventional studies (399,654) dominate the dataset, followed by observational studies (120,816), reflecting typical clinical research trends.
-- MAGIC - Unexpected entries, such as numeric values (e.g., 60, 150) and institution names (e.g., "Duke University"), indicate potential data entry errors.
-- MAGIC - 919 records have NULL values, suggesting missing or incomplete data that require further investigation.
-- MAGIC - Misclassified entries, such as interventions or locations appearing as study types, highlight the need for data validation and cleaning.
-- MAGIC - Stricter data entry controls and predefined categories should be implemented to ensure accuracy in future records.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Funder Type**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - SELECT Funder Type, COUNT(*) as Frequency: Picks each funder type and counts how many times it appears, calling it "Frequency."
-- MAGIC - FROM clinical_trials: Looks at the clinical_trials table for the data.
-- MAGIC - GROUP BY Funder Type``: Puts all the same funder types together to count them.
-- MAGIC - ORDER BY Frequency DESC: Lists them from most common to least common.

-- COMMAND ----------


SELECT `Funder Type`, COUNT(*) as Frequency
FROM clinical_trials
WHERE `Funder Type` IS NOT NULL
GROUP BY `Funder Type`
ORDER BY Frequency DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Result**
-- MAGIC - Top Funders – "OTHER" (367K), "INDUSTRY" (119K), and "OTHER_GOV" (13K) dominate.
-- MAGIC - Government Role – NIH (11K) and FED (4K) contribute but are smaller than industry.
-- MAGIC - Data Issues – Unexpected numeric and country name entries suggest errors.
-- MAGIC - Unclear Categories – "UNKNOWN," "null," and "AMBIG" indicate missing data.
-- MAGIC - Institutional Funders – Some universities and hospitals fund trials but in small numbers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. The top 10 conditions along with their frequency (note, that the Condition column can contain multiple conditions in each row, so you will need to separate these out and count each occurrence separately)?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - SELECT condition, COUNT(*) as Frequency: Picks each condition and counts how many times it shows up, calling it "Frequency."
-- MAGIC - FROM (...) exploded_conditions: Gets data from a step inside (called "exploded_conditions").
-- MAGIC - SELECT EXPLODE(SPLIT(Conditions, ',')) as condition: Breaks the Conditions list into separate pieces and makes a row for each one, calling it "condition"
-- MAGIC - FROM clinical_trials: Looks at the clinical_trials table for the data.
-- MAGIC - WHERE Conditions IS NOT NULL: Only uses rows where Conditions isn’t empty.
-- MAGIC - GROUP BY condition: Puts all the same conditions together to count them.
-- MAGIC - ORDER BY Frequency DESC: Lists them from most common to least common.
-- MAGIC - LIMIT 10: Shows only the top 10 most common conditions.

-- COMMAND ----------


SELECT condition, COUNT(*) as Frequency
FROM (
    SELECT EXPLODE(SPLIT(`Conditions`, ',')) as condition
    FROM clinical_trials
    WHERE `Conditions` IS NOT NULL
) exploded_conditions
GROUP BY condition
ORDER BY Frequency DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Result**
-- MAGIC - The query reveals the top 10 most frequent medical conditions associated with clinical trials, with "Healthy" being the most common condition, followed by "Breast Cancer" and "Diabetes Mellitus".
-- MAGIC - The frequency of conditions ranges from 8433 ("Healthy") to 2143 ("HIV Infections"), indicating a significant variation in the number of clinical trials focused on each condition.
-- MAGIC - The presence of conditions like "Breast Cancer", "Diabetes Mellitus", and "Prostate Cancer" in the top 10 list may reflect their relatively high prevalence rates in the population, as well as the ongoing research efforts to develop effective treatments.
-- MAGIC - The query's results may be influenced by data quality issues, such as inconsistent or missing values in the "Conditions" column. The use of string splitting and exploding functions may also introduce errors if the data is not properly formatted.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3. For studies with an end date, calculate the mean clinical trial length in months?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - SELECT AVG(DATEDIFF(Completion Date, Start Date) / 30.0) as MeanTrialLengthMonths: Finds the average time in months between Start Date and - Completion Date for each trial, calling it "MeanTrialLengthMonths."
-- MAGIC - FROM clinical_trials: Looks at the clinical_trials table for the data.
-- MAGIC - WHERE Completion DateIS NOT NULL ANDStart Date IS NOT NULL: Only uses trials that have both a start date and an end date filled in.

-- COMMAND ----------


SELECT AVG(DATEDIFF(`Completion Date`, `Start Date`) / 30.0) as MeanTrialLengthMonths
FROM clinical_trials
WHERE `Completion Date` IS NOT NULL AND `Start Date` IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Result**
-- MAGIC - The query calculates the average length of clinical trials in months, yielding a result of approximately 36.09 months .

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 4. From the studies with a non-null completion date and a status of ‘Completed’ in the Study Status, calculate how many of these related to Diabetes each year.Display the trend over time in an appropriate visualisation. (For this you can assume all relevant studies will contain an exact match for ‘Diabetes’ or ‘diabetes’ in the Conditions column)?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - SELECT YEAR(Completion Date) as Year, COUNT(*) as DiabetesStudies: Picks the year from Completion Date and counts trials, calling it "DiabetesStudies."
-- MAGIC - FROM clinical_trials: Looks at the clinical_trials table for the data.
-- MAGIC - WHERE Study Status = 'COMPLETED': Only uses trials that are finished (status is "COMPLETED").
-- MAGIC - AND YEAR(Completion Date) IS NOT NULL: Makes sure the completion year isn’t missing.
-- MAGIC - AND LOWER(Conditions) LIKE '%diabetes%': Only includes trials with "diabetes" in the conditions (ignoring case).
-- MAGIC - GROUP BY YEAR(Completion Date): Puts all trials from the same year together to count them.
-- MAGIC - ORDER BY Year: Lists the years in order from earliest to latest.

-- COMMAND ----------


SELECT YEAR(`Completion Date`) as Year, COUNT(*) as DiabetesStudies
FROM clinical_trials
WHERE `Study Status` = 'COMPLETED'
  AND YEAR(`Completion Date`) IS NOT NULL
  AND LOWER(`Conditions`) LIKE '%diabetes%'
GROUP BY YEAR(`Completion Date`)
ORDER BY Year

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Result**
-- MAGIC - Diabetes-related clinical trials increased significantly from the early 2000s, peaking around 2019.
-- MAGIC - The highest number of completed studies (755) occurred in 2019, showing strong research interest.
-- MAGIC - A sharp decline in 2020 (559 studies) suggests disruptions due to the pandemic.
-- MAGIC - While 2022 (654) and 2023 (616) saw some recovery, 2024 shows a steep drop (427).
-- MAGIC - The sudden fall to 1 study in 2025 likely indicates incomplete or ongoing data collection.
