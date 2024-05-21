-- Databricks notebook source
-- MAGIC %python
-- MAGIC fileroot = "clinicaltrial_2023"
-- MAGIC pharma = "pharma"
-- MAGIC
-- MAGIC dbutils.fs.head("/FileStore/tables/" + fileroot + ".csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Creating DataFrame
-- MAGIC from pyspark.sql.types import * #StructType, StructField
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC delimiter_selector = {
-- MAGIC     "clinicaltrial_2023": "\t",
-- MAGIC     "clinicaltrial_2021": "|",
-- MAGIC     "clinicaltrial_2020": "|",  
-- MAGIC     "pharma": ","
-- MAGIC }
-- MAGIC
-- MAGIC
-- MAGIC def create_pharma_dataframe(pharma):
-- MAGIC     df = spark.read.options(delimiter=delimiter_selector[pharma]).csv(f"/FileStore/tables/{pharma}.csv")
-- MAGIC     first = df.first()
-- MAGIC     for col in range(0, len(list(first))):
-- MAGIC         df = df.withColumnRenamed(f"_c{col}", list(first)[col])
-- MAGIC     df = df.withColumn('index', monotonically_increasing_id())
-- MAGIC     return df.filter(~df.index.isin([0])).drop('index')
-- MAGIC
-- MAGIC pharma_dataframe = create_pharma_dataframe(pharma)
-- MAGIC
-- MAGIC
-- MAGIC def create_clinical_dataframe(clinicaltrial_2023):
-- MAGIC     if clinicaltrial_2023 == "clinicaltrial_2023":
-- MAGIC         rdd = sc.textFile(f"/FileStore/tables/{clinicaltrial_2023}.csv").map(lambda row: row.replace(',', '').replace('"', '').split(delimiter_selector[clinicaltrial_2023]))
-- MAGIC         head = rdd.first()
-- MAGIC         rdd = rdd.map(lambda row: row + [" " for i in range(len(head) - len(row))] if len(row) < len(head) else row )
-- MAGIC         df = rdd.toDF()
-- MAGIC         first = df.first()
-- MAGIC         for col in range(0, len(list(first))):
-- MAGIC            df = df.withColumnRenamed(f"_{col + 1}", list(first)[col])
-- MAGIC         df = df.withColumn('index', monotonically_increasing_id())
-- MAGIC         return df.filter(~df.index.isin([0])).drop('index')
-- MAGIC     else:
-- MAGIC         return spark.read.csv(f"/FileStore/tables/clinicaltrial_2023.csv", sep=delimiter_selector[clinicaltrial_2023], header = True)
-- MAGIC
-- MAGIC clinical_dataframe = create_clinical_dataframe(fileroot)
-- MAGIC clinical_dataframe.show(20)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinical_dataframe.createOrReplaceTempView(fileroot)
-- MAGIC pharma_dataframe.createOrReplaceTempView(pharma)

-- COMMAND ----------

SELECT * 
FROM clinicaltrial_2023

-- COMMAND ----------

SELECT * 
FROM pharma

-- COMMAND ----------

SELECT DISTINCT count(*) 
FROM clinicaltrial_2023

-- COMMAND ----------

SELECT clinicaltrial_2023.Type, count(*) as count FROM clinicaltrial_2023
GROUP BY clinicaltrial_2023.Type
ORDER BY count DESC
LIMIT 3

-- COMMAND ----------


CREATE OR REPLACE TEMP VIEW all_conditions AS SELECT explode(split(clinicaltrial_2023.conditions, ",")) as conditions FROM clinicaltrial_2023;
SELECT conditions, count(*) as count FROM all_conditions
GROUP BY conditions
ORDER BY count DESC
LIMIT 5

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW non_pharma_sponsor AS SELECT Sponsor FROM clinicaltrial_2023 WHERE Sponsor NOT IN (SELECT Parent_Company FROM pharma);
SELECT Sponsor, count(*) as count FROM non_pharma_sponsor
GROUP BY Sponsor
ORDER BY count DESC
LIMIT 10

-- COMMAND ----------


SELECT SUBSTRING(Completion, 1, 4) AS Year, SUBSTRING(Completion, 6, 2) as Month,
CASE SUBSTRING(Completion, 6, 2)
    WHEN '01' THEN 'Jan'
    WHEN '02' THEN 'Feb'
    WHEN '03' THEN 'Mar'
    WHEN '04' THEN 'Apr'
    WHEN '05' THEN 'May'
    WHEN '06' THEN 'Jun'
    WHEN '07' THEN 'Jul'
    WHEN '08' THEN 'Aug'
    WHEN '09' THEN 'Sep'
    WHEN '10' THEN 'Oct'
    WHEN '11' THEN 'Nov'
    WHEN '12' THEN 'Dec'
  END AS MonthName, 
COUNT(Status) as Completed_Studies
FROM clinicaltrial_2023
WHERE SUBSTRING(Completion, 1, 4) = 2023 AND Status = 'COMPLETED'
GROUP BY 1, 2, 3
ORDER BY 2 ASC ;

-- COMMAND ----------

SELECT offense_group, COUNT(*) AS count
FROM pharma
GROUP BY offense_group
ORDER BY count DESC;

-- COMMAND ----------


