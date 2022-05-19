-- Databricks notebook source
-- MAGIC %py
-- MAGIC dbutils.fs.mkdirs("/FileStore/clinicaltrial") 

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.cp("FileStore/tables/clinicaltrial_2021.csv" , "/FileStore/clinicaltrial/", True)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.ls("/FileStore/clinicaltrial/")

-- COMMAND ----------

create external table clinicaltrial(
Id string ,
Sponsor string,
Status string,
Start string,
Completion string,
Type string,
Submission string,
Conditions string,
Interventions string )
Row format delimited 
fields terminated by "|"
TBLPROPERTIES("skip.header.line.count"="1") 
location "/FileStore/clinicaltrial/"

-- COMMAND ----------

--load data inpath "/FileStore/tables/"
--overwrite into Table clinicaltrial_2019;

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.mkdirs("/FileStore/mesh")

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.cp("FileStore/tables/mesh.csv" , "/FileStore/mesh/", True)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.ls("/FileStore/mesh/")

-- COMMAND ----------

create external table mesh(
term string,
tree string )
Row format delimited 
fields terminated by ","
lines terminated by "\n"
location "/FileStore/mesh/"
TBLPROPERTIES ('skip.header.line.count'='1')



-- COMMAND ----------

--load data inpath "/FileStore/tables/"
--overwrite into Table mesh;

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.mkdirs("/FileStore/pharma")

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.cp("FileStore/tables/pharma.csv" , "/FileStore/pharma/", True)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC dbutils.fs.ls("/FileStore/pharma/")

-- COMMAND ----------

create external table pharma(
Company string ,
Parent_Company string,
Penalty_Amount string,
Subtraction_from_Penalty string,
Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting string,
Penalty_Year string,
Penalty_Date string,
Offence_Date string,
Primary_Offence string, 
Secondary_Offence string ,
Level_of_Government string,
Action_Type string,
Agency string,
Prosecution_Agreement string,
Court string,
Case_ID string,
Private_Litigation_Case_Title string,
Lawsuit_Resolution string, 
Faciity_State string,
City string,
Address string,
Zip string,
NAICS_Code string)
Row format delimited 
fields terminated by ","
lines terminated by "\n"
TBLPROPERTIES("skip.header.line.count"="1")
location "/FileStore/pharma/"


-- COMMAND ----------

--load data inpath "/FileStore/tables/"
--overwrite into Table pharma;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 1 

-- COMMAND ----------

SELECT  distinct(count(*) )
FROM clinicaltrial
WHERE Id LIKE "N%%";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 2

-- COMMAND ----------

SELECT Type, COUNT(*) As Frequencies
FROM clinicaltrial
GROUP BY Type
ORDER BY Frequencies DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 3

-- COMMAND ----------

SELECT Conditions, COUNT(*) AS Frequencies
FROM (SELECT EXPLODE(SPLIT(Conditions, ","))AS Conditions FROM clinicaltrial)
WHERE Conditions <> ""
GROUP BY Conditions
ORDER BY Frequencies DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 4 

-- COMMAND ----------

SELECT *
FROM mesh

-- COMMAND ----------

SELECT term,  substring(tree, 0,3) AS Hierarchy , Conditions
FROM mesh
INNER JOIN clinicaltrial ON clinicaltrial.Conditions = mesh.term


-- COMMAND ----------

create view LISTEDCONDITIONS AS
SELECT LISTEDCONDITIONS
FROM clinicaltrial
LATERAL VIEW OUTER EXPLODE(SPLIT(Conditions, ",")) AS LISTED_CONDITIONS


-- COMMAND ----------

SELECT  substring(tree, 0,3) AS Hierarchy , COUNT(substring(tree, 0,3)) AS Frequency 
FROM mesh
INNER JOIN LISTEDCONDITIONS ON LISTEDCONDITIONS.LISTED_CONDITIONS = mesh.term
GROUP BY substring(tree, 0,3) 
ORDER BY Frequency DESC
LIMIT 5


-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 5 

-- COMMAND ----------

SELECT *
FROM pharma

-- COMMAND ----------

CREATE VIEW ParentCompany AS 
SELECT REPLACE (LTRIM(RTRIM(REPLACE(Parent_Company, '"', ''))), '', '"') AS New_Parent_Company
FROM pharma

-- COMMAND ----------

SELECT Sponsor  , COUNT(*) AS Number_of_Clinical_Trials 
FROM clinicaltrial
LEFT ANTI JOIN ParentCompany 
ON  clinicaltrial.Sponsor = ParentCompany.New_Parent_Company
GROUP BY Sponsor
ORDER BY COUNT(*) DESC
LIMIT 10


-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 6

-- COMMAND ----------

SELECT Completion , Count(*) As Completed_Studies
FROM clinicaltrial
WHERE (Status = "Completed"  AND Completion LIKE "%%2021")
GROUP BY Completion

-- COMMAND ----------

CREATE VIEW MONTHS AS 
SELECT UNIX_TIMESTAMP(LEFT(Completion,3), 'MMM') as Months, COUNT(*) AS Completed_Studies
FROM clinicaltrial
WHERE (Status = "Completed"  AND Completion LIKE "%%2021")
GROUP BY Completion
ORDER BY MONTHS 

-- COMMAND ----------

SELECT FROM_UNIXTIME(MONTHS, 'MMM') AS Months, Completed_Studies
FROM MONTHS

-- COMMAND ----------


