-- Databricks notebook source
SET TIME ZONE 'Pacific/Auckland';
--SET TIME ZONE 'Etc/UTC';

-- COMMAND ----------

SELECT PointOfConnectionCode, max(RunDateTime) FROM gold.realtimeenergyprices 
WHERE PointOfConnectionCode in ('HAM0551','TNG0551') and DollarsPerMegawattHour <> 0 
GROUP BY PointOfConnectionCode 

-- COMMAND ----------

SELECT * FROM gold.predispatchandfinalenergyprices WHERE PointOfConnectionCode in ('HAM0551','TNG0551') 
and TradingDate = CURRENT_DATE - 3 and TradingPeriodNumber >= 15
ORDER BY TradingPeriodNumber

-- COMMAND ----------

SELECT * FROM sensitive.spdnodal WHERE PointOfConnectionCode in ('HAM0551','TNG0551') 
and TradingDate = CURRENT_DATE - 3 and TradingPeriodNumber >= 14
ORDER BY TradingPeriodNumber

-- COMMAND ----------

SELECT Distinct TradingDate, TradingIntervalStartTime, RunDateTime, InsertedDatetime
--, * 
FROM sensitive.spdisland 
WHERE CaseTypeCode = 'RTD'
ORDER BY TradingDate desc, TradingIntervalStartTime desc
limit 10

-- COMMAND ----------

SELECT * FROM sensitive.spdnodal
--WHERE CaseTypeCode = 'RTD'
ORDER BY TradingDate desc--, TradingIntervalStartTime desc
limit 10

-- COMMAND ----------

SELECT * FROM bronze.casefiles_csv_rtd_solution_island
--WHERE TradingDate = '2022-10-10'
ORDER BY BronzeInsertTimestamp desc
limit 10

-- COMMAND ----------

SELECT * FROM processing.copper_solutionisland_nrss
--WHERE TradingDate = '2022-10-10'
ORDER BY CopperInsertedDateTime desc
limit 10

-- COMMAND ----------

DESCRIBE 
-- DETAIL
processing.copper_solutionisland_rtd

-- COMMAND ----------

-- MAGIC %md #Test

-- COMMAND ----------

SELECT CaseTypeCode, Tradingdate, Count(*) as N FROM sensitive.spdnodal
WHERE Tradingdate >= CURRENT_DATE - 10
GROUP BY CaseTypeCode, Tradingdate
ORDER BY CaseTypeCode, Tradingdate desc


-- COMMAND ----------

SELECT CaseTypeCode, Tradingdate, Count(*) as N FROM sensitive.spdisland
WHERE Tradingdate >= CURRENT_DATE - 7
GROUP BY CaseTypeCode, Tradingdate
ORDER BY CaseTypeCode, Tradingdate desc

-- COMMAND ----------

SELECT CaseTypeCode, Tradingdate, Count(*) as N FROM sensitive.spdbidsandoffers
WHERE Tradingdate >= CURRENT_DATE -7
AND CaseTypeCode is null
GROUP BY CaseTypeCode, Tradingdate
ORDER BY CaseTypeCode, Tradingdate desc

-- COMMAND ----------

SELECT * FROM sensitive.spdbidsandoffers
WHERE Tradingdate = CURRENT_DATE - 7
AND CaseTypeCode is null


-- COMMAND ----------

SELECT *  FROM bronze.casefiles_csv_rtd_mssdata_bidsandoffers
-- WHERE Tradingdate >= CURRENT_DATE - 7
-- GROUP BY CaseTypeCode, Tradingdate
-- ORDER BY CaseTypeCode, Tradingdate desc
ORDER BY BronzeInsertTimestamp desc
limit 10

-- COMMAND ----------

describe detail bronze.casefiles_csv_prsl_solution_island

-- COMMAND ----------

SELECT * FROM bronze.casefiles_csv_prsl_solution_island limit 1
