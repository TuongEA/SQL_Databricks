-- Databricks notebook source
SELECT IslandCode, a.ReservoirCode, date_format(StorageDate,'yyyyMMdd') as TradingDate, StorageTimeNZT, GigawattHours
FROM brown.environment_hydrostorages a INNER JOIN processing.silver_dimreservoir b
ON (a.ReservoirCode = b.ReservoirCode)
WHERE StorageDate >= '2022-08-22'
AND a.IsCurrentFlag = 'Y'
Order by StorageDate, StorageTimeNZT, ReservoirCode

-- COMMAND ----------

SELECT IslandCode, a.ReservoirCode, date_format(StorageDate,'yyyyMMdd') as TradingDate, StorageTimeNZT, GigawattHours
FROM brown.environment_hydrostorages a INNER JOIN processing.silver_dimreservoir b
ON (a.ReservoirCode = b.ReservoirCode)
WHERE StorageDate = '2022-06-08'
AND a.IsCurrentFlag = 'Y'
Order by StorageDate, StorageTimeNZT, ReservoirCode

-- COMMAND ----------

SELECT * FROM brown.environment_hmdflowdescription

-- COMMAND ----------

SELECT * FROM brown.environment_hmdflowdata
