-- Databricks notebook source
Select * from gold.dispatchinstructions
WHERE TradingDate = '2022-10-18'

-- COMMAND ----------

Select distinct TradingPeriodID, CaseName, SPDCaseID from processing.copper_dispatchinstructions
WHERE TradingDate = '2022-10-18'
ORDER BY TradingPeriodID, CaseName, SPDCaseID
