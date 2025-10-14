-- Databricks notebook source
select TradingDate, TradingPeriodNumber, TradingPeriodStartTime, IslandCode, SPDResidualEnergyMegawatts, * from copper.spd_prss_solution_island where TradingDate = current_date()-1 order by TradingDate, TradingPeriodNumber, FileName,IslandCode

-- COMMAND ----------

select TradingDate, TradingPeriodNumber, RunDateTime, IslandCode, SPDResidualEnergyMegawatts, * 
from sensitive.spdisland where TradingDate = current_date()-1  and CaseTypeCode = 'PRSS' order by TradingDate, TradingPeriodNumber, RunDateTime desc , IslandCode
