-- Databricks notebook source
SET TIME ZONE 'Pacific/Auckland';

-- COMMAND ----------

-- DBTITLE 1,Final FK cleared offer in GOLD table seems to come from "Two" source files (Should be from the last source file ony)
select TradingPeriodNumber, ParticipantCode, case when DispatchBlockCode = 'N/A' then PointOfConnectionCode || ' ' || UnitCode else DispatchBlockCode end as DispatchBlock, IslandCode, 
Megawatts, Dollars, regexp_replace(`FileName`,'/mnt/eadatalakeprd/datalake/FK_OffersDispatched/','') as FK_FileName
from gold.frequencykeepingdispatchedoffers 
where TradingDate = '2024-01-13' and TradingPeriodNumber = 3
order by FK_FileName, TradingPeriodNumber, ParticipantCode,PointOfConnectionCode, UnitCode, DispatchBlockCode;

-- COMMAND ----------

-- DBTITLE 1,The raw data show that the data for the same trading period in the "three course files" are different. WHY???
select TRADING_PERIOD,TRADER_NAME,PNODE_NAME,FREQUENCY_BAND_MW,OFFER_PRICE,ISLAND_NAME, 
regexp_replace(`FileName`,'/mnt/eadatalakeprd/datalake/FK_OffersDispatched/','') as FK_FileName
from bronze.fk_offersdispatched where TRADING_PERIOD_START_DATE = '13/01/2024' and TRADING_PERIOD = 3 
Order by FK_FileName

-- COMMAND ----------

-- DBTITLE 1,Offer data seems to show that WTR and WTO are the cheapest FKeepers in SI and NI respectively
select TradingPeriodNumber, ParticipantCode, case when DispatchBlockCode = 'N/A' then PointOfConnectionCode || ' ' || UnitCode else DispatchBlockCode end as DispatchBlock, IslandCode, 
Megawatts, Dollars, regexp_replace(`FileName`,'/mnt/eadatalakeprd/datalake/FK_OffersDispatched/','') as FK_FileName, * 
from gold.frequencykeepingoffers where TradingDate = '2024-01-13' and TradingPeriodNumber = 3 and IsLatestSubmissionFlag = 'Y' and Megawatts =15
Order by IslandCode, Dollars

-- COMMAND ----------

-- DBTITLE 1,Real time dispatch data shows that on WTR and WTO are FKeeprs for this trading period.
select `INTERVAL`, CONSTRAINTNAME, * from bronze.spd_rtd_mssdata_mndconstraint 
where `INTERVAL` like '13-JAN-2024 01:%' and CONSTRAINTNAME like '%CTRLMAX'
and `INTERVAL` not like '13-JAN-2024 01:3%'
and `INTERVAL` not like '13-JAN-2024 01:4%'
and `INTERVAL` not like '13-JAN-2024 01:5%'
order by `INTERVAL`
