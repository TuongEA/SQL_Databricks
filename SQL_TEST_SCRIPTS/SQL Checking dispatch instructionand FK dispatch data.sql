-- Databricks notebook source
SET TIME ZONE 'Pacific/Auckland';

-- COMMAND ----------

-- DBTITLE 1,Count daily records from each daily_dispatch file in bronze
select left(TRADING_PERIOD_ID,8) as TRADINGDATE,BronzeInsertTimestamp,InsertBatchLogID, FileName, count(*) as N from ea_prd.bronze.dispatchinstruction 
where left(TRADING_PERIOD_ID,8) in ('20240110','20240111','20240112','20240113','20240114','20240115')
group by left(TRADING_PERIOD_ID,8),BronzeInsertTimestamp,InsertBatchLogID, FileName
order by TRADINGDATE,BronzeInsertTimestamp,InsertBatchLogID, FileName

-- COMMAND ----------

-- DBTITLE 1,Records in coppers but not in gold (Example)
SELECT distinct c.SPDCaseID as CasseID,  c.DispatchInstructionSentDateTime, c.PNodeCode, c.DispatchTypeCode, c.TradingDate, c.TradingPeriodNumber, DispatchedMegawatts, Megawatts
FROM ea_prd.copper.dispatchinstruction c LEFT JOIN  ea_prd.gold.dispatchinstructions g
on (c.SPDCaseID = g.CaseID and c.DispatchInstructionSentDateTime = g.DispatchInstructionSentDateTime
and c.PNodeCode = g.PNodeCode and c.DispatchTypeCode = g.DispatchTypeCode and c.TradingDate = g.TradingDate)
WHERE g.Megawatts is null
--and c.TradingDate = '2024-01-13'-- and c.FileName = '/mnt/eadatalakeprd/datalake/DispatchInstruction/daily_dispatch_20240115.csv'

-- COMMAND ----------

SELECT distinct c.SPDCaseID as CasseID,  c.DispatchInstructionSentDateTime, c.PNodeCode, c.DispatchTypeCode, c.TradingDate, c.TradingPeriodNumber, DispatchedMegawatts, Megawatts
FROM ea_prd.copper.dispatchinstruction c LEFT JOIN  ea_prd.gold.dispatchinstructions g
on (c.SPDCaseID = g.CaseID and c.DispatchInstructionSentDateTime = g.DispatchInstructionSentDateTime
and c.PNodeCode = g.PNodeCode and c.DispatchTypeCode = g.DispatchTypeCode 
and c.TradingDate = g.TradingDate -- and c.TradingPeriodNumber = g.TradingPeriodNumber
)
WHERE g.Megawatts is null
and c.TradingDate between '2023-01-13' and '2024-01-13' -- and c.FileName = '/mnt/eadatalakeprd/datalake/DispatchInstruction/daily_dispatch_20240115.csv'
Order by c.DispatchInstructionSentDateTime, c.PNodeCode, c.DispatchTypeCode

-- COMMAND ----------

SELECT * FROM ea_prd.gold.dispatchinstructions WHERE TradingDate between '2023-01-13' and '2024-01-13' and CaseID = '11012023041440345' and PNodeCode = 'AVI2201 AVI0';

SELECT * FROM ea_prd.gold.dispatchinstructions WHERE TradingDate between '2023-01-13' and '2024-01-13' and CaseID = '271012023010910214' and PNodeCode = 'AVI2201 AVI0';

SELECT * FROM ea_prd.gold.dispatchinstructions WHERE TradingDate between '2023-01-13' and '2024-01-13' and CaseID = '121012024011700227' and PNodeCode = 'OHA2201 OHA0';

SELECT * FROM ea_prd.gold.dispatchinstructions WHERE TradingDate = '2023-01-24' and CaseID = -1 and PNodeCode = 'WTO';


-- COMMAND ----------

SELECT * FROM ea_prd.copper.dispatchinstruction WHERE TradingDate between '2023-01-13' and '2024-01-13' and SPDCaseID = '11012023041440345' and PNodeCode = 'AVI2201 AVI0';

SELECT * FROM ea_prd.copper.dispatchinstruction WHERE TradingDate between '2023-01-13' and '2024-01-13' and SPDCaseID = '271012023010910214' and PNodeCode = 'AVI2201 AVI0';

SELECT * FROM ea_prd.copper.dispatchinstruction WHERE TradingDate between '2023-01-13' and '2024-01-13' and SPDCaseID = '121012024011700227' and PNodeCode = 'OHA2201 OHA0';

SELECT * FROM ea_prd.copper.dispatchinstruction WHERE TradingDate = '2023-01-24' and PNodeCode = 'WTO' and SPDCaseID = -1 --and `FileName` = 'mnt/eadatalakeprd/datalake/DispatchInstruction/daily_dispatch_20230126.csv'
and DispatchTypeCode = 'MW'
Order by DispatchInstructionSentDateTime; 

-- COMMAND ----------

SELECT * FROM ea_prd.bronze.dispatchinstruction WHERE SRC_CASE_ID = '11012023041440345' and DISPATCH_NAME = 'AVI2201 AVI0';
-- SELECT * FROM ea_prd.bronze.dispatchinstruction WHERE SRC_CASE_ID = '271012023010910214' and DISPATCH_NAME = 'AVI2201 AVI0';


-- COMMAND ----------

SELECT c.`FileName`, c.SPDCaseID as CasseID,  c.DispatchInstructionSentDateTime, c.PNodeCode, c.DispatchTypeCode,c.TradingDate, c.TradingPeriodNumber, DispatchedMegawatts, Megawatts, 
c.UpdatedDatetime,g.UpdatedDateTime
FROM ea_prd.copper.dispatchinstruction c LEFT JOIN  ea_prd.gold.dispatchinstructions g
on (c.SPDCaseID = g.CaseID and c.DispatchInstructionSentDateTime = g.DispatchInstructionSentDateTime
and c.PNodeCode = g.PNodeCode and c.DispatchTypeCode = g.DispatchTypeCode and c.TradingDate = g.TradingDate)
WHERE c.TradingDate = '2024-01-14' and c.FileName = '/mnt/eadatalakeprd/datalake/DispatchInstruction/daily_dispatch_20240116.csv' and g.Megawatts is null

-- COMMAND ----------

-- DBTITLE 1,Count daily records from each FK_offers_dispatched file in bronze
select TRADING_PERIOD_START_DATE as TRADINGDATE,BronzeInsertTimestamp,InsertBatchLogID, FileName, count(*) as N from hive_metastore.bronze.fk_offersdispatched
where TRADING_PERIOD_START_DATE in ('10/01/2024','11/01/2024','12/01/2024','13/01/2024','14/01/2024','15/01/2024')
group by TRADING_PERIOD_START_DATE ,BronzeInsertTimestamp,InsertBatchLogID, FileName
order by TRADINGDATE,BronzeInsertTimestamp,InsertBatchLogID, FileName

-- COMMAND ----------

-- DBTITLE 1,Count daily records from each FK_offers_dispatched file in gold
select TradingDate,UpdatedDatetime,InsertBatchLogID, FileName, count(*) as N from hive_metastore.gold.frequencykeepingdispatchedoffers
where TradingDate between '2024-01-10' and '2024-01-15'
group by TradingDate,UpdatedDatetime,InsertBatchLogID, FileName
order by TradingDate,UpdatedDatetime,InsertBatchLogID, FileName

-- COMMAND ----------

-- DBTITLE 1,A closer look at the FK offersdispatched data for trading date 2024-01-13
select TradingDate,TradingPeriodNumber, case when PointOfConnectionCode = 'N/A' then DispatchBlockCode else PointOfConnectionCode || ' ' || UnitCode end as DispatchName,
IslandCode,Megawatts,Dollars,FileName
-- ,* 
from hive_metastore.gold.frequencykeepingdispatchedoffers
where TradingDate = '2024-01-13'

order by TradingDate,TradingPeriodNumber, UpdatedDatetime,InsertBatchLogID, FileName

-- COMMAND ----------

select distinct TradingDate, TradingPeriodNumber, SPDMarketNodeConstraintName, regexp_replace(regexp_replace(SPDMarketNodeConstraintName,'FK_',''),'_CTRLMAX','') as DispatchName
from ea_prd.copper.spd_rtd_mssdata_mndconstraint where TradingDate = '2024-01-13' and SPDMarketNodeConstraintName like '%CTRLMAX'
order by TradingDate, TradingPeriodNumber, SPDMarketNodeConstraintName
