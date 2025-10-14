-- Databricks notebook source
SET TIME ZONE 'Pacific/Auckland';

-- COMMAND ----------

-- DBTITLE 1,Count daily records from each FK_offers_dispatched file in bronze
select TRADING_PERIOD_START_DATE as TRADINGDATE,BronzeInsertTimestamp,InsertBatchLogID, FileName, count(*) as N from hive_metastore.bronze.fk_offersdispatched
--where TRADING_PERIOD_START_DATE in ('10/01/2024','11/01/2024','12/01/2024','13/01/2024','14/01/2024','15/01/2024')
group by TRADING_PERIOD_START_DATE ,BronzeInsertTimestamp,InsertBatchLogID, FileName
having count(*) < 96 or count(*) > 100
order by TRADINGDATE,BronzeInsertTimestamp,InsertBatchLogID, FileName

-- COMMAND ----------

-- DBTITLE 1,Count daily records from each FK_offers_dispatched file in gold
select TradingDate,UpdatedDatetime,InsertBatchLogID, FileName, count(*) as N from hive_metastore.gold.frequencykeepingdispatchedoffers
where TradingDate between '2024-01-10' and '2024-01-15'
group by TradingDate,UpdatedDatetime,InsertBatchLogID, FileName
order by TradingDate,UpdatedDatetime,InsertBatchLogID, FileName

-- COMMAND ----------

select * from hive_metastore.copper.fk_offersdispatched where TradingDate = '2024-01-12' order by TradingDate, TradingPeriodNumber, UpdatedDatetime

-- COMMAND ----------

-- DBTITLE 1,A closer look at the FK offersdispatched data for trading date 2024-01-13
select TradingDate,TradingPeriodNumber, case when PointOfConnectionCode = 'N/A' then DispatchBlockCode else PointOfConnectionCode || ' ' || UnitCode end as DispatchName,
IslandCode,Megawatts,Dollars,FileName
-- ,* 
from hive_metastore.gold.frequencykeepingdispatchedoffers
where TradingDate = '2024-01-13' and FileName = 

order by TradingDate,TradingPeriodNumber, UpdatedDatetime,InsertBatchLogID, FileName

-- COMMAND ----------

select distinct TradingDate, TradingPeriodNumber, SPDMarketNodeConstraintName, regexp_replace(regexp_replace(SPDMarketNodeConstraintName,'FK_',''),'_CTRLMAX','') as DispatchName
from ea_prd.silver.spd_mssdata_mndconstraint where TradingDate = '2024-01-13' and SPDMarketNodeConstraintName like '%CTRLMAX' and CaseTypeCode = 'RTD'
order by TradingDate, TradingPeriodNumber, SPDMarketNodeConstraintName

-- COMMAND ----------

With L1 (select TradingDate, TradingPeriodNumber, PointOfConnectionCode, UnitCode, DispatchBlockCode, Tranche, max(SubmissionDateTime) as SubmissionDateTime
from hive_metastore.copper.fk_offersall where TradingDate = '2024-01-12' and TradingPeriodNumber = 38 and DispatchBlockCode in ('CLU', 'WTR','WTO')
group by all)
select distinct a.TradingDate, a.TradingPeriodNumber, case when a.PointOfConnectionCode = 'N/A' then a.DispatchBlockCode else a.PointOfConnectionCode || ' ' || a.UnitCode end as DispatchName, a.SubmissionDateTime, a.Tranche, Megawatts,Dollars,MaximumMegawatts,MinimumMegawatts
from hive_metastore.copper.fk_offersall a inner join L1 b
on (a.TradingDate = b.TradingDate and a.TradingPeriodNumber = b.TradingPeriodNumber and a.Tranche = b.Tranche and a.SubmissionDateTime = b.SubmissionDateTime
and a.PointOfConnectionCode = b.PointOfConnectionCode and a.UnitCode = b.UnitCode and  a.DispatchBlockCode = b.DispatchBlockCode) 
--where Megawatts = 15
order by  TradingDate, TradingPeriodNumber, DispatchName, Tranche

-- COMMAND ----------

select * from hive_metastore.gold.frequencykeepingdispatchedoffers where TradingDate = '2024-01-13'

-- COMMAND ----------

-- DBTITLE 1,Mapping FK dispatched  to latest FK offers data and check for discrepancies
With L1 as (
  select TradingDate, TradingPeriodNumber, max(UpdatedDatetime) as UpdatedDatetime 
  from hive_metastore.copper.fk_offersdispatched 
  where year(TradingDate) = 2020 and TradingDate not in ('2024-01-12', '2024-01-13')
  group by TradingDate, TradingPeriodNumber
),
fkdispatch as (
select distinct a.* from hive_metastore.copper.fk_offersdispatched a inner join L1 
on a.TradingDate = L1.TradingDate and a.TradingPeriodNumber = L1.TradingPeriodNumber and a.UpdatedDatetime = L1.UpdatedDatetime 
),
L2 (
  select TradingDate, TradingPeriodNumber, PointOfConnectionCode,UnitCode,DispatchBlockCode,Tranche, max(SubmissionDateTime) as SubmissionDateTime
  from hive_metastore.copper.fk_offersall 
  where year(TradingDate) = 2020 and TradingDate not in ('2024-01-12', '2024-01-13')
  group by TradingDate, TradingPeriodNumber, PointOfConnectionCode,UnitCode,DispatchBlockCode,Tranche
), fkoffers as(
  select distinct a.* from hive_metastore.copper.fk_offersall a inner join L2
  on (a.TradingDate = L2.TradingDate and a.TradingPeriodNumber = L2.TradingPeriodNumber
  and a.PointOfConnectionCode = L2.PointOfConnectionCode and a.UnitCode = L2.UnitCode 
  and a.DispatchBlockCode = L2.DispatchBlockCode and a.Tranche = L2.Tranche 
  and a.SubmissionDateTime = L2.SubmissionDateTime
  )
)
select distinct a.TradingDate, a.TradingPeriodNumber, a.SubmissionDateTime, a.PointOfConnectionCode, a.UnitCode, a.DispatchBlockCode,
a.ParticipantCode,a.IslandCode,a.Megawatts,a.Dollars, b.Tranche, b.MinimumMegawatts,b.MaximumMegawatts,b.Megawatts as OfferedMW,b.Dollars as OfferedPrice
from fkdispatch a left join fkoffers b
on (a.TradingDate = b.TradingDate and a.TradingPeriodNumber = b.TradingPeriodNumber 
and a.SubmissionDateTime = b.SubmissionDateTime and a.PointOfConnectionCode = b.PointOfConnectionCode
and a.UnitCode = b.UnitCode and a.DispatchBlockCode = b.DispatchBlockCode
and a.ParticipantCode = b.ParticipantCode and a.Megawatts = b.Megawatts and a.Dollars = b.Dollars)
where b.Megawatts is null
Order by TradingDate desc, TradingPeriodNumber, IslandCode, PointOfConnectionCode, UnitCode, DispatchBlockCode


-- COMMAND ----------

SET TIME ZONE 'Pacific/Auckland';
select 
  FileName, TRADING_PERIOD_START_DATE as TRADINGDATE,TRADING_PERIOD as TRADINGPERIOD, 
  TO_TIMESTAMP(SUBMISSION_DATE, 'dd/MM/yyyy HH:mm:ss') as SUBMISSION_DATETIME,
  PNODE_NAME,TRADER_NAME,SRC_OFFER_BLOCK_ID as TRANCHE,MW,PRICE,MIN_MW,MAX_MW,
  RANK() OVER  (PARTITION BY TRADING_PERIOD_START_DATE,TRADING_PERIOD,PNODE_NAME,TRADER_NAME,SRC_OFFER_BLOCK_ID ORDER BY TO_TIMESTAMP(SUBMISSION_DATE, 'dd/MM/yyyy HH:mm:ss') DESC) AS ORD
from hive_metastore.bronze.fk_offersall 
-- where TRADING_PERIOD_START_DATE = '01/01/2020' and TRADING_PERIOD = '1' and PNODE_NAME = 'WTO'
where ( TRADING_PERIOD_START_DATE = '31/10/2023' and TRADING_PERIOD = '5' and PNODE_NAME = 'TKU2201 TKU0' and Filename = '/mnt/eadatalakeprd/datalake/FK_OffersAll/FK_offers_20231102.csv' )
or ( TRADING_PERIOD_START_DATE = '02/03/2024' and TRADING_PERIOD = '43' and PNODE_NAME = 'HLY2201 HLY5' and Filename = '/mnt/eadatalakeprd/datalake/FK_OffersAll/FK_offers_20240304.csv' )
Order by ORD, TRADINGDATE, TRADINGPERIOD, PNODE_NAME, TRANCHE, SUBMISSION_DATETIME desc

-- COMMAND ----------

select * from hive_metastore.bronze.fk_offersdispatched 
-- where TRADING_PERIOD_START_DATE = '01/01/2020' and TRADING_PERIOD = '1' --and PNODE_NAME = 'WTO'
where (TRADING_PERIOD_START_DATE = '31/10/2023' and TRADING_PERIOD = '5' and PNODE_NAME = 'TKU2201 TKU0')
or (TRADING_PERIOD_START_DATE = '02/03/2024' and TRADING_PERIOD = '43' and PNODE_NAME = 'HLY2201 HLY5')
-- where TRADING_PERIOD_START_DATE = '15/12/2020' --and TRADING_PERIOD = '1' --and PNODE_NAME = 'WTO'

-- COMMAND ----------

select * from ea_prd.silver.spd_mssdata_mndconstraint where UpdatedDate = current_date() and CaseTypeCode = 'RTD' order by CaseTypeCode, IntervalDateTime desc
