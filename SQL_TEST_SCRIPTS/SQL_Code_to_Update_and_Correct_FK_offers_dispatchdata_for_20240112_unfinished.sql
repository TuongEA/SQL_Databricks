-- Databricks notebook source
-- MAGIC %md #Description
-- MAGIC SO messed up with FK offer dispatch data for traing date 20240112 and 20240113. While waiting for a response from SO, we use the data from SPD market node conttraint to (and also checked against the FK indication data from WITS) to deined exactly which FK offers tranches were dispatch for these two trading date. The code be low manualy updates the **hive_metastore.gold.frequencykeepingdispatchedoffers ** to correct data

-- COMMAND ----------

SET TIME ZONE 'Pacific/Auckland';

-- COMMAND ----------

-- MAGIC %md #Trading date 2024-01-12
-- MAGIC 1. Remove the records from FileName "/mnt/eadatalakeprd/datalake/FK_OffersDispatched/FK_offers_dispatched_20240114.csv".
-- MAGIC 2. Add two records for TP1 and TP2 (one each)

-- COMMAND ----------

-- DBTITLE 1,Add two records for TP1 and TP2 (one each)
with L1 as (
select 'N/A' as `FileName`,
'Tuong_manually_update' || date_format(now(), 'yyyyMMddHHmm') InsertBatchLogID,
current_timestamp() as UpdatedDatetime,
TradingDate,TradingDateYear,TradingDateMonthNumber,TradingDateWeekOfYearNumber,TradingDateWeekdayName,DaylightSavingIndicator,
1 as TradingPeriodNumber,
'00:00' as TradingPeriodStartTime,
'2024-01-13 00:00:00' as IntervalDateTime,
'2024-01-12 05:21:10' as SubmissionDateTime,
'2024-01-12' as SubmissionDate,
'05:21:10' as SubmissionTime,
PlantName,PlantCapacityMegawatts,TechnologyCode,Technology,FuelCode,Fuel,PointOfConnectionCode,UnitCode,DispatchBlockCode,DispatchBlock,DispatchBlockType,SiteCode,`Site`,GridZoneCode,GridZone,ZoneCode,`Zone`,IslandCode,Island,Northing,Easting,Latitude,Longitude,ParticipantCode,ParticipantShortName,ParticipantLongName,Megawatts,Dollars
from hive_metastore.gold.frequencykeepingdispatchedoffers 
where TradingDate = '2024-01-13' and TradingPeriodNumber = 3 and DispatchBlockCode = 'WTR'
union
select 'N/A' as `FileName`,
'Tuong_manually_update' || date_format(now(), 'yyyyMMddHHmm') InsertBatchLogID,
current_timestamp() as UpdatedDatetime,
TradingDate,TradingDateYear,TradingDateMonthNumber,TradingDateWeekOfYearNumber,TradingDateWeekdayName,DaylightSavingIndicator,
2 as TradingPeriodNumber,
'00:30' as TradingPeriodStartTime,
'2024-01-13 00:30:00' as IntervalDateTime,
'2024-01-12 05:21:10' as SubmissionDateTime,
'2024-01-12' as SubmissionDate,
'05:21:10' as SubmissionTime,
PlantName,PlantCapacityMegawatts,TechnologyCode,Technology,FuelCode,Fuel,PointOfConnectionCode,UnitCode,DispatchBlockCode,DispatchBlock,DispatchBlockType,SiteCode,`Site`,GridZoneCode,GridZone,ZoneCode,`Zone`,IslandCode,Island,Northing,Easting,Latitude,Longitude,ParticipantCode,ParticipantShortName,ParticipantLongName,Megawatts,Dollars
 from hive_metastore.gold.frequencykeepingdispatchedoffers 
where TradingDate = '2024-01-13' and TradingPeriodNumber = 3 and DispatchBlockCode = 'WTR'
)

-- INSERT INTO hive_metastore.gold.frequencykeepingdispatchedoffers (`FileName`,InsertBatchLogID,UpdatedDatetime,TradingDate,TradingDateYear,TradingDateMonthNumber,TradingDateWeekOfYearNumber,TradingDateWeekdayName,DaylightSavingIndicator,TradingPeriodNumber,TradingPeriodStartTime,IntervalDateTime,SubmissionDateTime,SubmissionDate,SubmissionTime,PlantName,PlantCapacityMegawatts,TechnologyCode,Technology,FuelCode,Fuel,PointOfConnectionCode,UnitCode,DispatchBlockCode,DispatchBlock,DispatchBlockType,SiteCode,`Site`,GridZoneCode,GridZone,ZoneCode,`Zone`,IslandCode,Island,Northing,Easting,Latitude,Longitude,ParticipantCode,ParticipantShortName,ParticipantLongName,Megawatts,Dollars)

(select `FileName`,InsertBatchLogID,UpdatedDatetime,TradingDate,TradingDateYear,TradingDateMonthNumber,TradingDateWeekOfYearNumber,TradingDateWeekdayName,DaylightSavingIndicator,TradingPeriodNumber,TradingPeriodStartTime,IntervalDateTime,SubmissionDateTime,SubmissionDate,SubmissionTime,PlantName,PlantCapacityMegawatts,TechnologyCode,Technology,FuelCode,Fuel,PointOfConnectionCode,UnitCode,DispatchBlockCode,DispatchBlock,DispatchBlockType,SiteCode,`Site`,GridZoneCode,GridZone,ZoneCode,`Zone`,IslandCode,Island,Northing,Easting,Latitude,Longitude,ParticipantCode,ParticipantShortName,ParticipantLongName,Megawatts,Dollars from L1
)

-- COMMAND ----------

select * FROM hive_metastore.gold.frequencykeepingdispatchedoffers where TradingDate = '2024-01-12' order by TradingPeriodNumber

-- COMMAND ----------

with L1 as (select TradingDate, TradingPeriodNumber, PointOfConnectionCode,UnitCode,DispatchBlockCode, 
case when PointOfConnectionCode = 'N/A' then DispatchBlockCode else PointOfConnectionCode || ' ' || UnitCode end as DispatchName,
Tranche, max(SubmissionDateTime) as SubmissionDateTime
from gold.frequencykeepingoffers where TradingDate = '2024-01-12'
group by TradingDate, TradingPeriodNumber, PointOfConnectionCode,UnitCode,DispatchBlockCode, Tranche)
, L2 as (select distinct TradingDate, TradingPeriodNumber, SPDMarketNodeConstraintName, 
regexp_replace(regexp_replace(SPDMarketNodeConstraintName,'FK_',''),'_CTRLMAX','') as DispatchName
from ea_prd.copper.spd_rtd_mssdata_mndconstraint where TradingDate = '2024-01-12' and SPDMarketNodeConstraintName like '%CTRLMAX'
)
select  a.TradingDate, a.TradingPeriodNumber, SPDMarketNodeConstraintName, b.DispatchName, a.PointOfConnectionCode,a.UnitCode,a.DispatchBlockCode, a.Tranche, a.Megawatts, a.Dollars, a.SubmissionDateTime
from gold.frequencykeepingoffers a inner join L1 b
on (a.TradingDate = b.TradingDate and a.TradingPeriodNumber = b.TradingPeriodNumber and a.PointOfConnectionCode = b.PointOfConnectionCode
and a.UnitCode = b.UnitCode and a.DispatchBlockCode = b.DispatchBlockCode and a.Tranche = b.Tranche and a.SubmissionDateTime = b.SubmissionDateTime)
inner join L2 c on (a.TradingDate = c.TradingDate and a.TradingPeriodNumber = c.TradingPeriodNumber and b.DispatchName = c.DispatchName)
where a.Megawatts = 15

-- COMMAND ----------

select TradingDate, TradingPeriodNumber, PointOfConnectionCode,UnitCode,DispatchBlockCode, 
case when PointOfConnectionCode = 'N/A' then DispatchBlockCode else PointOfConnectionCode || ' ' || UnitCode end as DispatchName,
Tranche, max(SubmissionDateTime) as SubmissionDateTime
from gold.frequencykeepingoffers where TradingDate = '2024-01-12' and Megawatts = 15 and TradingPeriodNumber = 20
group by TradingDate, TradingPeriodNumber, PointOfConnectionCode,UnitCode,DispatchBlockCode, Tranche

-- COMMAND ----------

select  a.TradingDate, a.TradingPeriodNumber, a.PointOfConnectionCode,a.UnitCode,a.DispatchBlockCode, a.Tranche, a.Megawatts, a.Dollars, a.SubmissionDateTime, a.UpdatedDatetime
from copper.fk_offersall a
where TradingDate = '2024-01-12' and TradingPeriodNumber = 20 and PointOfConnectionCode = 'TKU2201' and UnitCode = 'TKU0' --and a.SubmissionDateTime = '2024-01-05 13:23:32'

-- COMMAND ----------

SET TIME ZONE 'Pacific/Auckland';
select  a.TradingDate, a.TradingPeriodNumber, a.PointOfConnectionCode,a.UnitCode,a.DispatchBlockCode, a.Tranche, a.Megawatts, a.Dollars, a.SubmissionDateTime, a.SubmissionDate, a.SubmissionTime
from gold.frequencykeepingoffers a
where TradingDate = '2024-01-12' and TradingPeriodNumber = 25 --and a.Megawatts = 15 
and a.DispatchBlockCode = 'CLU'
--  and PointOfConnectionCode = 'TKU2201' and UnitCode = 'TKU0' --and a.SubmissionDateTime = '2024-01-05 13:23:32'
order by a.SubmissionDateTime desc


-- COMMAND ----------

select  *
from bronze.fk_offersall
where TRADING_PERIOD_START_DATE = '12/01/2024' and TRADING_PERIOD = 25 --and a.Megawatts = 15 
and PNODE_NAME = 'CLU'
-- --  and PointOfConnectionCode = 'TKU2201' and UnitCode = 'TKU0' --and a.SubmissionDateTime = '2024-01-05 13:23:32'
-- order by a.SubmissionDateTime desc

-- COMMAND ----------

with L1 as (select TradingDate, TradingPeriodNumber, PointOfConnectionCode,UnitCode,DispatchBlockCode, 
-- case when PointOfConnectionCode = 'N/A' then DispatchBlockCode else PointOfConnectionCode || ' ' || UnitCode end as DispatchName,
Tranche, max(SubmissionDateTime) as SubmissionDateTime
from gold.frequencykeepingoffers where TradingDate = '2024-01-13'
group by TradingDate, TradingPeriodNumber, PointOfConnectionCode,UnitCode,DispatchBlockCode, Tranche)
-- , L2 as (select distinct TradingDate, TradingPeriodNumber, SPDMarketNodeConstraintName, 
-- regexp_replace(regexp_replace(SPDMarketNodeConstraintName,'FK_',''),'_CTRLMAX','') as DispatchName
-- from ea_prd.copper.spd_rtd_mssdata_mndconstraint where TradingDate = '2024-01-12' and SPDMarketNodeConstraintName like '%CTRLMAX'
-- )
select  a.TradingDate, a.TradingPeriodNumber,  a.PointOfConnectionCode,a.UnitCode,a.DispatchBlockCode, a.Tranche, a.Megawatts, a.Dollars, a.SubmissionDateTime
from gold.frequencykeepingoffers a right outer join L1 b
on (a.TradingDate = b.TradingDate and a.TradingPeriodNumber = b.TradingPeriodNumber and a.PointOfConnectionCode = b.PointOfConnectionCode
and a.UnitCode = b.UnitCode and a.DispatchBlockCode = b.DispatchBlockCode and a.Tranche = b.Tranche and a.SubmissionDateTime = b.SubmissionDateTime)

where b.TradingPeriodNumber = 1 and a.Megawatts = 15

-- COMMAND ----------

with L1 as (select distinct TradingDate, TradingPeriodNumber, SPDMarketNodeConstraintName, regexp_replace(regexp_replace(SPDMarketNodeConstraintName,'FK_',''),'_CTRLMAX','') as DispatchName
from ea_prd.copper.spd_rtd_mssdata_mndconstraint where TradingDate = '2024-01-12' and SPDMarketNodeConstraintName like '%CTRLMAX'
)
select * from L1
where TradingPeriodNumber = 20
order by TradingDate, TradingPeriodNumber, SPDMarketNodeConstraintName

-- COMMAND ----------

select * from gold.frequencykeepingoffers where TradingDate = '2024-01-12' and TradingPeriodNumber = 20 and Megawatts = 15
