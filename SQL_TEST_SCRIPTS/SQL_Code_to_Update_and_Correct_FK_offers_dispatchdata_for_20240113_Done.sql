-- Databricks notebook source
-- MAGIC %md #Description
-- MAGIC SO messed up with FK offer dispatch data for traing date 20240112 and 20240113. While waiting for a response from SO, we use the data from SPD market node conttraint to (and also checked against the FK indication data from WITS) to deined exactly which FK offers tranches were dispatch for these two trading date. The code be low manualy updates the **hive_metastore.gold.frequencykeepingdispatchedoffers ** to correct data

-- COMMAND ----------

SET TIME ZONE 'Pacific/Auckland';

-- COMMAND ----------

-- MAGIC %md #Trading date 2024-01-13
-- MAGIC 1. Remove the records from FileName "/mnt/eadatalakeprd/datalake/FK_OffersDispatched/FK_offers_dispatched_20240114.csv".
-- MAGIC 2. Add two records for TP1 and TP2 (one each)

-- COMMAND ----------

-- DBTITLE 1,Remove the records from FileName "/mnt/eadatalakeprd/datalake/FK_OffersDispatched/FK_offers_dispatched_20240114.csv".
DELETE FROM hive_metastore.gold.frequencykeepingdispatchedoffers 
where TradingDate = '2024-01-13' and FileName = '/mnt/eadatalakeprd/datalake/FK_OffersDispatched/FK_offers_dispatched_20240114.csv'

-- COMMAND ----------

-- DBTITLE 1,Add two records (WTR) for TP1 and TP2 (one each)
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

-- DBTITLE 1,Add two records (WTO) for TP1 and TP2 (one each)
with L1 as (
select 'N/A' as `FileName`,
'Tuong_manually_update' || date_format(now(), 'yyyyMMddHHmm') InsertBatchLogID,
current_timestamp() as UpdatedDatetime,
TradingDate,TradingDateYear,TradingDateMonthNumber,TradingDateWeekOfYearNumber,TradingDateWeekdayName,DaylightSavingIndicator,
1 as TradingPeriodNumber,
'00:00' as TradingPeriodStartTime,
'2024-01-13 00:00:00' as IntervalDateTime,
'2024-01-12 21:39:04' as SubmissionDateTime,
'2024-01-12' as SubmissionDate,
'21:39:04' as SubmissionTime,
PlantName,PlantCapacityMegawatts,TechnologyCode,Technology,FuelCode,Fuel,PointOfConnectionCode,UnitCode,DispatchBlockCode,DispatchBlock,DispatchBlockType,SiteCode,`Site`,GridZoneCode,GridZone,ZoneCode,`Zone`,IslandCode,Island,Northing,Easting,Latitude,Longitude,ParticipantCode,ParticipantShortName,ParticipantLongName,Megawatts
, 135 as Dollars
from hive_metastore.gold.frequencykeepingdispatchedoffers 
where TradingDate = '2024-01-13' and TradingPeriodNumber = 3 and DispatchBlockCode = 'WTO'
union
select 'N/A' as `FileName`,
'Tuong_manually_update' || date_format(now(), 'yyyyMMddHHmm') InsertBatchLogID,
current_timestamp() as UpdatedDatetime,
TradingDate,TradingDateYear,TradingDateMonthNumber,TradingDateWeekOfYearNumber,TradingDateWeekdayName,DaylightSavingIndicator,
2 as TradingPeriodNumber,
'00:30' as TradingPeriodStartTime,
'2024-01-13 00:30:00' as IntervalDateTime,
'2024-01-12 21:39:04' as SubmissionDateTime,
'2024-01-12' as SubmissionDate,
'21:39:04' as SubmissionTime,
PlantName,PlantCapacityMegawatts,TechnologyCode,Technology,FuelCode,Fuel,PointOfConnectionCode,UnitCode,DispatchBlockCode,DispatchBlock,DispatchBlockType,SiteCode,`Site`,GridZoneCode,GridZone,ZoneCode,`Zone`,IslandCode,Island,Northing,Easting,Latitude,Longitude,ParticipantCode,ParticipantShortName,ParticipantLongName,Megawatts
, 140 as Dollars
 from hive_metastore.gold.frequencykeepingdispatchedoffers 
where TradingDate = '2024-01-13' and TradingPeriodNumber = 3 and DispatchBlockCode = 'WTO'
)

-- INSERT INTO hive_metastore.gold.frequencykeepingdispatchedoffers (`FileName`,InsertBatchLogID,UpdatedDatetime,TradingDate,TradingDateYear,TradingDateMonthNumber,TradingDateWeekOfYearNumber,TradingDateWeekdayName,DaylightSavingIndicator,TradingPeriodNumber,TradingPeriodStartTime,IntervalDateTime,SubmissionDateTime,SubmissionDate,SubmissionTime,PlantName,PlantCapacityMegawatts,TechnologyCode,Technology,FuelCode,Fuel,PointOfConnectionCode,UnitCode,DispatchBlockCode,DispatchBlock,DispatchBlockType,SiteCode,`Site`,GridZoneCode,GridZone,ZoneCode,`Zone`,IslandCode,Island,Northing,Easting,Latitude,Longitude,ParticipantCode,ParticipantShortName,ParticipantLongName,Megawatts,Dollars)

(select `FileName`,InsertBatchLogID,UpdatedDatetime,TradingDate,TradingDateYear,TradingDateMonthNumber,TradingDateWeekOfYearNumber,TradingDateWeekdayName,DaylightSavingIndicator,TradingPeriodNumber,TradingPeriodStartTime,IntervalDateTime,SubmissionDateTime,SubmissionDate,SubmissionTime,PlantName,PlantCapacityMegawatts,TechnologyCode,Technology,FuelCode,Fuel,PointOfConnectionCode,UnitCode,DispatchBlockCode,DispatchBlock,DispatchBlockType,SiteCode,`Site`,GridZoneCode,GridZone,ZoneCode,`Zone`,IslandCode,Island,Northing,Easting,Latitude,Longitude,ParticipantCode,ParticipantShortName,ParticipantLongName,Megawatts,Dollars from L1
)

-- COMMAND ----------

select * FROM hive_metastore.gold.frequencykeepingdispatchedoffers where TradingDate = '2024-01-13' and TradingPeriodNumber <= 2 order by TradingPeriodNumber

-- COMMAND ----------



-- COMMAND ----------

select * FROM hive_metastore.gold.frequencykeepingdispatchedoffers where TradingDate = '2024-01-13' and TradingPeriodNumber <=2
