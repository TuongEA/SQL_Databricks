-- Databricks notebook source
SET TIME ZONE 'Pacific/Auckland';
--SET TIME ZONE 'Etc/UTC';

-- COMMAND ----------

select InsertDateTime, CaseID, CaseTypeCode, FirstPeriodDateTime, RunDateTime, RunDateTime + INTERVAL 5 Minute as TestDateTime from vspd.spdcase 
WHERE CaseTypeCode = 'RTD'
AND FirstPeriodDateTime > RunDateTime + INTERVAL 5 Minute
AND FirstPeriodDateTime > RunDateTime + INTERVAL 1 HOUR  + INTERVAL 5 Minute
--AND InsertDateTime > TimeStamp('2021-01-12 09:59:59')
ORDER BY FirstPeriodDateTime Desc

-- COMMAND ----------

-- -- Are you sure to run this?
-- INSERT INTO vspd.spdcase
-- (
-- SELECT CaseID, CaseName, 
--        'FP' as CaseTypeCode, 
--        StudyModeCode, 
--        'T' as PriceTypeCode, 
--        FirstPeriodDateTime, 
--        FirstPeriodDateTimeNZT, 
--        FirstPeriodDateTimeUTC,
--        RunDateTime, RunDateTimeNZT, RunDateTimeUTC, 
--        'FP_20220622_I.gdx' as GDXFileName, EnergyPriceComparisonFlag, ReservePriceComparisonFlag, 
--        IntervalCostComparisonFlag, DifferenceReason, FileName, 
--        current_timestamp() as InsertDateTime, 
--        date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss.SSS') as InsertDateTimeNZT,
--        date_format(to_utc_timestamp(current_timestamp(), 'Pacific/Auckland') ,'yyyy-MM-dd HH:mm:ss.SSS') as InsertDateTimeUTC
-- from vspd.spdcase 
-- WHERE CaseID in ('211112022061200904')
-- AND CaseTypeCode = 'FP'
-- AND InsertDateTimeUTC = '2022-06-22 22:12:31.362'
-- )


-- COMMAND ----------

-- -- Are you sure to run this?
-- Update
-- vspd.spdcase
-- set
-- DifferenceReason = 'Reserve price difference is caused by multiple reserve price solutions (Cleared FIR is zero)'
-- where
-- FirstPeriodDateTime >= '2022-08-14'
-- and ReservePriceComparisonFlag = 'Failed' 
-- and PriceTypeCode = 'T'
-- and CaseTypeCode = 'FP'
-- and CaseID = '91112022081200157'
-- ;



-- COMMAND ----------

select * from sensitive.spdisland WHERE CaseID = '21112021101100182'

-- COMMAND ----------

-- Are you sure to run this?
-- UPDATE sensitive.spdisland_v3 
-- SET
-- PriceTypeKey = '317987c1-4ceb-4581-8ea4-fb056516e066', 
-- PriceTypeCode = 'F', 
-- PriceType = 'Final'
-- WHERE CaseID = '21112021101100182'


-- COMMAND ----------

-- Are you sure to run this?
-- UPDATE vspd.spdcase 
-- SET 
-- GDXFileName = 'FP_20191207_61112019121100999_20191209140048.gdx', 
-- ReservePriceComparisonFlag = 'Passed', 
-- DifferenceReason = 'Reserve price difference (0.002$/MWh) is caused by multiple reserve price solution situation and the difference is within tolerance (< 0.005)'
-- WHERE CaseID = '301112021091100205'
-- and GDXFileName = 'FP_20211001x_F.gdx' 
-- AND UpdateTimeStampUTC = '2021-09-30 23:31:22.150'


-- COMMAND ----------

-- DELETE FROM vspd.spdcase
select * FROM vspd.spdcase
where CaseTypeCode = 'RTD'
and caseID = '81012023102120920'
and InsertDateTime = '2023-10-09 10:42:30';



-- COMMAND ----------

-- Are you sure to run this?
-- DELETE FROM vspd.spdcase
-- where CaseTypeCode = 'SAD_RTD'
-- AND UpdateTimeStampUTC = '2021-10-06 09:17:13.976'

-- DELETE FROM vspd.spdcase
-- WHERE CaseID in (Select CaseID FROM vspd.spdcase where FileName is not NULL)
-- AND FileName is NULL

-- COMMAND ----------

-- OPTIMIZE vspd.spdcase ZORDER BY CaseTypeCode,CaseID,InsertedDateTime;

-- COMMAND ----------

-- CaseID,
  -- CaseName,
  -- CaseTypeCode,
  -- StudyModeCode,
  -- PriceTypeCode,
  -- FirstPeriodDateTime,
  -- FirstPeriodDateTimeNZT,
  -- FirstPeriodDateTimeUTC,
  -- RunDateTime,
  -- RunDateTimeNZT,
  -- RunDateTimeUTC,
  -- GDXFileName,EnergyPriceComparisonFlag,ReservePriceComparisonFlag,IntervalCostComparisonFlag,DifferenceReason,
  -- FileName,
  -- InsertDateTime,
  -- InsertDateTimeNZT,
  -- InsertDateTimeUTC

-- INSERT INTO vspd.spdcase 
--   Select 
--     '301012023081525839',
--     'Auto RTD 31-AUG-23 03:25 839',
--     'RTD',
--     '101',
--     'D',
--     to_timestamp('2023-08-31 03:25:00', 'yyyy-MM-dd HH:mm:ss'),
--     '2023-08-31 03:25:00',
--     '2023-08-30 15:25:00',
--     to_timestamp('2023-08-31 03:24:01', 'yyyy-MM-dd HH:mm:ss'),
--     '2023-08-31 03:24:01',
--     '2023-08-30 15:24:01',
--     null,null,null,null,null,
--     'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/PRSS/MSS_301012023081525839_0X.ZIP',
--     now(),
--     date_format(now(), 'yyyy-MM-dd HH:mm:ss.SSS'),
--     date_format(to_utc_timestamp(now(),'UTC-12'),'yyyy-MM-dd HH:mm:ss.SSS')
  

-- Are you sure to run this?
  -- UPDATE vspd.spdcase 
  -- SET 
  -- -- FirstPeriodDateTime = to_timestamp('2023-08-30 22:40:00', 'yyyy-MM-dd HH:mm:ss'),
  -- -- FirstPeriodDateTimeNZT = '2023-08-30 22:40:00',
  -- -- -- FirstPeriodDateTimeUTC,
  -- RunDateTime = to_timestamp('2023-08-30 23:39:01', 'yyyy-MM-dd HH:mm:ss'),
  -- RunDateTimeNZT = '2023-08-30 23:39:01',
  -- RunDateTimeUTC = '2023-08-30 11:39:01'
  -- WHERE CaseID = '301012023081140775'



-- COMMAND ----------

-- CaseID
  -- CaseName
  -- CaseTypeCode
  -- CaseType
  -- PriceTypeCode
  -- PriceType
  -- StudyModeCode
  -- FirstPeriodDateTime
  -- RunDateTime
  -- FileName
  -- InsertDateTime
  -- InsertedBy

-- INSERT INTO silver.dimspdcase 
--   Select  
--     '301012023081525839',
--     'Auto RTD 31-AUG-23 03:25 839',
--     'RTD',
--     'Real-time dispatch',
--     'D',
--     'Real-time dispatch prices',
--     '101',
--     to_timestamp('2023-08-31 03:25:00', 'yyyy-MM-dd HH:mm:ss'),
--     to_timestamp('2023-08-31 03:24:01', 'yyyy-MM-dd HH:mm:ss'),
--     'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/PRSS/MSS_301012023081525839_0X.ZIP',
--     now(),
--     'Manual by Tuong' 

-- UPDATE silver.dimspdcase  
-- SET 
-- -- FirstPeriodDateTime = to_timestamp('2023-08-30 22:40:00', 'yyyy-MM-dd HH:mm:ss'),
-- RunDateTime = to_timestamp('2023-08-30 23:39:01', 'yyyy-MM-dd HH:mm:ss')
-- WHERE CaseID = '301012023081140775'



-- COMMAND ----------

select * from silver.dimspdcase where CaseTypeCode = 'RTD' and FileName like '%PRSS%'

-- COMMAND ----------

-- UPDATE silver.dimspdcase  
-- SET FileName = CASE
--   WHEN FileName = 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/PRSS/MSS_301012023081525839_0X.ZIP' 
--              THEN 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/RTD/MSS_301012023081525839_0X.ZIP'

--   WHEN FileName = 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/PRSS/MSS_301012023081515837_0X.ZIP' 
--              THEN 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/RTD/MSS_301012023081515837_0X.ZIP'
  
--   WHEN FileName = 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/PRSS/MSS_301012023081340810_0X.ZIP' 
--              THEN 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/RTD/MSS_301012023081340810_0X.ZIP'

--   WHEN FileName = 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/PRSS/MSS_301012023081330805_0X.ZIP' 
--              THEN 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/RTD/MSS_301012023081330805_0X.ZIP'

--   WHEN FileName = 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/PRSS/MSS_301012023081205782_0X.ZIP' 
--              THEN 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/RTD/MSS_301012023081205782_0X.ZIP'

--   WHEN FileName = 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/PRSS/MSS_301012023081140775_0X.ZIP' 
--              THEN 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/RTD/MSS_301012023081140775_0X.ZIP'

--   WHEN FileName = 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/PRSS/MSS_301012023081130771_0X.ZIP' 
--              THEN 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/RTD/MSS_301012023081130771_0X.ZIP'

--   WHEN FileName = 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/PRSS/MSS_301012023081105764_0X.ZIP' 
--              THEN 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/RTD/MSS_301012023081105764_0X.ZIP'

--   WHEN FileName = 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/PRSS/MSS_301012023081040755_0X.ZIP' 
--              THEN 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/RTD/MSS_301012023081040755_0X.ZIP'
--   END
-- WHERE CaseTypeCode = 'RTD' and FileName like '%PRSS%';




-- COMMAND ----------

-- UPDATE vspd.spdcase  
-- SET FileName = CASE
--   WHEN FileName = 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/PRSS/MSS_301012023081525839_0X.ZIP' 
--              THEN 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/RTD/MSS_301012023081525839_0X.ZIP'

--   WHEN FileName = 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/PRSS/MSS_301012023081515837_0X.ZIP' 
--              THEN 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/RTD/MSS_301012023081515837_0X.ZIP'
  
--   WHEN FileName = 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/PRSS/MSS_301012023081340810_0X.ZIP' 
--              THEN 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/RTD/MSS_301012023081340810_0X.ZIP'

--   WHEN FileName = 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/PRSS/MSS_301012023081330805_0X.ZIP' 
--              THEN 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/RTD/MSS_301012023081330805_0X.ZIP'

--   WHEN FileName = 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/PRSS/MSS_301012023081205782_0X.ZIP' 
--              THEN 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/RTD/MSS_301012023081205782_0X.ZIP'

--   WHEN FileName = 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/PRSS/MSS_301012023081140775_0X.ZIP' 
--              THEN 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/RTD/MSS_301012023081140775_0X.ZIP'

--   WHEN FileName = 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/PRSS/MSS_301012023081130771_0X.ZIP' 
--              THEN 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/RTD/MSS_301012023081130771_0X.ZIP'

--   WHEN FileName = 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/PRSS/MSS_301012023081105764_0X.ZIP' 
--              THEN 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/RTD/MSS_301012023081105764_0X.ZIP'

--   WHEN FileName = 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/PRSS/MSS_301012023081040755_0X.ZIP' 
--              THEN 'abfss://casefiles@madatasourcesaprd.dfs.core.windows.net/processed/RTD/MSS_301012023081040755_0X.ZIP'
--   END
-- WHERE CaseTypeCode = 'RTD' and FileName like '%PRSS%';



