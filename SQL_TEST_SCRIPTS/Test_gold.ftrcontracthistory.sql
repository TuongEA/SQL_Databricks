-- Databricks notebook source
-- DBTITLE 1,Count Test
select count(1) as cnt , "gold" as schemaN from gold.ftrcontracthistory  
union 
select count(*) as cnt , "tableau" as schemaN from tableau.ftr_contractstradinghistory --WHERE FTRTradeDate <> '2022-06-16'

-- COMMAND ----------

-- DBTITLE 1,Cross test sum(FTRAcquisitionCost)
select sum(FTRAcquisitionCost)  as sum1 , "gold" as schemaN from gold.ftrcontracthistory  
union 
select  sum(FTRAcquisitionCost) as sum1, "tableau" as schemaN from tableau.ftr_contractstradinghistory 

-- COMMAND ----------

-- DBTITLE 1,Cross test sum(FTRContractVolumeMegawattHours)
select sum(FTRContractVolumeMegawattHours)  as sum1 , "gold" as schemaN from gold.ftrcontracthistory  
union 
select  sum(FTRContractVolumeMegawattHours) as sum1, "tableau" as schemaN from tableau.ftr_contractstradinghistory  

-- COMMAND ----------

-- DBTITLE 1,Cross test sum(FTRContractDollarsPerMegawattHour)
select sum(FTRContractDollarsPerMegawattHour)  as sum1 , "gold" as schemaN from gold.ftrcontracthistory  
union 
select  sum(FTRContractDollarsPerMegawattHour) as sum1, "tableau" as schemaN from tableau.ftr_contractstradinghistory

-- COMMAND ----------

-- DBTITLE 1,Find bad rows
select * 
from gold.ftrcontracthistory a FULL OUTER JOIN tableau.ftr_contractstradinghistory b 
ON (a.FTRAuctionName = b.FTRAuctionName
and a.FTRContractOwnerParticipantCode = b.FTRContractOwnerCode
and a.FTRTradeType = b.FTRTradeType
and a.FTRTradeDate = b.FTRTradeDate
and a.FTRContractID = b.FTRContractID
and a.FTRContractMegawatts = b.FTRContractMegawatts
and a.FTRContractDollarsPerMegawattHour = b.FTRContractDollarsPerMegawattHour
)
WHERE a.FTRContractID is null or b.FTRContractID is null

-- COMMAND ----------

-- DBTITLE 1,Check data against FTR register
WITH L1 AS (
SELECT FTRContractID, sum(FTRContractMegawatts) as FTRContractMegawatts
FROM tableau.ftr_contractstradinghistory GROUP BY FTRContractID
)
SELECT "ContractTradingHistory" as TableName, count(*) as N, sum(FTRContractMegawatts) as TotalMW FROM  L1
UNION
SELECT "FTR_register" as TableName, count(*) as N, sum(FTRContractMegawatts) as TotalMW FROM processing.copper_ftr_register WHERE 
CopperInsertedDateTime = (SELECT max(CopperInsertedDateTime) FROM processing.copper_ftr_register)

