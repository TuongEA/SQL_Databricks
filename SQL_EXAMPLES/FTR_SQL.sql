-- Databricks notebook source
-- DBTITLE 1,Monthly Rental
SELECT
* 
FROM ea_dev.gold.factftrrental
ORDER BY FTRContractStartDate

-- COMMAND ----------

-- DBTITLE 1,FTR Trading profit from reselling
WITH L1 AS (
SELECT FTRContractOwnerParticipantShortName,FTRContractID,FTRContractHours,
FTRContractMegawatts as SoldMW,
FTRContractDollarsPerMegawattHour as SoldPrice,
FTRContractVolumeMegawattHours as SoldVolume,
FTRAcquisitionCost as SoldValue
FROM gold.ftrcontracthistory 
WHERE FTRTradeType = 'Sell'
-- AND FTRContractOwnerParticipantShortName = 'Jarden Securities Ltd'
AND Year(FTRTradeDate) = 2024
), L2 AS (
SELECT b.*, a.FTRContractMegawatts as BoughtMW,
a.FTRContractDollarsPerMegawattHour as BoughtPrice,
a.FTRContractVolumeMegawattHours as BoughtVolume,
a.FTRAcquisitionCost as BoughtValue
FROM  gold.ftrcontracthistory a INNER JOIN L1 b
ON (a.FTRContractOwnerParticipantShortName = b.FTRContractOwnerParticipantShortName
AND a.FTRContractID = b.FTRContractID
AND a.FTRContractHours = b.FTRContractHours)
WHERE a.FTRTradeType = 'Buy')
SELECT FTRContractOwnerParticipantShortName, Sum(-SoldMW * FTRContractHours * (SoldPrice - BoughtPrice)) as Profit
FROM L2
GROUP BY FTRContractOwnerParticipantShortName
ORDER BY Profit desc



-- COMMAND ----------

-- DBTITLE 1,FTR Trader's monthly profit
WITh L1 AS (
SELECT FTRContractOwnerParticipantShortName,
FTRContractID,
SourcePointOfConnectionCode as FTRSourceNode,
SinkPointOfConnectionCode as FTRSinkNode,
FTRContractStartDate,
Case
  When FTRContractTypeCode = 'OBL' Then 'Obligation'
  Else 'Option'
End as FTRContractType,
FTRContractHours,
FTRContractMegawatts,
FTRContractDollarsPerMegawattHour,
FTRContractVolumeMegawattHours,
FTRAcquisitionCost
FROM gold.ftrcontracthistory a
), L2 AS (
SELECT distinct a.* , FTRSettlementDollarsPerMegawattHour, Payment_Scaling_Factor
FROM L1 a INNER JOIN gold.ftr_settlement_prices b
ON (a.FTRSourceNode = b.FTRSourceNode
AND a.FTRSinkNode = b.FTRSinkNode
AND a.FTRContractType = b.FTRContractType
AND a.FTRContractStartDate = b.FTRContractStartDate)
INNER JOIN gold.ftr_monthly_rental c
ON a.FTRContractStartDate = c.Start_Date
)
SELECT FTRContractOwnerParticipantShortName, FTRContractStartDate, 
Sum( (Payment_Scaling_Factor*FTRSettlementDollarsPerMegawattHour - FTRContractDollarsPerMegawattHour) * FTRContractVolumeMegawattHours) as Profit
FROM L2
WHERE Year(FTRContractStartDate) = 2024
GROUP BY FTRContractOwnerParticipantShortName, FTRContractStartDate
ORDER BY FTRContractStartDate, Profit desc

-- COMMAND ----------

-- DBTITLE 1,Get FTR total cash flow by financial year
select 
  CASE 
    WHEN month(Start_Date) >= 7 then cast(year(Start_Date) as STRING) || '/' || cast(year(Start_Date)+1 as STRING) 
    ELSE cast(year(Start_Date)-1 as STRING) || '/' || cast(year(Start_Date) as STRING) 
  END as FinancialYear, 
  min(Start_Date) as StartDate, max(End_Date) as EndDate, 
  round(sum(FTR_Auction_Revenue_Dollars)/1e6,3) as Total_Auction_Revenue_Mil,
  round((sum(FTR_Payable_Funded_From_FTR_Rental_Dollars) + sum(FTR_Payable_Funded_From_Auction_Revenue_Dollars) - sum(FTR_Auction_Revenue_Dollars))/1e6,3)  as Total_Net_Pay_to_FTR_Participant_Mil
from gold.ftr_monthly_rental
GROUP BY 
  CASE 
    WHEN month(Start_Date) >= 7 then cast(year(Start_Date) as STRING) || '/' || cast(year(Start_Date)+1 as STRING) 
    ELSE cast(year(Start_Date)-1 as STRING) || '/' || cast(year(Start_Date) as STRING) 
  END
ORDER BY StartDate
