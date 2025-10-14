-- Databricks notebook source
SELECT * FROM processing.bronze_hedgecontracts 
WHERE OtherPartyLegalName in ('Bluescope Steel (Finance) Limited','NZ Steel Limited','New Zealand Steel','New Zealand Steel Ltd')

-- COMMAND ----------

SELECT * FROM processing.bronze_hedgecontracts WHERE PartyCode in ('NZSC','NZSP','NZST','NZSD')

-- COMMAND ----------

select distinct entity from processing.bronze_asx24_eanz_datasphere_participant_accounts ORDER BY entity

-- COMMAND ----------

SELECT TradingDate, TradingPeriodNumber as TradingPeriod, FlowDirection, sum(KilowattHours/500) as MW FROM gold.reconciledinjectionandofftake 
WHERE PointOfConnectionCode in ('GLN0331','GLN0332')
AND NetworkCode in ('NZSC','NZSP','NZST','NZSD','ALNT')
AND TradingDate >= '2010-06-01' 
GROUP BY TradingDate, TradingPeriodNumber, FlowDirection
ORDER BY TradingDate, TradingPeriodNumber, FlowDirection
