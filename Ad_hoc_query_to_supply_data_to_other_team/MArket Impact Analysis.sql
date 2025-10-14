-- Databricks notebook source
select TradingDate, TradingPeriodNumber, IslandCode,FIRDollarsPerMegawattHour,SIRDollarsPerMegawattHour from gold.predispatchandfinalreserveprices
where PriceTypeCode = 'F'
and TradingDate between '2023-04-07' and '2023-04-07'
and TradingPeriodNumber between 6 and 7
Order by TradingDate, TradingPeriodNumber,IslandCode

-- COMMAND ----------

select TradingDate, TradingPeriodNumber, PointOfConnectionCode, DollarsPerMegawattHour from gold.predispatchandfinalenergyprices 
where PriceTypeCode = 'F'
and TradingDate between '2023-04-07' and '2023-04-07'
and TradingPeriodNumber between 6 and 7
Order by TradingDate, TradingPeriodNumber, PointOfConnectionCode

-- COMMAND ----------

select TradingDate, TradingPeriodNumber, PointOfConnectionCode, ParticipantShortName,FlowDirection,KilowattHours/1000 as MWh from gold.reconciledinjectionandofftake
where TradingDate between '2023-04-07' and '2023-04-07' and TradingPeriodNumber between 6 and 7 and KilowattHours <> 0
Order by TradingDate, TradingPeriodNumber, PointOfConnectionCode, ParticipantShortName

-- COMMAND ----------

select FTRContractStartDate, FTRContractType, SinkPointOfConnectionCode, SourcePointOfConnectionCode, sum(FTRContractMegawatts) as FTR_MW 
from gold.ftrcontracthistory where FTRContractStartDate = '2023-04-01' and FTRContractOwnerParticipantShortName = 'Meridian Energy'
Group BY FTRContractStartDate, FTRContractType, SinkPointOfConnectionCode, SourcePointOfConnectionCode
Order by FTRContractType, SinkPointOfConnectionCode, SourcePointOfConnectionCode
