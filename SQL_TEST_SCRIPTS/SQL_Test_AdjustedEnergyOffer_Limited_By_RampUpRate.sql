-- Databricks notebook source
-- DBTITLE 1,This query show an example where adjusted energy offers are different between spdbidsandoffers and  spdbidsandoffers_sevendays
-- This query show an example where adjusted energy offers are different between spdbidsandoffers and spdbidsandoffers_sevendays
-- Note that the ramp-up rate for this HLY2201 HLY1 is 300MW/hour --> 5 MW/minute --> 25MW/5-minute
select 
a.CaseID, a.TradingDate, a.TradingPeriodNumber, a.IntervalDateTime, a.PointOfConnectionCode, a.UnitCode,
a.ProductType, a.ProductClass, a.OfferTypeCode, a.IsFirFlag, a.ReserveTypeCode, a.TrancheNumber,
a.SPDClearedFIRMegawatts, a.SPDClearedSIRMegawatts, a.SPDClearedGenerationMegawatts,
a.SPDForecastOfGenerationPotentialMegawatts,a.SPDIntermittentGenerationPotentialMegawatts, a.SPDMaximumOutputMegawatts,
a.SPDMaximumRampDownMegawattPerHour, a.SPDMaximumRampUpMegawattPerHour,
a.SPDPartiallyLoadedSpinningReservePercent, a.SPDDollarsPerMegawattHour, a.SPDMegawatts, a.SPDAdjustedMegawatts, 
b.SPDDollarsPerMegawattHour as SPDDollarsPerMegawattHour_7, 
b.SPDMegawatts as SPDMegawatts_7 , b.SPDAdjustedMegawatts as SPDAdjustedMegawatts_7 
,a.*
from sensitive.spdbidsandoffers a inner join sensitive.spdbidsandoffers_sevendays b 
ON (a.CaseID = b.CaseID and a.TradingDate = b.TradingDate and a.TradingPeriodNumber = b.TradingPeriodNumber
and a.PointOfConnectionCode = b.PointOfConnectionCode and a.UnitCode = b.UnitCode
and a.ProductType = b.ProductType and a.ProductClass = b.ProductClass and a.OfferTypeCode = b.TradeType
and a.IsFirFlag = b.SixSec = 1 and a.TrancheNumber = b.TrancheNumber 
and a.CaseTypeCode = b.CaseType and a.CaseTypeCode = 'RTD')
where a.SPDAdjustedMegawatts <> b.SPDAdjustedMegawatts 
and a.OfferTypeCode = 'ENOF'
and a.CaseID = '291012023031950982' and a.TradingPeriodNumber = 18 and a.PointOfConnectionCode = 'HLY2201' and a.UnitCode = 'HLY1'
Limit 100

-- COMMAND ----------

-- DBTITLE 1,Let's have a look at offer data in sensitive.spdbidsandoffers 
-- The data in sensitive.spdbidsandoffers shows that the total origianl/adjusted energy offers are 230 MW (original) and 195.001 MW (adjusted) 
select a.CaseID, a.IntervalDateTime, a.PnodeCode, a.OfferTypeCode, a.ReserveTypeCode, a.TrancheNumber,
SPDMegawatts, SPDAdjustedMegawatts, SPDPartiallyLoadedSpinningReservePercent, SPDDollarsPerMegawattHour
, *
from sensitive.spdbidsandoffers a 
where CaseID = '291012023031950982' and a.TradingPeriodNumber = 18 and a.PointOfConnectionCode = 'HLY2201' and a.UnitCode = 'HLY1'
Order by OfferTypeCode, TrancheNumber

-- COMMAND ----------

-- DBTITLE 1,Let's have a look at offer data in sensitive.spdbidsandoffers_sevendays
-- The data in spdbidsandoffers_sevendays shows that the total origianl/adjusted energy offers are 230 MW (original) and 135.464 MW (adjusted).  
select CaseID, TradingDate,TradingPeriodStartTime,PointOfConnectionCode || ' ' || UnitCode as PNodeCode, TradeType, SixSec,
SPDMegawatts, SPDAdjustedMegawatts, SPDPartiallyLoadedSpinningReservePercent, SPDDollarsPerMegawattHour
--, *
from sensitive.spdbidsandoffers_sevendays a
where CaseID = '291012023031950982' and a.TradingPeriodNumber = 18 and a.PointOfConnectionCode = 'HLY2201' and a.UnitCode = 'HLY1'
Order by TradeType, a.TrancheNumber


-- COMMAND ----------

-- DBTITLE 1,why is the difference? Let's find out
-- The query below show that the intial MW of HLY2201 HLY1 is 110.464. Based on the ramp-up rate shown in Cmd 1, the maximum offer at HLY2201 HLY1 for this RTD 5-minute interval is 110.464 + 25  = 135.464 --> Not sure why the data in sensitive.spdbidsandoffers show 195.001 MW.
select CaseID, TradingDate,TradingPeriodNumber,`Interval`,PNodeCode, SPDInitialMegawatts, SPDGenerationMegawatts
from copper.spd_rtd_solution_pnode where CaseID = '291012023031950982' and TradingPeriodNumber = 18
and PNodeCode = 'HLY2201 HLY1'

-- COMMAND ----------


