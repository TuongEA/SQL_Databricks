-- Databricks notebook source
select 
--a.*,
a.CaseID, a.TradingDate, a.TradingPeriodNumber, a.IntervalDateTime, a.PointOfConnectionCode, a.UnitCode,
a.ProductType, a.ProductClass, a.OfferTypeCode, a.IsFirFlag, a.ReserveTypeCode, a.TrancheNumber,
a.SPDClearedFIRMegawatts, a.SPDClearedSIRMegawatts, a.SPDClearedGenerationMegawatts,
a.SPDForecastOfGenerationPotentialMegawatts,a.SPDIntermittentGenerationPotentialMegawatts, a.SPDMaximumOutputMegawatts,
a.SPDMaximumRampDownMegawattPerHour, a.SPDMaximumRampUpMegawattPerHour,
a.SPDPartiallyLoadedSpinningReservePercent, a.SPDDollarsPerMegawattHour, a.SPDMegawatts, a.SPDAdjustedMegawatts, 
-- b.SPDDollarsPerMegawattHour as SPDDollarsPerMegawattHour_7, b.SPDMegawatts as SPDMegawatts_7, 
b.SPDAdjustedMegawatts as SPDAdjustedMegawatts_7
from sensitive.spdbidsandoffers a inner join sensitive.spdbidsandoffers_sevendays b 
ON (a.CaseID = b.CaseID and a.TradingDate = b.TradingDate and a.TradingPeriodNumber = b.TradingPeriodNumber
and a.PointOfConnectionCode = b.PointOfConnectionCode and a.UnitCode = b.UnitCode
and a.ProductType = b.ProductType and a.ProductClass = b.ProductClass and a.OfferTypeCode = b.TradeType
and a.IsFirFlag = b.SixSec = 1 and a.TrancheNumber = b.TrancheNumber 
and a.CaseTypeCode = b.CaseType and a.CaseTypeCode = 'FP')
where a.SPDAdjustedMegawatts <> b.SPDAdjustedMegawatts 
and a.CaseID = '291112021061200404' and a.TradingPeriodNumber = 14 and a.PointOfConnectionCode = 'SFD2201'
Limit 100

-- COMMAND ----------

select a.CaseID, a.TradingDate, a.TradingPeriodNumber, a.IntervalDateTime, a.PointOfConnectionCode, a.UnitCode,
a.ProductType, a.ProductClass, a.OfferTypeCode, a.IsFirFlag, a.ReserveTypeCode, a.TrancheNumber,
a.SPDClearedFIRMegawatts, a.SPDClearedSIRMegawatts, a.SPDClearedGenerationMegawatts,
a.SPDForecastOfGenerationPotentialMegawatts,a.SPDIntermittentGenerationPotentialMegawatts, a.SPDMaximumOutputMegawatts,
a.SPDMaximumRampDownMegawattPerHour, a.SPDMaximumRampUpMegawattPerHour, 
a.SPDPartiallyLoadedSpinningReservePercent, a.SPDDollarsPerMegawattHour, a.SPDMegawatts, a.SPDAdjustedMegawatts
--a.* 
from sensitive.spdbidsandoffers a 
where CaseID = '291112021061200404' and a.TradingPeriodNumber = 14 
and a.PointOfConnectionCode = 'SFD2201' and a.UnitCode = 'SFD22'

-- COMMAND ----------

select * from sensitive.spdbidsandoffers_sevendays a
where CaseID = '291112021061200404' and a.TradingPeriodNumber = 14 
and a.PointOfConnectionCode = 'SFD2201' and a.UnitCode = 'SFD22'
ORDER BY a.ProductType,a.TradeType, a.SixSec, a.TrancheNumber

-- COMMAND ----------

select * from copper.spd_fp_mssdata_mndconstraint 
where CaseID = '291112021061200404' and TradingPeriodNumber = 14 and SPDMarketNodeConstraintName like 'FK_SFD2201 SFD22%'

-- COMMAND ----------


