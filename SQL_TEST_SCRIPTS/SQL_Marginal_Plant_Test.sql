-- Databricks notebook source
Select * from gold.marginalplant 
where TradingDate =  '2024-07-05' and CaseID = '41012024072010317'


-- COMMAND ----------

SET caseID = '261012023041950957';

-- COMMAND ----------

select * from bronze.spd_rtd_solution_island
where FileName like '%' || ${hiveconf:caseID} || '%'

-- COMMAND ----------

select tpr.CaseTypeCode, tpr.CaseID, tpr.TradingDate, tpr.TradingPeriodNumber, tpr.IntervalDateTime, tpr.PNodeCode, 
SPDGenerationMegawatts, SPDClearedFIRMegawatts,SPDClearedSIRMegawatts, SPDClearedPriceDollarsPerMegawattHour
from copper.spd_rtd_solution_traderperiod tpr
where OfferTypeCode <> 'ILRO' and tpr.PNodeCode = 'AVI2201 AVI0'
and CaseID = ${hiveconf:caseID}
  --group by tpr.CaseTypeCode, tpr.CaseID, tpr.TradingDate, tpr.TradingPeriodNumber, tpr.IntervalDateTime, tpr.PNodeCode

-- COMMAND ----------

select tpr.CaseTypeCode, tpr.CaseID, tpr.TradingDate, tpr.TradingPeriodNumber, tpr.IntervalDateTime, tpr.PNodeCode, 
SPDInitialMegawatts,SPDGenerationMegawatts,SPDDollarsPerMegawattHour, *
from copper.spd_rtd_solution_pnode tpr where tpr.PNodeCode = 'AVI2201 AVI0' and CaseID = ${hiveconf:caseID}

-- COMMAND ----------

with tpr as ( -- Get cleared energy, cleared FIR, cleared SIR and max cleared price for energy into one row
  select tpr.CaseTypeCode, tpr.CaseID, tpr.TradingDate, tpr.TradingPeriodNumber, tpr.IntervalDateTime, tpr.PNodeCode, 
  sum(tpr.SPDGenerationMegawatts) as SPDGenerationMegawatts, sum(tpr.SPDClearedFIRMegawatts) as SPDClearedFIRMegawatts,
  sum(tpr.SPDClearedSIRMegawatts) as SPDClearedSIRMegawatts, 
  sum(case when OfferTypeCode = 'ENOF' then tpr.SPDClearedPriceDollarsPerMegawattHour else 0 end) as EnrgClrPrice
  from copper.spd_rtd_solution_traderperiod tpr
  where OfferTypeCode <> 'ILRO' 
  and CaseID = ${hiveconf:caseID}
  group by tpr.CaseTypeCode, tpr.CaseID, tpr.TradingDate, tpr.TradingPeriodNumber, tpr.IntervalDateTime, tpr.PNodeCode
),
L2 as ( -- Get marginal energy price and marginal reserve price at each offer node. 
select tpr.CaseTypeCode as CaseType, tpr.CaseID, tpr.TradingDate, tpr.TradingPeriodNumber as TradingPeriod, tpr.IntervalDateTime, 
tpr.PNodeCode as PnodeName, poc.IslandCode, SPDMaximumOutputMegawatts as Capacity, 
tpr.SPDGenerationMegawatts as Energy_MW, tpr.SPDClearedFIRMegawatts as FIR_MW, tpr.SPDClearedSIRMegawatts as SIR_MW,
ild.SPDFIRDollarsPerMegawattHour as FIR_Price, ild.SPDSIRDollarsPerMegawattHour as SIR_Price,
pn.SPDDollarsPerMegawattHour as Energy_Price, tpr.EnrgClrPrice as Energy_Cleared_Price, 
ild.SPDFIRRiskSetter as Island_FIR_Risk_Setter,ild.SPDSIRRiskSetter as Island_SIR_Risk_Setter,
Case When rskF.ReserveTypeCode is Null then 0 Else 1 End as FIR_risk_setter, 
Case When rskS.ReserveTypeCode is Null then 0 Else 1 End as SIR_risk_setter
from tpr 
inner join copper.spd_rtd_mssdata_traderperiods a 
on (tpr.CaseID = a.CaseID and tpr.IntervalDateTime = a.IntervalDateTime and tpr.PNodeCode = a.PNodeCode) 
inner join silver.dimpointofconnection poc 
on (a.PointOfConnectionCode = poc.PointOfConnectionCode and poc.IsCurrentFlag = 'Y' and a.OfferTypeCode like 'EN%')  
inner join copper.spd_rtd_solution_island ild 
on (tpr.caseID = ild.CaseID and tpr.IntervalDateTime = ild.IntervalDateTime and ild.IslandCode = poc.IslandCode)
inner join copper.spd_rtd_solution_pnode pn 
on (tpr.CaseID = pn.CaseID and tpr.IntervalDateTime = pn.IntervalDateTime and tpr.PNodeCode = pn.PNodeCode)
left join copper.spd_rtd_solution_risksetter rskF
on (tpr.CaseID = rskF.CaseID and tpr.IntervalDateTime = rskF.IntervalDateTime 
and tpr.PNodeCode = rskF.RiskSetterID and rskF.ReserveTypeCode = 'FIR')
left join copper.spd_rtd_solution_risksetter rskS
on (tpr.CaseID = rskS.CaseID and tpr.IntervalDateTime = rskS.IntervalDateTime 
and tpr.PNodeCode = rskS.RiskSetterID and rskS.ReserveTypeCode = 'SIR')
), L3 as (-- Condition to check if a energy offer is marginal 
select *,
Case 
  When Energy_MW = 0 Then 0
  Else Case
    -- When Energy_Cleared_Price + FIR_Price*FIR_risk_setter + SIR_Price*SIR_risk_setter - Energy_Price  = 0 Then 1
    -- When Energy_Cleared_Price + FIR_Price*FIR_risk_setter + SIR_Price*SIR_risk_setter - Energy_Price =-5e-4 Then 1
    -- When Energy_Cleared_Price + FIR_Price*FIR_risk_setter + SIR_Price*SIR_risk_setter - Energy_Price = 5e-4 Then 1
    When Energy_Cleared_Price + FIR_Price*FIR_risk_setter + SIR_Price*SIR_risk_setter - Energy_Price  between -5e-4 and 5e-4 Then 1
    
    Else 0
  End
End as Is_Marginal from L2)
select
 --*,
 CaseType,CaseID,IntervalDateTime,PnodeName,Capacity,Energy_MW,FIR_MW,SIR_MW,FIR_Price,SIR_Price,Energy_Price,Energy_Cleared_Price
 from L3 where Is_Marginal = 1 
 and PnodeNAme = 'AVI2201 AVI0'
 order by IntervalDateTime, IslandCode, PnodeName


-- COMMAND ----------

Select --*,
CaseTypeCode as  CaseType,CaseID,IntervalDateTime,PnodeCode,CapacityMegawatts as Capacity, EnergyMegawatts as Energy_MW, 
FIRClearedMegawatts as FIR_MW, SIRClearedMegawatts as SIR_MW, FIRPriceDollarsPerMegawattHour as FIR_Price,
SIRPriceDollarsPerMegawattHour as SIR_Price,EnergyPriceDollarsPerMegawattHour as Energy_Price,
EnergyClearedPriceDollarsPerMegawattHour as Energy_Cleared_Price
from gold.marginalplant where CaseID = ${hiveconf:caseID} 
and PnodeCode = 'AVI2201 AVI0'
order by IslandCode,PnodeCode

-- COMMAND ----------

select * from copper.spd_rtd_solution_constraint where CaseID = ${hiveconf:caseID} and 

-- COMMAND ----------

select * from sensitive.spdbidsandoffers_sevendays 
where caseID = ${hiveconf:caseID} and PointOfConnectionCode = 'ROX1101' 
order by TradeType, sixSec, TrancheNumber

-- COMMAND ----------

select SPDInitialMegawatts,SPDGenerationMegawatts, * from sensitive.spdnodal where caseID = ${hiveconf:caseID} and UnitCode = 'AVI0'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 84.159 + 48.465 + 55.669 + 25.707

-- COMMAND ----------


