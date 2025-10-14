-- Databricks notebook source
SET tradingdate = '2023-07-21';
SET caseID = '261012023041930549';

-- COMMAND ----------

with tpr as ( -- Get cleared energy, cleared FIR, cleared SIR and max cleared price for energy into one row
  select tpr.CaseTypeCode, tpr.CaseID, tpr.TradingDate, tpr.TradingPeriodNumber, tpr.IntervalDateTime, tpr.PNodeCode, 
  sum(tpr.SPDGenerationMegawatts) as SPDGenerationMegawatts, sum(tpr.SPDClearedFIRMegawatts) as SPDClearedFIRMegawatts,
  sum(tpr.SPDClearedSIRMegawatts) as SPDClearedSIRMegawatts, 
  sum(case when OfferTypeCode = 'ENOF' then tpr.SPDClearedPriceDollarsPerMegawattHour else 0 end) as EnrgClrPrice
  from copper.spd_rtd_solution_traderperiod tpr
  where OfferTypeCode <> 'ILRO' 
  and TradingDate = ${hiveconf:tradingdate}
  -- and CaseID = ${hiveconf:caseID}
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
select * from L3 
where Is_Marginal = 1 
order by IntervalDateTime, IslandCode, PnodeName


-- COMMAND ----------

SET TIME ZONE 'Pacific/Auckland';

-- COMMAND ----------

select * from gold.marginalplant where TRadingDate  = '2022-03-10'
order by IntervalDateTime

-- COMMAND ----------


