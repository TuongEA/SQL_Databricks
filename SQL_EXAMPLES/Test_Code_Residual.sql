-- Databricks notebook source
SET TIME ZONE 'Pacific/Auckland';

-- COMMAND ----------

-- DBTITLE 1,Get Island Solution Data
select CaseID,IntervalDateTime,
sum(SPDOfferedEnergyMegawatts) as SPDOfferedEnergyMegawatts, 
sum(SPDClearedEnergyMegawatts) as SPDClearedEnergyMegawatts, 
sum(SPDResidualEnergyMegawatts) as SPDResidualEnergyMegawatts,
sum(SPDDispatchableDemandClearedMegawatts) as SPDDispatchableDemandClearedMegawatts,
sum(SPDResidualEnergyMegawatts + SPDClearedEnergyMegawatts) as SPDOfferedandAvaialbleEnergyMegawatts
from ea_prd.gold.factspdisland a
where CaseTypeCode = 'RTD' and TradingDate = '2024-05-02' and caseID = '11012024051905394' 
group by CaseID, IntervalDateTime

-- COMMAND ----------

-- DBTITLE 1,Get Mnode Constraint Info
With L1 as (
  select CaseID,IntervalDateTime,PNodeCode, sum(SPDClearedFIRMegawatts) as SPDClearedFIRMegawatts, sum(SPDClearedSIRMegawatts) as SPDClearedSIRMegawatts
  from ea_prd.silver.spd_solution_traderperiod 
  where CaseTypeCode = 'RTD' and TradingDate = '2024-05-02' and caseID = '11012024051905394' and OfferTypeCode <> 'ILRO' 
  group by CaseID,IntervalDateTime,PNodeCode
)
select distinct a.CaseID,a.IntervalDateTime,a.SPDMarketNodeConstraintName, a.SPDConstraintUpperLimit, b.PNodeCode, SPDClearedFIRMegawatts, SPDClearedSIRMegawatts
from ea_prd.silver.spd_mssdata_mndconstraint a
inner join ea_prd.silver.spd_mssdata_mndconstraintfactors b
on (a.CaseID = b.caseID and a.IntervalDateTime = b.IntervalDateTime and a.SPDMarketNodeConstraintName = b.SPDMarketNodeConstraintName)
inner join L1
on (a.CaseID = L1.caseID and a.IntervalDateTime = L1.IntervalDateTime and b.PNodeCode = L1.PNodeCode)
where a.CaseTypeCode = 'RTD' and a.TradingDate = '2024-05-02' and a.caseID = '11012024051905394' 
and a.SPDConstraintHasUpperLimitFlag = 'Y'

-- COMMAND ----------

-- DBTITLE 1,Get total Cleared generation and reserve at each offer
select CaseID,IntervalDateTime,PNodeCode, sum(SPDGenerationMegawatts) as SPDGenerationMegawatts,
sum(SPDClearedFIRMegawatts) as SPDClearedFIRMegawatts, sum(SPDClearedSIRMegawatts) as SPDClearedSIRMegawatts
from ea_prd.silver.spd_solution_traderperiod 
where CaseTypeCode = 'RTD' and TradingDate = '2024-05-02' and caseID = '11012024051905394' 
and OfferTypeCode <> 'ILRO' 
group by CaseID,IntervalDateTime,PNodeCode

-- COMMAND ----------

-- DBTITLE 1,Get total offered energy for each offer 
select CaseID,IntervalDateTime,PNodeCode, sum(SPDMaximumOutputMegawatt) as SPDOfferOutputMegawatts
from ea_prd.silver.spd_mssdata_bidsandoffers
where CaseTypeCode = 'RTD' and TradingDate = '2024-05-02' and caseID = '11012024051905394' 
and SPDIsDispatchableFlag <> 'N' and OfferTypeCode = 'ENOF' 
group by CaseID,IntervalDateTime,PNodeCode

-- COMMAND ----------

-- DBTITLE 1,Get generation and ramping capacity
with L1 as (-- Get Cleared Reserve
  select CaseID,IntervalDateTime,PNodeCode, sum(SPDClearedFIRMegawatts) as SPDClearedFIRMegawatts, sum(SPDClearedSIRMegawatts) as SPDClearedSIRMegawatts
  from ea_prd.silver.spd_solution_traderperiod 
  where CaseTypeCode = 'RTD' and TradingDate = '2024-05-02' and caseID = '11012024051905394' 
  and OfferTypeCode <> 'ILRO' 
  group by CaseID,IntervalDateTime,PNodeCode
), L2 AS ( -- Get total energy offer
select CaseID,IntervalDateTime,PNodeCode, sum(SPDMaximumOutputMegawatt) as SPDOfferOutputMegawatts
from ea_prd.silver.spd_mssdata_bidsandoffers
where CaseTypeCode = 'RTD' and TradingDate = '2024-05-02' and caseID = '11012024051905394' 
and SPDIsDispatchableFlag <> 'N' and OfferTypeCode = 'ENOF' 
group by CaseID,IntervalDateTime,PNodeCode
)
select a.CaseID, a.IntervalDateTime,a.PNodeCode,SPDOfferOutputMegawatts, SPDMaximumOutputMegawatts as Capacity,
least(SPDMaximumOutputMegawatts-SPDClearedFIRMegawatts,SPDMaximumOutputMegawatts-SPDClearedSIRMegawatts) as CapacityAfterReserve,
case
  when SPDIsIntermittentUnitFlag = 'Y' and SPDIsIntermittentUnitResponseToPriceFlag = 'Y' then SPDIntermittentGenerationPotentialMegawatts
  when SPDIsIntermittentUnitFlag = 'Y' and SPDIsIntermittentUnitResponseToPriceFlag = 'N' then 0
  else SPDIntermittentGenerationPotentialMegawatts
end as IntermittentPotentialCapacity,
SPDMaximumRampUpMegawattPerHour/12 + SPDInitialMegawatts as RampingUpCapacity
-- SPDClearedFIRMegawatts, SPDClearedSIRMegawatts, SPDIntermittentGenerationPotentialMegawatts,SPDIsIntermittentUnitFlag,SPDIsIntermittentUnitResponseToPriceFlag,SPDMaximumRampUpMegawattPerHour, SPDInitialMegawatts
from ea_prd.silver.spd_mssdata_traderperiods a
left join ea_prd.silver.spd_mssdata_unitdata b
On (a.CaseID = b.CaseID and a.IntervalDateTime = b.IntervalDateTime and a.PNodeCode = b.PNodeCode)
left join ea_prd.silver.spd_solution_pnode c
On (a.CaseID = c.CaseID and a.IntervalDateTime = c.IntervalDateTime and a.PNodeCode = c.PNodeCode)
left join L1
On (a.CaseID = L1.CaseID and a.IntervalDateTime = L1.IntervalDateTime and a.PNodeCode = L1.PNodeCode)
inner join L2
On (a.CaseID = L2.CaseID and a.IntervalDateTime = L2.IntervalDateTime and a.PNodeCode = L2.PNodeCode)
where a.CaseTypeCode = 'RTD' and a.TradingDate = '2024-05-02'and a.caseID = '11012024051905394'
and a.OfferTypeCode = 'ENOF' 
Order By a.CaseID, a.IntervalDateTime,a.PNodeCode

-- COMMAND ----------

select CaseID,IntervalDateTime,PNodeCode,SPDIsIntermittentUnitResponseToPriceFlag,*
from ea_prd.silver.spd_mssdata_unitdata
where CaseTypeCode = 'RTD' and TradingDate = '2024-05-02'and caseID = '11012024051905394'

-- COMMAND ----------

select CaseID,IntervalDateTime,PNodeCode, SPDIsIntermittentUnitFlag,SPDIsIntermittentUnitResponseToPriceFlag,
sum(SPDOfferOutputMegawatts) as SPDOfferOutputMegawatts,sum(SPDAdjustedOfferOutputMegawatts) as SPDAdjustedOfferOutputMegawatts,
avg(SPDIntermittentGenerationPotentialMegawatts) as SPDIntermittentGenerationPotentialMegawatts

from ea_prd.gold.factspdbidsandoffers 
where CaseTypeCode = 'RTD' and TradingDate = '2024-05-02'and caseID = '11012024051905394' 
 and OfferTypeCode = 'ENOF' 
group by CaseID,IntervalDateTime,PNodeCode, SPDIsIntermittentUnitFlag,SPDIsIntermittentUnitResponseToPriceFlag

-- COMMAND ----------

select CaseID,IntervalDateTime, --PnodeCode,
sum(SPDAdjustedOfferOutputMegawatts) as SPDOfferOutputMegawatts
from ea_prd.gold.factspdbidsandoffers where CaseTypeCode = 'RTD' and TradingDate = '2024-05-02' and OfferTypeCode = 'ENOF' and caseID = '11012024051905394' 
group by CaseID,IntervalDateTime--,PnodeCode

-- COMMAND ----------

select CaseID,IntervalDateTime,PNodeCode,OfferTypeCode, SPDGenerationMegawatts,SPDClearedFIRMegawatts,SPDClearedSIRMegawatts
from ea_prd.silver.spd_solution_traderperiod where caseID = '11012024051905394' 
and PNodeCode in ('ARA2201 ARA0','ARI1101 ARI0','ARI1102 ARI0','ATI2201 ATI0','WPA2201 WPA0','MTI2201 MTI0','OHK2201 OHK0','WKM2201 WKM0','KPO1101 KPO0')
