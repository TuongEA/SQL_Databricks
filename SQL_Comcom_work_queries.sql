-- Databricks notebook source
SET TIME ZONE 'Pacific/Auckland';

-- COMMAND ----------

-- DBTITLE 1,Get reconciled demand for CTCT
select TradingDate, TradingPeriodNumber as Period, PointOfConnectionCode as Pnode, TraderCode as Trader, 
round(sum(KilowattHours/500),3) as MW 
from ea_prd.gold.factreconciledinjectionandofftake 
where TradingDate between '2019-11-01' and '2022-10-31'
and TraderCode in ('CTCT','CNIR','TRUS')
and FlowDirection = 'Offtake'
group by all
order by TradingDate, Period, Pnode

-- COMMAND ----------

-- DBTITLE 1,Get reconciled generation for CTCT and Manawa
select TradingDate, TradingPeriodNumber as Period, PointOfConnectionCode as Pnode, 
NetworkParticipantCode, TraderCode as Trader, 
round(sum(KilowattHours/500),3) as MW 
from ea_prd.gold.factreconciledinjectionandofftake 
where TradingDate between '2019-11-01' and '2022-10-31'
and TraderCode in ('CTCT','CNIR','TRUS')
and FlowDirection = 'Injection'
and KilowattHours > 0
group by all
order by TradingDate, Period, Pnode

-- COMMAND ----------

-- DBTITLE 1,Get FTR contracts
select FTRContractOwnerCode, FTRContractType, FTRSourceNode, FTRSinkNode, FTRContractStartDate, FTRContractEndDate, sum(FTRContractMegawatts) as FTR_MW
from ea_prd.gold.factftrcontracttradinghistory
where FTRContractOwnerCode in ('CTCT','TRUS','CNIR')
and FTRContractStartDate between '2019-11-01' and '2022-10-31'
group by all
order by FTRContractStartDate, FTRContractType, FTRSourceNode, FTRSinkNode, FTRContractOwnerCode

-- COMMAND ----------

-- DBTITLE 1,Get Hedge contracts
With L1 as (select distinct DisclosureCode from hive_metastore.copper.hedgecontracts where OtherPartyLegalName like "Manawa%")
select DisclosureCode, ParticipantShortName, ParticipantRole, 
case 
  when DisclosureCode in ('1196460','1196461','1196462') then "Octopus Energy NZ Limited"
  when DisclosureCode = '1068826' then "Nelson Marlborough District Health Board"
  when DisclosureCode in (select * from L1) then "Manawa"
  else CounterPartyNameAsMapped
end as CounterPartyNameAsMapped, 
InstrumentCode, VolumeType,ForAllTradingPeriodsFlag,ZoneCode,EffectiveFromDate, EffectiveToDate, MegawattHours, ContractStatus, DisclosureUpdatedDatetime
from hive_metastore.sensitive.hedgedisclosures
where EffectiveFromDate <= '2022-10-31' and EffectiveToDate >= '2019-11-01'
and (ParticipantCode in ('CTCT','CNIR','TRUS') 
  or CounterPartyShortNameAsMapped = 'Contact Energy' 
  or CounterPartyNameAsMapped = 'TrustPower' 
  or DisclosureCode in (select * from L1)
    )
and DisclosureUpdateOrder = 1
and ContractStatus <> 'Deleted'
order by DisclosureCode

-- COMMAND ----------

-- DBTITLE 1,Get TradingDate - Period list
with L1 as ( select ActualDate, DaylightSavingIndicator from silver.dimdate where ActualDate between '2019-11-01' and '2022-10-31')
, L2 as (select distinct DaylightSavingIndicatorFlag, Trading30MinIntervalNumber as Period from silver.dimtradinginterval)
select L1.ActualDate as TradingDate, L2.Period
from L1 inner join L2 on L1.DaylightSavingIndicator = L2.DaylightSavingIndicatorFlag
order by TradingDate, Period
