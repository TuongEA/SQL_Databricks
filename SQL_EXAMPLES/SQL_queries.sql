-- Databricks notebook source
SET TIME ZONE 'Pacific/Auckland';

-- COMMAND ----------

select * from datahub.environment.hydrostorage
where ReservoirCode = 'Tekapo'
and StorageDate >= '2005-01-01'

order by StorageDate, StorageTime


-- COMMAND ----------

With L1 as (
  select TradingDate, TradingPeriodNumber, sum(KilowattHours/500) as MW from datahub.wholesale.reconciledinjectionandofftake
  where TradingDate between '2024-01-01' and '2024-12-31'
  and FlowDirection = 'Injection'
  and KilowattHours > 0
group by all
), L2 as (
  SELECT *,
        CASE 
            WHEN Ceil(DAYOFYEAR(TradingDate) / 7.0) >= 52 THEN 52
            ELSE Ceil(DAYOFYEAR(TradingDate) / 7.0)
        END AS Week
  FROM L1
  -- and tradingperiodnumber = 1
), L3 as (
  SELECT *,  RANK() OVER (PARTITION BY `Week` ORDER BY MW DESC) AS MW_Rank FROM L2
)
Select Week,
Case 
  when MW_Rank <= 40 then "PEAK"
  when MW_Rank <= 160 then "SHOULDER"
  else "OFFPEAK"
end as Block,
round(avg(MW),3) as MW
From L3
group by all
order by Week, MW

-- COMMAND ----------

With L1 as (
  select TradingDate, TradingPeriodNumber, sum(KilowattHours/500) as MW from datahub.wholesale.reconciledinjectionandofftake
  where TradingDate between '2024-01-01' and '2024-12-31'
  and FlowDirection = 'Injection'
  and KilowattHours > 0
group by all
), L2 as (
  SELECT *,
        CASE 
            WHEN Ceil(DAYOFYEAR(TradingDate) / 7.0) >= 52 THEN 52
            ELSE Ceil(DAYOFYEAR(TradingDate) / 7.0)
        END AS Week
  FROM L1
  -- and tradingperiodnumber = 1
), L3 as (
  SELECT *,  RANK() OVER (PARTITION BY `Week` ORDER BY MW DESC) AS MW_Rank FROM L2
)
Select Week,
Case 
  when MW_Rank <= 20 then "PEAK"
  when MW_Rank <= 160 then "SHOULDER"
  else "OFFPEAK"
end as Block,
round(avg(MW),3) as MW
From L3
group by all
order by Week, MW

-- COMMAND ----------

With L1 as (
  select TradingDate, TradingPeriodNumber, sum(KilowattHours/500) as MW from datahub.wholesale.reconciledinjectionandofftake
  where TradingDate between '2024-01-01' and '2024-12-31'
  and FlowDirection = 'Injection'
  and KilowattHours > 0
group by all
), L2 as (
  SELECT *,
        CASE 
            WHEN Ceil(DAYOFYEAR(TradingDate) / 7.0) >= 52 THEN 52
            ELSE Ceil(DAYOFYEAR(TradingDate) / 7.0)
        END AS Week
  FROM L1
  -- and tradingperiodnumber = 1
), L3 as ( SELECT *,  RANK() OVER (PARTITION BY `Week` ORDER BY MW DESC) AS MW_Rank FROM L2 )
, peak_20 as (  Select Week, round(avg(MW),3) as Peak_20Hr From L3 where MW_Rank <= 40 group by all )
, peak_10 as (  Select Week, round(avg(MW),3) as Peak_10Hr From L3 where MW_Rank <= 20 group by all )
, peak_5 as (  Select Week, round(avg(MW),3) as Peak_5Hr From L3 where MW_Rank <= 10 group by all )
, peak_1 as (  Select Week, round(avg(MW),3) as Peak_1Hr From L3 where MW_Rank <= 2 group by all)
Select peak_1.Week, Peak_20Hr, Peak_10Hr, Peak_5Hr, Peak_1Hr
from peak_20 join peak_10 on peak_20.Week = peak_10.Week
join peak_5 on peak_20.Week = peak_5.Week
join peak_1 on peak_20.Week = peak_1.Week
order by Week

-- COMMAND ----------

with L1 as (
  SELECT TradingDate, Case when CarbonVWAPDollarsPerNZU = 0 then nullif(CarbonVWAPDollarsPerNZU,0) else CarbonVWAPDollarsPerNZU end AS Value
  FROM datahub.wholesale.gasandcarbonpriceindices
), L2 as (
SELECT 
  TradingDate,
  Value as Original,
  LAST(Value,TRUE) OVER ( ORDER BY TradingDate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS New_Value
FROM (SELECT *, LAST(Value, TRUE) OVER (ORDER BY TradingDate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS New_Value FROM L1) AS sub WHERE sub.New_Value IS NOT NULL
)
select * from L2 where New_Value is not null


-- COMMAND ----------

with L1 as (select IntervalDateTime, IslandCode, PointOfConnectionCode, UnitCode, avg(SPDClearedFIRMegawatts) as Cleared_FIR 
from datahub.wholesale.spdbidsandoffers
where TradingDate = '2025-09-18' and TradingPeriodNumber = 20 and CaseTypeCode = 'RTD' and TradingIntervalNumber = 1
and ReserveTypeCode = 'FIR'
group by all)

,L2 as (
  select IntervalDateTime, IslandCode, sum(Cleared_FIR) as FIR_MW from L1 group by all 
)
, L3 as (
select IntervalDateTime,IslandCode, SPDAdjustedMegawatts, SPDTranchePriceDollarsPerMegawattHour from datahub.wholesale.spdbidsandoffers
where TradingDate = '2025-09-18' and TradingPeriodNumber = 20 and CaseTypeCode = 'RTD' and TradingIntervalNumber = 1
and ReserveTypeCode = 'FIR' and SPDAdjustedMegawatts > 0 
)
select L3.*, FIR_MW from L3 inner join L2 on  L3.IntervalDateTime= L2.IntervalDateTime and L3.IslandCode = L2.IslandCode
order by islandcode, SPDTranchePriceDollarsPerMegawattHour 

-- COMMAND ----------



-- COMMAND ----------

select * from ea_prd.bronze.fkcons 
where TRADING_DATE = '01/05/2025'
and TRADING_PERIOD = '26'

-- COMMAND ----------

select * from ea_prd.gold.dimgenerationplant where TechnologyCode = 'BAT'

-- COMMAND ----------

select PNodeCode, ParticipantCode, ConstraintTypeCode, ConstraintDirectionIndicator, 
Min(TradingDate) as FromDate, Max(TradingDate) as ToDate, sum(Dollars) as Dollars, avg(Megawatts) as Average_MW
from ea_prd.gold.factconstrainedonoff 
where PointOfConnectionCode in ('HLY0331','BRB0331','SWN2201')
group by all

-- COMMAND ----------

select * from ea_prd.gold.factconstrainedonoff where ConstraintTypeCode = 'ECONS' and IsConstraintActiveFlag = 'N'
order by TradingDate

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
