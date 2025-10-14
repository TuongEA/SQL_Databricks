-- Databricks notebook source
SET TIME ZONE 'Pacific/Auckland';

-- COMMAND ----------

-- MAGIC %md #I. Declining market concentration

-- COMMAND ----------

-- DBTITLE 1,HHI Generation by year
With L1 as (
  select year(a.TradingDate) as `Year`, b.ParticipantShortName as Trader, sum(a.KilowattHours)/1000 as MWh 
  from ea_prd.gold.factreconciledinjectionandofftake a
  inner join ea_prd.gold.dimparticipant b
  on a.TraderCode = b.ParticipantCode
  and a.TradingDate between b.EffectiveStartDate and b.EffectiveEndDate
  where FlowDirection = 'Injection' 
  Group BY year(a.TradingDate), b.ParticipantShortName
), L2 as (
  select year(a.TradingDate) as `Year`,  sum(a.KilowattHours)/1000 as Total_MWh 
  from ea_prd.gold.factreconciledinjectionandofftake a
  where FlowDirection = 'Injection' 
  Group BY year(a.TradingDate)
), L3 as(
select L1.`Year`, Trader, MWh, Total_MWh, power(100*MWh/Total_MWh,2) as HHI
From L1 inner join L2 on L1.`Year` = L2.`Year`
)
select cast(`Year` as string) as `Year`, round(sum(HHI),0) as HHI
from L3
Group by L3.Year
order by L3.Year  

-- COMMAND ----------

-- DBTITLE 1,Retail HHI by ICP
Select * from hive_metastore.brown.retail_aggregatedhhiandcr 
where regiontype = 'NZ' 
and marketSegment = 'All' 
order by Month

-- COMMAND ----------

-- DBTITLE 1,Number of independent generators
select DATE_FORMAT(TradingDate, 'yyyy') as `Year`, b.ParticipantLongName, sum(KilowattHours)/1000000 as GWh 
from ea_prd.gold.factreconciledinjectionandofftake a 
inner join  ea_prd.gold.dimparticipant b 
on (a.TraderCode = b.ParticipantCode and a.TradingDate between b.EffectiveStartDate and b.EffectiveEndDate)
where FlowDirection = "Injection"
group by DATE_FORMAT(TradingDate, 'yyyy'), b.ParticipantLongName
order by `Year`,b.ParticipantLongName

-- COMMAND ----------

-- DBTITLE 1,Number of retail firms (not combine retail branch of the same parent company)
Select Month, ParticipantLongName, sum(ActiveICPs) from hive_metastore.brown.retail_aggregatedicpcounts a 
inner join ea_prd.gold.dimparticipant b on a.RetailerCode = b.ParticipantCode and a.Month between b.EffectiveStartDate and b.EffectiveEndDate
where regiontype = 'NZ' and MarketSegment = 'All'
and RetailerCode <> 'RB00' --Direct Purchasers
and RetailerCode not in ('RETA','RETB','RETC') --Not real!
group by Month, ParticipantLongName
order by Month, ParticipantLongName

-- COMMAND ----------

-- MAGIC %md #II. Price

-- COMMAND ----------

-- DBTITLE 1,Forward Price Curve - BaseLoad
Select * from hive_metastore.brown.forwardmarkets_closingsnapshot 
where CommodityTypeCode = 'BASE' 
and ContractLocationCode = 'OTA' and DurationCode = 'QTR' 
and Instrument = 'Future'
and Instrument not in ('Call','Put')


-- COMMAND ----------

-- DBTITLE 1,OTC price
select * from hive_metastore.sensitive.hedgedisclosures 
where CounterPartyNameAsMapped not like '%ASX%'
and InstrumentCode = 'CfD' and DisclosureUpdateOrder = 1
and ZoneCode = 'UNI'
order by TradingDate desc

-- COMMAND ----------

-- DBTITLE 1,Price and volatility
select TradingDate, TradingPeriodNumber, TradingPeriodStartTime, DollarsPerMegawattHour 
from ea_prd.gold.factpredispatchandfinalenergyprices 
where PointOfConnectionCode = 'OTA2201'
and PriceTypeCode = 'F'
and TradingDate >= '2022-11-01'
order by TradingDate, TradingPeriodNumber

-- COMMAND ----------

-- MAGIC %md #III. Liquidity

-- COMMAND ----------

-- DBTITLE 1,Volume transacted by contract types on ASX
Select TradeDate, Instrument, sum(TradedContracts) as TradedContracts
from hive_metastore.brown.forwardmarkets_tradelog
group by TradeDate, Instrument

-- COMMAND ----------

-- DBTITLE 1,Volume transacted by contract types on OTC
Select TradingDate, Instrument, sum(MegawattHours) as MegawattHours
from hive_metastore.sensitive.hedgedisclosures
where TradingDate >= '2015-01-01' 
and CounterPartyNameAsMapped not like '%ASX%'
and DisclosureUpdateOrder = 1
group by  TradingDate, Instrument


-- COMMAND ----------

-- DBTITLE 1,ASX Traded volumes, by participant (confidential)
Select * from hive_metastore.sensitive.forwardmarketsdeanonymisedtrades 

-- COMMAND ----------

-- DBTITLE 1,OTC Traded volumes, by participant (confidential)
select * from hive_metastore.sensitive.hedgedisclosures
where TradingDate >= '2015-01-01' 
and CounterPartyNameAsMapped not like '%ASX%'
and DisclosureUpdateOrder = 1

-- COMMAND ----------

-- MAGIC %md #VI. Product availability

-- COMMAND ----------

Select year(TradingDate) as TradingYear, Instrument, sum(MegawattHours) as MegawatHours
from hive_metastore.sensitive.hedgedisclosures
where TradingDate >= '2015-01-01' 
-- and CounterPartyNameAsMapped not like '%ASX%'
and DisclosureUpdateOrder = 1
group by year(TradingDate), Instrument


-- COMMAND ----------

-- MAGIC %md #V. Access to flexibility products on reasonable terms

-- COMMAND ----------

-- DBTITLE 1,Contracts between gentailers and others – number and volume
Select year(TradingDate) as TradingYear, ParticipantCode, sum(MegawattHours) as MegawatHours
from hive_metastore.sensitive.hedgedisclosures
where TradingDate >= '2015-01-01' 
and ParticipantCode in ('GENE','CTCT','TRUS','CNIR','MRPL','MERI')
and CounterPartyNameAsMapped not like '%ASX%'
and DisclosureUpdateOrder = 1
GROUP BY year(TradingDate), ParticipantCode


-- COMMAND ----------

-- MAGIC %md ##Effectiveness of available information

-- COMMAND ----------

-- DBTITLE 1,Comsumer Switching By Region
Select * from hive_metastore.brown.retail_aggregatedswitchcounts 
where regiontype = 'NZ' and month = '2024-07-31' and MarketSegment = 'All'
and RetailerCode <> 'RB00' --Direct Purchasers
and RetailerCode not in ('RETA','RETB','RETC') --Not real!

-- COMMAND ----------

-- MAGIC %md ##DISTRIBUTED GENERATION DATA

-- COMMAND ----------

-- DBTITLE 1,Distributed Generation
select Month, ICP, GenerationCapacityKilowatts, FuelType, MarketSegment, NetworkShortName, RetailerShortName, Latitude, Longitude, PhysicalAddressRegion, PhysicalAddress, InstallationType, Island, Zone,
	NewICP, NewGenerationCapacityKilowatts, PointOfConnectionCode, ReconciliationType
from (
    select Month, ICP, GenerationCapacityKilowatts, FuelType, row_number() over (partition by Month, ICP order by EventDate desc) as LatestEventFlag, PointOfConnectionCode, ReconciliationType,
        MarketSegment, NetworkShortName, RetailerShortName, WGS84_Latitude as Latitude, WGS84_Longitude as Longitude, PhysicalAddressRegion, PhysicalAddress, InstallationType, Island, Zone,
        case when last_day(FirstEventDate) = Month then 1 end as NewICP, case when last_day(FirstEventDate) = Month then GenerationCapacityKilowatts end as NewGenerationCapacityKilowatts
    from (
        select distinct last_day(ActualDate) as Month
        from brown.common_dimdatelegacy
    ) m inner join (
        select n.ICP, n.GenerationCapacityKilowatts, n.FuelType, e.EventDate, e.EventEndDate, n.MarketSegment, w.ShortName as NetworkShortName,
            min(n.EventDate) over (partition by n.ICP) as FirstEventDate, n.PointOfConnectionCode, e.ReconciliationType,
            t.ShortName as RetailerShortName, WGS84_Latitude, WGS84_Longitude, PhysicalAddress, PhysicalAddressRegion, e.InstallationType, Island, p.Zone
        from brown.retail_dimicp_scd n inner join brown.retail_dimicp i on n.ICP = i.ICP
            inner join brown.retail_registryevents_network e on n.ICP = e.ICP and n.EventDate = e.EventDate
            inner join ea_prd.gold.dimpointofconnection p on i.PointOfConnectionCode = p.PointOfConnectionCode and n.EventDate between p.EffectiveStartDatetime and p.EffectiveEndDateTime
            inner join brown.common_marketparticipants_mapped t on n.TraderCode = t.ParticipantCodeFrom and t.Relationship = 'RETAILER_REPORTED_AS' and n.EventDate between t.EffectiveDate and t.EndDate
            inner join brown.common_marketparticipants_mapped w on n.NetworkCode = w.ParticipantCodeFrom and w.Relationship = 'RETAIL_NWKP_REPORTED_AS' and n.EventDate between w.EffectiveDate and w.EndDate
        where coalesce(n.GenerationCapacityKilowatts, 0) <> 0
            and coalesce(n.FuelType, '') <> ''
            and n.Status = '002'
    ) n on Month between n.EventDate and n.EventEndDate
) i
where LatestEventFlag = 1
    and Month between '2024-06-01' and last_day(current_date)
    and Month = '2024-06-30'
    and i.PointOfConnectionCode = 'HAM0111'



-- COMMAND ----------

-- DBTITLE 1,Distributed Generation Capacity/Count ICP
Select * from hive_metastore.brown.retail_aggregatedDistributedGeneration 
where regiontype = 'NZ' and month = '2024-07-31' and MarketSegment = 'All'
and RetailerCode <> 'RB00' --Direct Purchasers
and RetailerCode not in ('RETA','RETB','RETC') --Not real!

-- COMMAND ----------

-- MAGIC %md #Not Considered

-- COMMAND ----------

select * from ea_prd.gold.factreconciledinjectionandofftake 
where TradingDate = '2024-06-01' and TradingPeriodNumber = 24
and FlowDirection = 'Injection'
and PointOfConnectionCode not like '%220%'
and PointOfConnectionCode not like '%110%'
and PointOfConnectionCode = 'HAM0111'

-- COMMAND ----------

-- DBTITLE 1,Comsumer Switching By Region - not used and not consistent with data above
select * from hive_metastore.brown.retail_switches
where NTEventDate between '2024-07-01' and '2024-07-31'
and TraderCodeFrom = 'MERX'
and MarketSegment = 'Com'

-- COMMAND ----------

-- DBTITLE 1,HHI Retail ??????
select RetailerCode, sum(ActiveICPs) as N from hive_metastore.brown.retail_aggregatedicpcounts 
where regiontype = 'NZ' 
and Month = '2003-12-31' 
and MarketSegment = 'All'
and RetailerCode <> 'RB00' --Direct Purchasers
and RetailerCode not in ('RETA','RETB','RETC')
group by RetailerCode

-- Select * from hive_metastore.brown.retail_aggregatedicpcounts where regiontype = 'NZ' and month = '2024-07-31' and MarketSegment = 'All'
--   and RetailerCode <> 'RB00' --Direct Purchasers
--   and RetailerCode not in ('RETA','RETB','RETC') --Not real!

-- COMMAND ----------

-- DBTITLE 1,Profit Margin - Data not available yet


-- COMMAND ----------

-- MAGIC %md ##Committed generation relative to demand growth

-- COMMAND ----------

-- DBTITLE 1,Committed investment capacity - Data not yet available


-- COMMAND ----------

-- DBTITLE 1,Generation development pace - Data not yet available


-- COMMAND ----------

-- DBTITLE 1,Required new generation by 2030 - Data not yet available.


-- COMMAND ----------

-- DBTITLE 1,Investment by participant (diversity) - Data not yet available


-- COMMAND ----------

-- MAGIC %md ##Investment in demand response

-- COMMAND ----------

-- DBTITLE 1,Dispatchable demand (DD) and Dispatch notification load (DNL) volume
--Data not yet available. (We have some information about dispatchable demand from dispatch instruction data)

-- COMMAND ----------

-- DBTITLE 1,HHI Generation by moth
With L1 as (
  select year(a.TradingDate) as `Year`, month(a.TradingDate) as `Month`, b.ParticipantShortName as Trader, sum(a.KilowattHours)/1000 as MWh 
  from ea_prd.gold.factreconciledinjectionandofftake a
  inner join ea_prd.gold.dimparticipant b
  on a.TraderCode = b.ParticipantCode
  and a.TradingDate between b.EffectiveStartDate and b.EffectiveEndDate
  where FlowDirection = 'Injection' 
  Group BY year(a.TradingDate), month(a.TradingDate), b.ParticipantShortName
), L2 as (
  select year(a.TradingDate) as `Year`, month(a.TradingDate) as `Month`, sum(a.KilowattHours)/1000 as Total_MWh 
  from ea_prd.gold.factreconciledinjectionandofftake a
  where FlowDirection = 'Injection' 
  Group BY year(a.TradingDate),month(a.TradingDate)
), L3 as(
select L1.Year, L1.Month, Trader, MWh, Total_MWh, power(100*MWh/Total_MWh,2) as HHI
From L1 inner join L2 on L1.Year = L2.Year and L1.Month = L2.Month
)
select cast(`Year` as string) as `Year`,L3.Month, round(sum(HHI),0) as HHI
from L3
Group by L3.Year,L3.Month
order by L3.Year,L3.Month
