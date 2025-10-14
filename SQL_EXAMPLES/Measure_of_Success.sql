-- Databricks notebook source
SET TIME ZONE 'Pacific/Auckland';

-- COMMAND ----------

-- DBTITLE 1,HHI Generation by year
With L1 as (
  select year(a.TradingDate) as Y, b.ParticipantShortName as Trader, sum(a.KilowattHours)/1000 as MWh 
  from ea_prd.gold.factreconciledinjectionandofftake a
  inner join ea_prd.gold.dimparticipant b
  on a.TraderCode = b.ParticipantCode
  and a.TradingDate between b.EffectiveStartDate and b.EffectiveEndDate
  where FlowDirection = 'Injection' and a.TradingDate >= '2010-01-01'
  Group BY year(a.TradingDate), b.ParticipantShortName
), L2 as (
  select year(a.TradingDate) as Y,MAx(TRadingDate) as Date,  sum(a.KilowattHours)/1000 as Total_MWh 
  from ea_prd.gold.factreconciledinjectionandofftake a
  where FlowDirection = 'Injection' and a.TradingDate >= '2010-01-01'
  Group BY year(a.TradingDate)
), L3 as(
select L1.Y, L2.Date,Trader, MWh, Total_MWh, power(100*MWh/Total_MWh,2) as HHI
From L1 inner join L2 on L1.Y = L2.Y
)
select Date, 'HHI for Generation' as Measure, 'Year' as Duration, round(sum(HHI),0) as Value
from L3
Group by L3.Date
order by L3.Date 

-- COMMAND ----------

-- DBTITLE 1,HHI Generation by Quarter
With L1 as (
  select year(a.TradingDate) as Y, quarter(a.TradingDate) as Q, b.ParticipantShortName as Trader, sum(a.KilowattHours)/1000 as MWh 
  from ea_prd.gold.factreconciledinjectionandofftake a
  inner join ea_prd.gold.dimparticipant b
  on a.TraderCode = b.ParticipantCode
  and a.TradingDate between b.EffectiveStartDate and b.EffectiveEndDate
  where FlowDirection = 'Injection' 
  Group BY year(a.TradingDate),quarter(a.TradingDate), b.ParticipantShortName
), L2 as (
  select year(a.TradingDate) as Y, quarter(a.TradingDate) as Q, MAx(TRadingDate) as Date,  sum(a.KilowattHours)/1000 as Total_MWh 
  from ea_prd.gold.factreconciledinjectionandofftake a
  where FlowDirection = 'Injection' and a.TradingDate >= '2010-01-01'
  Group BY year(a.TradingDate),quarter(a.TradingDate)
), L3 as(
select L2.Date,Trader, MWh, Total_MWh, power(100*MWh/Total_MWh,2) as HHI
From L1 inner join L2 on L1.Y = L2.Y and L1.Q = L2.Q
)
select Date, 'HHI for Generation' as Measure, 'Quarter' as Duration, round(sum(HHI),0) as Value
from L3
Group by L3.Date
order by L3.Date 

-- COMMAND ----------

-- DBTITLE 1,Number of independent generators by Year
With L1 as (
  select Year(TradingDate) as Y, b.ParticipantLongName, max(TradingDate) as `Date`
  from ea_prd.gold.factreconciledinjectionandofftake a inner join  ea_prd.gold.dimparticipant b 
  on (a.TraderCode = b.ParticipantCode and a.TradingDate between b.EffectiveStartDate and b.EffectiveEndDate)
  where FlowDirection = "Injection" and KilowattHours > 0 and a.TradingDate >= '2010-01-01'
  group by Year(TradingDate), b.ParticipantLongName
), L2 as (select Y, max(Date) as `Date`, count(*) as Value from L1 group by L1.Y) 
select Date, 'Number of Independnt Generators' as Measure, 'Year' as Duration,  Value from L2 order by L2.`Date`

-- COMMAND ----------

-- DBTITLE 1,Number of independent generators by Quarter
With L1 as (
  select Year(TradingDate) as Y, quarter(TradingDate) as Q, b.ParticipantLongName, max(TradingDate) as `Date`
  from ea_prd.gold.factreconciledinjectionandofftake a inner join  ea_prd.gold.dimparticipant b 
  on (a.TraderCode = b.ParticipantCode and a.TradingDate between b.EffectiveStartDate and b.EffectiveEndDate)
  where FlowDirection = "Injection" and KilowattHours > 0 and a.TradingDate >= '2010-01-01'
  group by Year(TradingDate), quarter(TradingDate),b.ParticipantLongName
), L2 as (select Y,Q, max(Date) as `Date`, count(*) as Value from L1 group by Y,Q)
select Date, 'Number of Independnt Generators' as Measure, 'Quarter' as Duration,  Value from L2 order by L2.`Date`

-- COMMAND ----------

-- DBTITLE 1,Price Volatility - Price, Standard Deviation, 5th & 95th Percentile - Quarter
With L1 as (Select b.CalendarYear, b.CalendarQuarterNumber, PointOfConnectionCode,
max(TRadingDate) as `Date`, avg(DollarsPerMegawattHour) as Value_avg, stddev(DollarsPerMegawattHour) as Value_stddev,
percentile(DollarsPerMegawattHour, 0.05) as Value_5th, percentile(DollarsPerMegawattHour, 0.95) as Value_95th
from ea_prd.gold.factpredispatchandfinalenergyprices a 
inner join ea_prd.gold.dimdate b on a.TradingDate = b.ActualDate 
where TRadingDate >= '2010-01-01' and PointOfConnectionCode in ('OTA2201','BEN2201') and PriceTypeCode = 'F' 
group by b.CalendarYear,b.CalendarQuarterNumber,PointOfConnectionCode)
select `Date`, 'OTA average price' as Measure, 'Quarter' as Duration, round(Value_avg,2) as Value from L1 where PointOfConnectionCode = 'OTA2201'
union
select `Date`, 'BEN average price' as Measure, 'Quarter' as Duration, round(Value_avg,2) as Value from L1 where PointOfConnectionCode = 'BEN2201'
union
select `Date`, 'OTA price standard deviation' as Measure, 'Quarter' as Duration, round(Value_stddev,2) as Value from L1 where PointOfConnectionCode = 'OTA2201'
union
select `Date`, 'BEN price standard deviation' as Measure, 'Quarter' as Duration, round(Value_stddev,2) as Value from L1 where PointOfConnectionCode = 'BEN2201'
union
select `Date`, 'OTA 5th percentile price' as Measure, 'Quarter' as Duration, round(Value_5th,2) as Value from L1 where PointOfConnectionCode = 'OTA2201'
union
select `Date`, 'BEN 5th percentile price' as Measure, 'Quarter' as Duration, round(Value_5th,2) as Value from L1 where PointOfConnectionCode = 'BEN2201'
union
select `Date`, 'OTA 95th percentile price' as Measure, 'Quarter' as Duration, round(Value_95th,2) as Value from L1 where PointOfConnectionCode = 'OTA2201'
union
select `Date`, 'BEN 95th percentile price' as Measure, 'Quarter' as Duration, round(Value_95th,2) as Value from L1 where PointOfConnectionCode = 'BEN2201'

order by Measure, `Date`

-- COMMAND ----------

-- DBTITLE 1,Price Volatility - Price, Standard Deviation, 5th & 95th Percentile - Yearly
With L1 as (
  Select b.CalendarYear, a.PointOfConnectionCode, max(TRadingDate) as `Date`, 
  avg(DollarsPerMegawattHour) as Value_avg, stddev(DollarsPerMegawattHour) as Value_stddev,
  percentile(DollarsPerMegawattHour, 0.05) as Value_5th, percentile(DollarsPerMegawattHour, 0.95) as Value_95th
  from ea_prd.gold.factpredispatchandfinalenergyprices a 
  inner join ea_prd.gold.dimdate b on a.TradingDate = b.ActualDate 
  where TRadingDate >= '2010-01-01' and PointOfConnectionCode in ('BEN2201','OTA2201') and PriceTypeCode = 'F' 
  group by b.CalendarYear,a.PointOfConnectionCode
)
select `Date`, 'OTA average price' as Measure, 'Year' as Duration, round(Value_avg,2) as Value from L1 where PointOfConnectionCode = 'OTA2201'
union
select `Date`, 'BEN average price' as Measure, 'Year' as Duration, round(Value_avg,2) as Value from L1 where PointOfConnectionCode = 'BEN2201'
union
select `Date`, 'OTA price standard deviation' as Measure, 'Year' as Duration, round(Value_stddev,2) as Value from L1 where PointOfConnectionCode = 'OTA2201'
union
select `Date`, 'BEN price standard deviation' as Measure, 'Year' as Duration, round(Value_stddev,2) as Value from L1 where PointOfConnectionCode = 'BEN2201'
union
select `Date`, 'OTA 5th percentile price' as Measure, 'Year' as Duration, round(Value_5th,2) as Value from L1 where PointOfConnectionCode = 'OTA2201'
union
select `Date`, 'BEN 5th percentile price' as Measure, 'Year' as Duration, round(Value_5th,2) as Value from L1 where PointOfConnectionCode = 'BEN2201'
union
select `Date`, 'OTA 95th percentile price' as Measure, 'Year' as Duration, round(Value_95th,2) as Value from L1 where PointOfConnectionCode = 'OTA2201'
union
select `Date`, 'BEN 95th percentile price' as Measure, 'Year' as Duration, round(Value_95th,2) as Value from L1 where PointOfConnectionCode = 'BEN2201'
order by Measure, `Date`

-- COMMAND ----------

-- DBTITLE 1,Volume transacted – OTC
with L1 as (Select Year(TradingDate) as Y, quarter(TradingDate) as Q, round(sum(MegawattHours)/1000,3) as Value
from hive_metastore.sensitive.hedgedisclosures 
where CounterPartyNameAsMapped not like '%ASX%'
and DisclosureUpdateOrder = 1
and ContractStatus = 'Verified'
-- and ForAllTradingPeriodsFlag = 'Y'  
-- and InstrumentCode = 'CfD'
-- and ZoneCode = 'UNI'
-- and EffectiveToDate <= TradingDate + interval 1 year
and TradingDate >= '2010-01-01'
group by Year(TradingDate), quarter(TradingDate))
select max(ActualDate) as `Date`, 'OTC traded volume GWh' as Measure, 'Quarter' as Duration, Value 
from L1 inner join ea_prd.gold.dimdate 
on L1.Y = ea_prd.gold.dimdate.CalendarYear and L1.Q = ea_prd.gold.dimdate.CalendarQuarterNumber
group by Y, Q, Value
order by `Date`
