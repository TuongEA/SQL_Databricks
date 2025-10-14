-- Databricks notebook source
-- MAGIC %md #Spot market size

-- COMMAND ----------

-- DBTITLE 1,Summary by year ((excluding the UTS period TP 27 to 42 of 2021-08-09)
With L1 as (
  select a.TradingDate, a.TradingPeriodNumber, a.PointOfConnectionCode, FlowDirection, 
  sum(KilowattHours/1000) as GWh, sum(KilowattHours*DollarsPerMegawattHour)/1000 as Dollars
  from gold.reconciledinjectionandofftake a inner join gold.predispatchandfinalenergyprices b
  On (a.TradingDate = b.TradingDate and a.TradingPeriodNumber = b.TradingPeriodNumber and a.PointOfConnectionCode = b.PointOfConnectionCode)
  Where PriceTypeCode = 'F' and year(a.TradingDate) >= 2010
  group by a.TradingDate, a.TradingPeriodNumber, a.PointOfConnectionCode, FlowDirection
), L2 as (
  select year(TradingDate) as `year`, round(sum(GWh),3) as Generation_GWh, round(sum(Dollars),0) as Generation_Dollars from L1
  where FlowDirection = 'Injection' Group by year(TradingDate)
), L3 as (
  select year(TradingDate) as `year`, round(sum(GWh),3) as Load_GWh, round(sum(Dollars),0) as Load_Dollars from L1
  where FlowDirection = 'Offtake' Group by year(TradingDate)
)
select L2.year, Generation_GWh, Generation_Dollars,  Load_GWh, Load_Dollars
from L2 inner join L3 on (L2.year = L3.year) order by L2.year

-- COMMAND ----------

-- MAGIC %md #FTR market size

-- COMMAND ----------

-- DBTITLE 1,Summary by year
with L0 as ( --FTR market size by contract effective year (only include net awarded contracts that are settled by FTR manager)
  select year(Start_Date) as `Year`, round(sum(FTR_Auction_Revenue_Dollars) ,0)as Auction_Revenue_Dollars, 
  round(sum(FTR_Net_Hedge_Value_Payable_Dollars),0) as FTR_Settlement_Dollars from hive_metastore.gold.ftr_monthly_rental
  group by year(Start_Date)
), L1 as ( --FTR market size by trade year (include all contracts traded between participants)
  select year(FTRTradeDate) as `Year`, round(sum(FTRAcquisitionCost),0) as X from hive_metastore.gold.ftrcontracthistory group by year(FTRTradeDate) 
), L2 as ( --FTR market size by contract effective year (include all contracts traded between participants)
  select year(FTRContractStartDate) as `Year`, round(sum(FTRAcquisitionCost),0) as X from hive_metastore.gold.ftrcontracthistory group by year(FTRContractStartDate)
)
select L0.Year, Auction_Revenue_Dollars, FTR_Settlement_Dollars, L1.X as FTR_Acquisition_Cost_by_Traded_Year_Dollars, L2.X as FTR_Acquisition_Cost_by_Contract_Year_Dollars 
from L0 inner join L1 on (L0.Year = L1.Year) 
inner join L2 on (L0.Year = L2.Year) 
order by `Year`

-- COMMAND ----------

-- MAGIC %md #Hedge market size

-- COMMAND ----------

With L1 as (
  select distinct DisclosureCode,PartyCode,ContractType,year(TradingDate) as `Year`,TradingDate,ContractEffectiveFromDate,ContractEffectiveToDate,MegawattHours,DollarsPerMegawattHour,TradedOnASXFlag
  from hive_metastore.copper.hedgecontracts where ContractStatus in ('Not disputed','Verified') and (TradedOnASXFlag is null or TradedOnASXFlag = 'N')
), otc as (
select `Year`, round(sum(MegawattHours*DollarsPerMegawattHour),0) as OTC_Value_By_Trade_Year_Dollars from L1 group by L1.Year order by L1.Year
),asx1 as (
  select year(TradeDate) as `Year`, round(sum(TradeValue),0) as ASX_Value_By_Trade_Year_Dollars from brown.forwardmarkets_tradelog group by year(TradeDate)
), asx2 as (
  select ExpiryYear as `Year`, round(sum(TradeValue),0) as ASX_Value_By_Expiry_Year_Dollars from brown.forwardmarkets_tradelog group by ExpiryYear
)
select otc.Year, ASX_Value_By_Trade_Year_Dollars,	ASX_Value_By_Expiry_Year_Dollars,	OTC_Value_By_Trade_Year_Dollars
from otc left join asx1 on otc.Year = asx1.Year left join asx2 on otc.Year = asx2.Year
order by otc.Year


-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit('Stop here!')

-- COMMAND ----------

-- MAGIC %md #Old Code

-- COMMAND ----------

-- DBTITLE 1,Electricity spot market size (excluding the UTS period TP 27 to 42 of 2021-08-09
With L1 as (
  select a.TradingDate, a.TradingPeriodNumber, a.PointOfConnectionCode, FlowDirection, 
  sum(KilowattHours/1000) as GWh, sum(KilowattHours*DollarsPerMegawattHour)/1000 as Dollars
from gold.reconciledinjectionandofftake a inner join gold.predispatchandfinalenergyprices b
On (a.TradingDate = b.TradingDate and a.TradingPeriodNumber = b.TradingPeriodNumber and a.PointOfConnectionCode = b.PointOfConnectionCode)
Where PriceTypeCode = 'F' and year(a.TradingDate) >= 2010
group by a.TradingDate, a.TradingPeriodNumber, a.PointOfConnectionCode, FlowDirection
)
select year(TradingDate) as `year`, month(TradingDate) as `month`, 
case when FlowDirection = 'Injection' then 'Generation' else 'Load' end as TradeType, round(sum(GWh),3) as GWh, round(sum(Dollars),0) as Dollars from L1
Group by year(TradingDate), month(TradingDate), FlowDirection
order by `year`, `month`, TradeType

-- COMMAND ----------

-- DBTITLE 1,Electricity spot market size (of the UTS periods TP 27 to 42 of 2021-08-09)
select year(a.TradingDate) as `year`, case when FlowDirection = 'Injection' then 'Generation' else 'Load' end as TradeType, 
  sum(KilowattHours/1000) as GWh, sum(KilowattHours*DollarsPerMegawattHour)/1000 as Dollars
from gold.reconciledinjectionandofftake a inner join gold.predispatchandfinalenergyprices b
On (a.TradingDate = b.TradingDate and a.TradingPeriodNumber = b.TradingPeriodNumber and a.PointOfConnectionCode = b.PointOfConnectionCode)
Where PriceTypeCode = 'T' and a.TradingDate = '2021-08-09' and a.TradingPeriodNumber between 37 and 42
group by year(a.TradingDate), FlowDirection

-- COMMAND ----------

-- DBTITLE 1,FTR market size by contract effective year (only include net awarded contracts that are settled by FTR manager)
select year(Start_Date) as `Year`, round(sum(FTR_Auction_Revenue_Dollars) ,0)as Auction_Revenue_Dollars, 
round(sum(FTR_Net_Hedge_Value_Payable_Dollars),0) as FTR_Settlement_Dollars from gold.ftr_monthly_rental
group by year(Start_Date)
order by `Year`

-- COMMAND ----------

-- DBTITLE 1,FTR market size by contract effective year and by trade year (include all contracts traded between participants)
With L1 as (select year(FTRTradeDate) as `Year`, round(sum(FTRAcquisitionCost),0) as X from hive_metastore.gold.ftrcontracthistory group by year(FTRTradeDate) 
), L2 as ( select year(FTRContractStartDate) as `Year`, round(sum(FTRAcquisitionCost),0) as X from hive_metastore.gold.ftrcontracthistory group by year(FTRContractStartDate)
)
select L2.Year, L1.X as FTR_Acquisition_Cost_by_Traded_Year_Dollars, L2.X as FTR_Acquisition_Cost_by_Contract_Year_Dollars 
from L1 right join L2 on (L1.Year = L2.Year) order by L2.Year

-- COMMAND ----------

-- DBTITLE 1,ASX market size
With L1 as (
  select year(TradeDate) as `Year`, round(sum(TradeValue),0) as Value_By_Trade_Year_Dollars from brown.forwardmarkets_tradelog group by year(TradeDate)
), L2 as (
  select ExpiryYear as `Year`, round(sum(TradeValue),0) as Value_By_Expiry_Year_Dollars from brown.forwardmarkets_tradelog group by ExpiryYear
)
select L2.Year, Value_By_Trade_Year_Dollars, Value_By_Expiry_Year_Dollars from L1 left join L2 on L1.Year = L2.Year
order by L2.Year


-- COMMAND ----------

-- DBTITLE 1,OTC market size (data excluding contracts traded on AXS)
with L1 as (
  select distinct DisclosureCode,PartyCode,ContractType,year(TradingDate) as `Year`,TradingDate,ContractEffectiveFromDate,ContractEffectiveToDate,MegawattHours,DollarsPerMegawattHour,TradedOnASXFlag
  from copper.hedgecontracts where ContractStatus in ('Not disputed','Verified') and (TradedOnASXFlag is null or TradedOnASXFlag = 'N')
)
select `Year`, round(sum(MegawattHours*DollarsPerMegawattHour),0) as Value_By_Trade_Year_Dollars from L1 group by L1.Year order by L1.Year


-- COMMAND ----------

With L1 as (
  select a.TradingDate, a.TradingPeriodNumber, a.PointOfConnectionCode, FlowDirection, 
  sum(KilowattHours/1000) as GWh, sum(KilowattHours*DollarsPerMegawattHour)/1000 as Dollars
from gold.reconciledinjectionandofftake a inner join gold.predispatchandfinalenergyprices b
On (a.TradingDate = b.TradingDate and a.TradingPeriodNumber = b.TradingPeriodNumber and a.PointOfConnectionCode = b.PointOfConnectionCode)
Where PriceTypeCode = 'F' and year(a.TradingDate) = 2023
group by a.TradingDate, a.TradingPeriodNumber, a.PointOfConnectionCode, FlowDirection
)
select year(TradingDate) as `year`, month(TradingDate) as `month`, 
round(sum(GWh),3) as GWh, round(sum(Dollars),0) as Dollars, round(sum(Dollars),0)/(1000*round(sum(GWh),3)) as GWAP  from L1
where  FlowDirection = 'Injection'
Group by year(TradingDate), month(TradingDate), FlowDirection
order by `year`, `month`
