-- Databricks notebook source
SET TIME ZONE 'Pacific/Auckland';

-- COMMAND ----------

-- DBTITLE 1,Viktoria's request
-- From: Viktoria Nordstrom <Viktoria.Nordstrom@ea.govt.nz> 
-- Sent: Tuesday, 3 December 2024 10:01 am
-- To: Tuong Nguyen <Tuong.Nguyen@ea.govt.nz>
-- Subject: RE: missing data in gold.marginalplant - Update on Data Transition to Unity Catalog 

-- Hi Tuong, sorry for the delayed reply.

-- Im interested in times knowing when the following plants were marginal:
-- ['Huntly 6','Huntly Rankine 1','Huntly Rankine 2','Huntly Rankine 4','Huntly 5', 'Junction Road', 'McKee',
--        'Stratford Peaker 1', 'Stratford Peaker 2',
--        'Whirinaki','Taranaki Combined Cycle']

-- During the following trading periods:  [15,16,17,18,19,20,21,22,23,35,36,37,38,39,40,41,42,43]

-- Between: 2018-2024, or if 2018 cant be achieved, at least back to 2020.

-- Thanks!
-- -Viktoria 

with L1 as (--Get caseIDs used for pricing
  select distinct CaseID from ea_prd.gold.dimspdcase where PriceTypeCode = 'F' and FirstPeriodDateTime >= '2018-01-01'
), L2 as (--Get caseIDs used for pricing
  select CaseID, to_date(TradingDate) as TradingDate, cast(TradingPeriod as Int) as TradingPeriodNumber from ea_prd.bronze.casefilepublishtimes where InterimTimeWeightSeconds <> '0'
)
select distinct a.CaseID, CaseTypeCode, TradingDate, TradingPeriodNumber, IntervalDateTime, a.PNodeCode, b.PlantName, IsMarginalFlag,
SPDClearedPriceDollarsPerMegawattHour,SPDDollarsPerMegawattHour,SPDFIRDollarsPerMegawattHour,SPDSIRDollarsPerMegawattHour,
SPDGenerationMegawatts,SPDClearedFIRMegawatts,SPDClearedSIRMegawatts,FIRRiskSetterFlag,SIRRiskSetterFlag
from ea_prd.gold.factspdmarginalenergyoffers a
inner join ea_prd.gold.dimgenerationplant b
on a.PNodeCode = b.PNodeCode
inner join L1 on L1.CaseID = a.CaseID
where TradingDate between '2018-01-01' and '2024-11-30'
and TradingPeriodNumber in (15,16,17,18,19,20,21,22,23,35,36,37,38,39,40,41,42,43)
and a.PointOfConnectionCode in ('HLY2201','JRD1101','MKE1101','SFD2201','WHI2201')
and IsMarginalFlag = 'Y'

union

select distinct a.CaseID, CaseTypeCode, a.TradingDate, a.TradingPeriodNumber, IntervalDateTime, a.PNodeCode, b.PlantName, IsMarginalFlag,
SPDClearedPriceDollarsPerMegawattHour,SPDDollarsPerMegawattHour,SPDFIRDollarsPerMegawattHour,SPDSIRDollarsPerMegawattHour,
SPDGenerationMegawatts,SPDClearedFIRMegawatts,SPDClearedSIRMegawatts,FIRRiskSetterFlag,SIRRiskSetterFlag
from ea_prd.gold.factspdmarginalenergyoffers a
inner join ea_prd.gold.dimgenerationplant b
on a.PNodeCode = b.PNodeCode
inner join L2 on L2.CaseID = a.CaseID and L2.TradingDate = a.TradingDate and L2.TradingPeriodNumber = a.TradingPeriodNumber
where a.TradingDate between '2018-01-01' and '2024-11-30'
and a.TradingPeriodNumber in (15,16,17,18,19,20,21,22,23,35,36,37,38,39,40,41,42,43)
and a.PointOfConnectionCode in ('HLY2201','JRD1101','MKE1101','SFD2201','WHI2201')
and IsMarginalFlag = 'Y'

order by TradingDate, TradingPeriodNumber,IntervalDateTime, CaseTypeCode, CaseID

-- COMMAND ----------

-- DBTITLE 1,Viktoria's request 2
-- From: Viktoria Nordstrom <Viktoria.Nordstrom@ea.govt.nz> 
-- Sent: Tuesday, 3 December 2024 3:04 pm
-- To: Tuong Nguyen <Tuong.Nguyen@ea.govt.nz>
-- Cc: Phil Bishop <Phil.Bishop@ea.govt.nz>
-- Subject: RE: missing data in gold.marginalplant - Update on Data Transition to Unity Catalog 

-- Hi Tuong, 

-- Would it actually be possible to get all the marginal units for trading periods: [15,16,17,18,19,20,21,22,23,35,36,37,38,39,40,41,42,43] between 2018 and 2024.

-- Thanks again!
-- -Viktoria 


with L1 as (--Get caseIDs used for pricing
  select distinct CaseID from ea_prd.gold.dimspdcase where PriceTypeCode = 'F' and FirstPeriodDateTime >= '2018-01-01'
), L2 as (--Get caseIDs used for pricing
  select CaseID, to_date(TradingDate) as TradingDate, cast(TradingPeriod as Int) as TradingPeriodNumber from ea_prd.bronze.casefilepublishtimes where InterimTimeWeightSeconds <> '0'
)
select distinct a.CaseID, CaseTypeCode, TradingDate, TradingPeriodNumber, IntervalDateTime, a.PNodeCode, b.PlantName, IsMarginalFlag,
SPDClearedPriceDollarsPerMegawattHour,SPDDollarsPerMegawattHour,SPDFIRDollarsPerMegawattHour,SPDSIRDollarsPerMegawattHour,
SPDGenerationMegawatts,SPDClearedFIRMegawatts,SPDClearedSIRMegawatts,FIRRiskSetterFlag,SIRRiskSetterFlag
from ea_prd.gold.factspdmarginalenergyoffers a
inner join ea_prd.gold.dimgenerationplant b
on a.PNodeCode = b.PNodeCode
inner join L1 on L1.CaseID = a.CaseID
where TradingDate between '2018-01-01' and '2024-11-30'
and TradingPeriodNumber in (15,16,17,18,19,20,21,22,23,35,36,37,38,39,40,41,42,43)
-- and a.PointOfConnectionCode in ('HLY2201','JRD1101','MKE1101','SFD2201','WHI2201')
and IsMarginalFlag = 'Y'

union

select distinct a.CaseID, CaseTypeCode, a.TradingDate, a.TradingPeriodNumber, IntervalDateTime, a.PNodeCode, b.PlantName, IsMarginalFlag,
SPDClearedPriceDollarsPerMegawattHour,SPDDollarsPerMegawattHour,SPDFIRDollarsPerMegawattHour,SPDSIRDollarsPerMegawattHour,
SPDGenerationMegawatts,SPDClearedFIRMegawatts,SPDClearedSIRMegawatts,FIRRiskSetterFlag,SIRRiskSetterFlag
from ea_prd.gold.factspdmarginalenergyoffers a
inner join ea_prd.gold.dimgenerationplant b
on a.PNodeCode = b.PNodeCode
inner join L2 on L2.CaseID = a.CaseID and L2.TradingDate = a.TradingDate and L2.TradingPeriodNumber = a.TradingPeriodNumber
where a.TradingDate between '2018-01-01' and '2024-11-30'
and a.TradingPeriodNumber in (15,16,17,18,19,20,21,22,23,35,36,37,38,39,40,41,42,43)
--and a.PointOfConnectionCode in ('HLY2201','JRD1101','MKE1101','SFD2201','WHI2201')
and IsMarginalFlag = 'Y'

order by TradingDate, TradingPeriodNumber,IntervalDateTime, CaseTypeCode, CaseID
