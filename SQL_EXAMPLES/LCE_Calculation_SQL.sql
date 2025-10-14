-- Databricks notebook source
-- DBTITLE 1,Estimate LCE for Aug 2021
WITH L1 AS ( 
SELECT TradingDate, TradingPeriodNumber, PointOfConnectionCode, DollarsPerMegawattHour, PriceTypeShortName 
FROM gold.predispatchandfinalenergyprices 
WHERE TradingDate between '2021-08-01' and '2021-08-31' AND PriceTypeShortName ='Final' 
), L2 AS ( 
SELECT a.TradingDate, a.TradingPeriodNumber, a.PointOfConnectionCode, a.DollarsPerMegawattHour, a.PriceTypeShortName
FROM gold.predispatchandfinalenergyprices a
LEFT JOIN L1 b
On (a.TradingDate = b.TradingDate
AND a.TradingPeriodNumber = b.TradingPeriodNumber
AND a.PointOfConnectionCode = b.PointOfConnectionCode)
WHERE a.TradingDate between '2021-08-01' and '2021-08-31' 
AND a.PriceTypeShortName ='Interim' 
AND b.PriceTypeShortName is Null
UNION 
SELECT * FROM L1
), L3 AS (
SELECT a.TradingDate, a.TradingPeriodNumber, a.PointOfConnectionCode, a.DollarsPerMegawattHour, a.PriceTypeShortName
FROM gold.predispatchandfinalenergyprices a
LEFT JOIN L2 b
On (a.TradingDate = b.TradingDate
AND a.TradingPeriodNumber = b.TradingPeriodNumber
AND a.PointOfConnectionCode = b.PointOfConnectionCode)
WHERE a.TradingDate between '2021-08-01' and '2021-08-31' 
AND a.PriceTypeShortName ='Provisional' 
AND b.PriceTypeShortName is Null
UNION 
SELECT * FROM L2
), L4 AS (
SELECT a.PointOfConnectionCode, a.TradingDate, a.TradingPeriodNumber, 
FlowDirection, KilowattHours/1000 as MegawattHours, DollarsPerMegawattHour,
Case FlowDirection
  When 'Offtake' Then DollarsPerMegawattHour * KilowattHours/1000
Else -DollarsPerMegawattHour * KilowattHours/1000
End as Amount
FROM gold.reconciledinjectionandofftake a INNER JOIN L3 b
ON (KilowattHours  <> 0
AND a.TradingDate = b.TradingDate
AND a.TradingPeriodNumber = b.TradingPeriodNumber
AND a.PointOfConnectionCode = b.PointOfConnectionCode)
)
SELECT sum(Amount) FROM L4

-- COMMAND ----------

-- DBTITLE 1,Estimate LCE for Jan 2022
WITH L1 AS ( 
SELECT TradingDate, TradingPeriodNumber, PointOfConnectionCode, DollarsPerMegawattHour, PriceTypeShortName 
FROM gold.predispatchandfinalenergyprices 
WHERE TradingDate between '2022-01-01' and '2022-01-31' AND PriceTypeShortName ='Final' 
), L2 AS ( 
SELECT a.TradingDate, a.TradingPeriodNumber, a.PointOfConnectionCode, a.DollarsPerMegawattHour, a.PriceTypeShortName
FROM gold.predispatchandfinalenergyprices a
LEFT JOIN L1 b
On (a.TradingDate = b.TradingDate
AND a.TradingPeriodNumber = b.TradingPeriodNumber
AND a.PointOfConnectionCode = b.PointOfConnectionCode)
WHERE a.TradingDate between '2022-01-01' and '2022-01-31' 
AND a.PriceTypeShortName ='Interim' 
AND b.PriceTypeShortName is Null
UNION 
SELECT * FROM L1
), L3 AS (
SELECT a.TradingDate, a.TradingPeriodNumber, a.PointOfConnectionCode, a.DollarsPerMegawattHour, a.PriceTypeShortName
FROM gold.predispatchandfinalenergyprices a
LEFT JOIN L2 b
On (a.TradingDate = b.TradingDate
AND a.TradingPeriodNumber = b.TradingPeriodNumber
AND a.PointOfConnectionCode = b.PointOfConnectionCode)
WHERE a.TradingDate between '2022-01-01' and '2022-01-31' 
AND a.PriceTypeShortName ='Provisional' 
AND b.PriceTypeShortName is Null
UNION 
SELECT * FROM L2
), L4 AS (
SELECT a.PointOfConnectionCode, a.TradingDate, a.TradingPeriodNumber, 
FlowDirection, KilowattHours/1000 as MegawattHours, DollarsPerMegawattHour,
Case FlowDirection
  When 'Offtake' Then DollarsPerMegawattHour * KilowattHours/1000
Else -DollarsPerMegawattHour * KilowattHours/1000
End as Amount
FROM gold.reconciledinjectionandofftake a INNER JOIN L3 b
ON (KilowattHours  <> 0
AND a.TradingDate = b.TradingDate
AND a.TradingPeriodNumber = b.TradingPeriodNumber
AND a.PointOfConnectionCode = b.PointOfConnectionCode)
)
SELECT sum(Amount) FROM L4

-- COMMAND ----------

-- DBTITLE 1,Estimate LCE for Feb 2022
WITH L1 AS ( 
SELECT TradingDate, TradingPeriodNumber, PointOfConnectionCode, DollarsPerMegawattHour, PriceTypeShortName 
FROM gold.predispatchandfinalenergyprices 
WHERE TradingDate between '2022-02-01' and '2022-02-28' AND PriceTypeShortName ='Final' 
), L2 AS ( 
SELECT a.TradingDate, a.TradingPeriodNumber, a.PointOfConnectionCode, a.DollarsPerMegawattHour, a.PriceTypeShortName
FROM gold.predispatchandfinalenergyprices a
LEFT JOIN L1 b
On (a.TradingDate = b.TradingDate
AND a.TradingPeriodNumber = b.TradingPeriodNumber
AND a.PointOfConnectionCode = b.PointOfConnectionCode)
WHERE a.TradingDate between '2022-02-01' and '2022-02-28' 
AND a.PriceTypeShortName ='Interim' 
AND b.PriceTypeShortName is Null
UNION 
SELECT * FROM L1
), L3 AS (
SELECT a.TradingDate, a.TradingPeriodNumber, a.PointOfConnectionCode, a.DollarsPerMegawattHour, a.PriceTypeShortName
FROM gold.predispatchandfinalenergyprices a
LEFT JOIN L2 b
On (a.TradingDate = b.TradingDate
AND a.TradingPeriodNumber = b.TradingPeriodNumber
AND a.PointOfConnectionCode = b.PointOfConnectionCode)
WHERE a.TradingDate between '2022-02-01' and '2022-02-28' 
AND a.PriceTypeShortName ='Provisional' 
AND b.PriceTypeShortName is Null
UNION 
SELECT * FROM L2
), L4 AS (
SELECT a.PointOfConnectionCode, a.TradingDate, a.TradingPeriodNumber, 
FlowDirection, KilowattHours/1000 as MegawattHours, DollarsPerMegawattHour,
Case FlowDirection
  When 'Offtake' Then DollarsPerMegawattHour * KilowattHours/1000
Else -DollarsPerMegawattHour * KilowattHours/1000
End as Amount
FROM gold.reconciledinjectionandofftake a INNER JOIN L3 b
ON (KilowattHours  <> 0
AND a.TradingDate = b.TradingDate
AND a.TradingPeriodNumber = b.TradingPeriodNumber
AND a.PointOfConnectionCode = b.PointOfConnectionCode)
)
SELECT sum(Amount) FROM L4

-- COMMAND ----------

-- DBTITLE 1,Estimate monthly CLE (don't use, too slow)
WITH L1 AS ( 
SELECT TradingDateYear, TradingDateMonthNumber, TradingDate, TradingPeriodNumber, 
PointOfConnectionCode, DollarsPerMegawattHour, PriceTypeShortName 
FROM gold.predispatchandfinalenergyprices 
WHERE TradingDateYear = 2021 AND PriceTypeShortName ='Final' 
), L2 AS ( 
SELECT a.TradingDateYear, a.TradingDateMonthNumber, a.TradingDate, a.TradingPeriodNumber, 
a.PointOfConnectionCode, a.DollarsPerMegawattHour, a.PriceTypeShortName
FROM gold.predispatchandfinalenergyprices a
LEFT JOIN L1 b
On (a.TradingDate = b.TradingDate
AND a.TradingPeriodNumber = b.TradingPeriodNumber
AND a.PointOfConnectionCode = b.PointOfConnectionCode)
WHERE a.TradingDateYear = 2021 
AND a.PriceTypeShortName ='Interim' 
AND b.PriceTypeShortName is Null
UNION 
SELECT * FROM L1
), L3 AS (
SELECT a.TradingDateYear, a.TradingDateMonthNumber,a.TradingDate, a.TradingPeriodNumber, 
a.PointOfConnectionCode, a.DollarsPerMegawattHour, a.PriceTypeShortName
FROM gold.predispatchandfinalenergyprices a
LEFT JOIN L2 b
On (a.TradingDate = b.TradingDate
AND a.TradingPeriodNumber = b.TradingPeriodNumber
AND a.PointOfConnectionCode = b.PointOfConnectionCode)
WHERE a.TradingDateYear = 2021
AND a.PriceTypeShortName ='Provisional' 
AND b.PriceTypeShortName is Null
UNION 
SELECT * FROM L2
), L4 AS (
SELECT a.PointOfConnectionCode, a.TradingDateYear, a.TradingDateMonthNumber, 
a.TradingDate, a.TradingPeriodNumber, 
FlowDirection, KilowattHours/1000 as MegawattHours, DollarsPerMegawattHour,
Case FlowDirection
  When 'Offtake' Then DollarsPerMegawattHour * KilowattHours/1000
Else -DollarsPerMegawattHour * KilowattHours/1000
End as Amount
FROM gold.reconciledinjectionandofftake a INNER JOIN L3 b
ON (KilowattHours  <> 0
AND a.TradingDate = b.TradingDate
AND a.TradingPeriodNumber = b.TradingPeriodNumber
AND a.PointOfConnectionCode = b.PointOfConnectionCode)
)
SELECT TradingDateYear, TradingDateMonthNumber, sum(Amount) FROM L4
GROUP BY TradingDateYear, TradingDateMonthNumber
ORDER BY TradingDateYear, TradingDateMonthNumber
