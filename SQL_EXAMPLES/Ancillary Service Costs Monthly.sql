-- Databricks notebook source
-- DBTITLE 1,Frequency Keeping Costs
select date_format(TradingDate, 'yyyyMM') as monthID, ParticipantCode, ConstraintDirectionIndicator, sum(Dollars) as Dollars
from copper.fkcons
where month(TradingDate) = 3 and year(TradingDate) = 2022
group by date_format(TradingDate, 'yyyyMM'), ParticipantCode, ConstraintDirectionIndicator

-- COMMAND ----------

-- DBTITLE 1,FrequencyKeeping Market Cost
select date_format(TradingDate, 'yyyyMM') as monthID, ParticipantCode, sum(Dollars) as Dollars
from gold.frequencykeepingdispatchedoffers
where month(TradingDate) = 4 and year(TradingDate) = 2022
group by date_format(TradingDate, 'yyyyMM'), ParticipantCode

-- COMMAND ----------

-- DBTITLE 1,ConstOn Res
select  date_format(TradingDate, 'yyyyMM') as monthID, ParticipantCode,sum(Dollars) as Dollars 
from copper.rescons
where month(TradingDate) = 3 and year(TradingDate) = 2020
group by date_format(TradingDate, 'yyyyMM'), ParticipantCode
order by ParticipantCode

-- COMMAND ----------

select * from gold.predispatchandfinalreserveprices where PriceTypeCode = "F" and TradingDate between '2023-01-01' and '2023-01-31'

-- COMMAND ----------

SET TIME ZONE 'Pacific/Auckland';
select TradingDate, TradingPeriodNumber,  DispatchInstructionSentDateTime, ParticipantCode,PNodeCode,ReserveTypeCode,ReserveCategoryCode, Megawatts, * 
from gold.dispatchinstructions 
where TradingDate between '2023-01-01' and '2023-01-31' and DispatchTypeCode <> 'MW' and ParticipantCode = 'CTCT' and ReserveCategoryCode = 'IL'
order by TradingDate, TradingPeriodNumber, ParticipantCode,PNodeCode, DispatchInstructionSentDateTime

-- COMMAND ----------

select * from copper.ddcons

-- COMMAND ----------

select * from copper.econs

-- COMMAND ----------

select * from bronze.cons_cost

-- COMMAND ----------

select * from bronze.res_cost where MONTH_DATE = '01/12/2022'

-- COMMAND ----------

-- Assuming you have a list of CSV files in a directory
DROP TABLE IF EXISTS temp_fkcons_cost;
CREATE TABLE temp_fkcons_cost
USING CSV
OPTIONS (
  'header' 'true',
  'inferSchema' 'true'
)
LOCATION '/mnt/eadatalakeprd/datalake/FKCONS_COST'

-- COMMAND ----------

select * from temp_fkcons_cost where ORG_CODE in ('CTCT','CTCS') and MONTH_DATE = '01/03/2022'
