-- Databricks notebook source
-- DBTITLE 1,Retail HHI
Select * from hive_metastore.brown.retail_aggregatedhhiandcr where regiontype = 'NZ' and marketSegment = 'All'  order by Month desc limit 1


-- COMMAND ----------

-- DBTITLE 1,Retail firms/traders
Select * from hive_metastore.brown.retail_aggregatedicpcounts where regiontype = 'NZ' and month = '2024-07-31' and MarketSegment = 'All'
  and RetailerCode <> 'RB00' --Direct Purchasers
  and RetailerCode not in ('RETA','RETB','RETC') --Not real!

-- COMMAND ----------

-- DBTITLE 1,Switches
Select * from hive_metastore.brown.retail_aggregatedswitchcounts where regiontype = 'NZ' and month = '2024-07-31' and MarketSegment = 'All'
  and RetailerCode <> 'RB00' --Direct Purchasers
  and RetailerCode not in ('RETA','RETB','RETC') --Not real!

  

-- COMMAND ----------

-- DBTITLE 1,DG (Capacity/Counts only)
Select * from hive_metastore.brown.retail_aggregatedDistributedGeneration 
where regiontype = 'NZ' and month = '2024-07-31' and MarketSegment = 'All'
and RetailerCode <> 'RB00' --Direct Purchasers
and RetailerCode not in ('RETA','RETB','RETC') --Not real!

-- COMMAND ----------

-- DBTITLE 1,ASX/Forward prices
Select * from hive_metastore.brown.forwardmarkets_closingsnapshot

-- COMMAND ----------

-- DBTITLE 1,ASX Traded volumes, by contract type (EMI)
Select * from hive_metastore.brown.forwardmarkets_tradelog

-- COMMAND ----------

-- DBTITLE 1,ASX Traded volumes, by participant (confidential)
Select * from sensitive.forwardmarketsdeanonymisedtrades

-- COMMAND ----------

-- DBTITLE 1,OTC/Hedges
Select * from sensitive.hedgedisclosures

-- COMMAND ----------



-- COMMAND ----------


