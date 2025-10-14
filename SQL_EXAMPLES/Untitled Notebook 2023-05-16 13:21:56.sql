-- Databricks notebook source
select * from bronze.spd_nrsl_solution_constraint

-- COMMAND ----------

describe history bronze.spd_nrsl_mssdata_mndconstraint

-- COMMAND ----------

select * from copper.spd_nrsl_mssdata_mndconstraint where InsertBatchLogID = 259067851

-- COMMAND ----------

select * from bronze.spd_nrsl_mssdata_mndconstraint where InsertBatchLogID = 259067851
