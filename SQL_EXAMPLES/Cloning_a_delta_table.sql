-- Databricks notebook source
-- DBTITLE 1,This will copy the current version of vspd.spdcase and create an independent delta table
CREATE OR REPLACE TABLE ea_dev.configuration.loadconfiguration CLONE ea_prd.configuration.loadconfiguration

-- COMMAND ----------

describe detail vspd.spdcase

-- COMMAND ----------

describe detail silver.dimspdcase
