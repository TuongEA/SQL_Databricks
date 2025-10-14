-- Databricks notebook source
SET TIME ZONE 'Pacific/Auckland';

-- COMMAND ----------

-- UPDATE ea_prd.configuration.loadconfiguration SET OutputPartitionColumn = '' WHERE LoadConfigurationID = 350

-- COMMAND ----------

select * from ea_dev.configuration.loadconfiguration 
where LoadConfigurationID >= (select max(LoadConfigurationID) from ea_dev.configuration.loadconfiguration)-1

-- COMMAND ----------

-- UPDATE ea_dev.configuration.loadconfiguration SET 
-- INputschema = 'Date STRING,   FOBMelawanDollarsPerGJ STRING,  CarbonExclusiveCoalSRMCviaAUckland STRING,   FOBtoDESPriceUplift STRING',
-- IsSchemaSpecifiedFlag = true

-- WHERE LoadConfigurationID = 447

-- COMMAND ----------

-- Insert INTO ea_dev.configuration.loadconfiguration
select 448 as LoadConfigurationID,
"mbieweeklyfuelprices" as FileType,
"MBIE weekly fuel prices - week end Friday" as Description,
"mbieweeklyfuelprices*.csv" as FileMasks,
"Download from https://www.mbie.govt.nz/assets/Data-Files/Energy/Weekly-fuel-price-monitoring/weekly-table.csv" as Source,
"2024-07-01" as StartDate,
null as EndDate,
"N/A" as SchedulingGroup,
SchedulingSubgroup,
BronzeWorkbook,SilverWorkbook, GoldWorkbook,
true as InputFileHasHeaders,
"Append" as OutputMode,
OutputPartitionColumn,
false as IsSchemaSpecifiedFlag,
null as inputSchema,
RawDataDFConfig,
current_date() as InsertedDate,
"tuong.nguyen@ea.govt.nz" as InsertedBy
from ea_dev.configuration.loadconfiguration where LoadConfigurationID = 447
