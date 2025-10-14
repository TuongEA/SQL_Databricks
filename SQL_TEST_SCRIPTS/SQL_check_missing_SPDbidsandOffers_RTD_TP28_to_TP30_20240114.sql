-- Databricks notebook source
with bronze as (
  select 'RTD' as CaseTypeCode, count(*) as N_bronze  from ea_prd.bronze.spd_rtd_mssdata_bidsandoffers where InsertBatchLogID = '1010507962199636'
  union
  select 'PRSS' as CaseTypeCode, count(*) as N_bronze from ea_prd.bronze.spd_prss_mssdata_bidsandoffers where InsertBatchLogID = '1010507962199636'
  union
  select 'PRSL' as CaseTypeCode, count(*) as N_bronze from ea_prd.bronze.spd_prsl_mssdata_bidsandoffers where InsertBatchLogID = '1010507962199636'
  union
  select 'NRSS' as CaseTypeCode, count(*) as N_bronze from ea_prd.bronze.spd_nrss_mssdata_bidsandoffers where InsertBatchLogID = '1010507962199636'
  union
  select 'NRSL' as CaseTypeCode, count(*) as N_bronze from ea_prd.bronze.spd_nrsl_mssdata_bidsandoffers where InsertBatchLogID = '1010507962199636'
), copper as (
  select 'RTD' as CaseTypeCode, count(*) as N_copper  from ea_prd.copper.spd_rtd_mssdata_bidsandoffers where InsertBatchLogID = '1010507962199636'
  union
  select 'PRSS' as CaseTypeCode, count(*) as N_copper from ea_prd.copper.spd_prss_mssdata_bidsandoffers where InsertBatchLogID = '1010507962199636'
  union
  select 'PRSL' as CaseTypeCode, count(*) as N_copper from ea_prd.copper.spd_prsl_mssdata_bidsandoffers where InsertBatchLogID = '1010507962199636'
  union
  select 'NRSS' as CaseTypeCode, count(*) as N_copper from ea_prd.copper.spd_nrss_mssdata_bidsandoffers where InsertBatchLogID = '1010507962199636'
  union
select 'NRSL' as CaseTypeCode, count(*) as N_copper from ea_prd.copper.spd_nrsl_mssdata_bidsandoffers where InsertBatchLogID = '1010507962199636'
), copper1 as (
  select 'RTD' as CaseTypeCode, count(*) as N_copper  from ea_prd.copper.spd_rtd_mssdata_bidsandoffers where InsertBatchLogID = '1010507962199636' and SPDMaximumOutputMegawatt <> 0
  union
  select 'PRSS' as CaseTypeCode, count(*) as N_copper from ea_prd.copper.spd_prss_mssdata_bidsandoffers where InsertBatchLogID = '1010507962199636' and SPDMaximumOutputMegawatt <> 0
  union
  select 'PRSL' as CaseTypeCode, count(*) as N_copper from ea_prd.copper.spd_prsl_mssdata_bidsandoffers where InsertBatchLogID = '1010507962199636' and SPDMaximumOutputMegawatt <> 0
  union
  select 'NRSS' as CaseTypeCode, count(*) as N_copper from ea_prd.copper.spd_nrss_mssdata_bidsandoffers where InsertBatchLogID = '1010507962199636' and SPDMaximumOutputMegawatt <> 0
  union
select 'NRSL' as CaseTypeCode, count(*) as N_copper from ea_prd.copper.spd_nrsl_mssdata_bidsandoffers where InsertBatchLogID = '1010507962199636' and SPDMaximumOutputMegawatt <> 0), sensitive as (
  SELECT CaseTypeCode, count(*) as N_sensitive
  FROM ea_prd.sensitive.spdbidsandoffers
  WHERE InsertBatchLogID = '1010507962199636'
  GROUP BY CaseTypeCode
)
select b.CaseTypeCode, b.N_bronze,c.N_copper,c1.N_copper as N_copper_nonzero,s.N_sensitive
from bronze b left outer join copper c on (b.CaseTypeCode = c.CaseTypeCode)
left outer join copper1 c1 on (b.CaseTypeCode = c1.CaseTypeCode)
left outer join sensitive s on (b.CaseTypeCode = s.CaseTypeCode)

