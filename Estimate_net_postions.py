# Databricks notebook source
import pyspark.sql.types as T
import pyspark.sql.functions as F
import pyspark.sql.window as wd
from delta.tables import *
import datetime as dt
import pytz
import os
import glob 

# COMMAND ----------

Testing = False
startdate = '2021-08-09'
enddate = '2021-08-09'
startprd = 37
endperd = 42 

FactualCase = False # Set this to true to use final/Interim price for Factual Case. Otherwise, counter factual prices will be used


# COMMAND ----------

# MAGIC %md #PRICES and RECONCILED DATA

# COMMAND ----------

# DBTITLE 1,Get reserve prices
sqlquery = "\
SELECT TradingDate, TradingPeriodNumber, PointOfConnectionCode, FIRDollarsPerMegawattHour, SIRDollarsPerMegawattHour, PriceTypeCode \
FROM gold.predispatchandfinalreserveprices \
WHERE TradingDate between '" + startdate + "' and '" + enddate + "' " \
"AND TradingPeriodNumber between '" + str(startprd) + "' and '" + str(endperd) + "' " \
"AND PriceTypeShortName in ('Interim','Final')" \
"ORDER BY TradingPeriodNumber"

df_resv_prices = spark.sql(sqlquery)

if Testing:
  display(df_resv_prices)
               

# COMMAND ----------

# DBTITLE 1,Get study periods
studied_periods = df_resv_prices.select(['TradingDate','TradingPeriodNumber']).distinct()
if Testing:
  studied_periods.show()

# COMMAND ----------

# DBTITLE 1,Get energy prices
if FactualCase:
  sqlquery = "\
  SELECT TradingDate, TradingPeriodNumber, PointOfConnectionCode, DollarsPerMegawattHour,PriceTypeCode \
  FROM gold.predispatchandfinalenergyprices \
  WHERE TradingDate between '" + startdate + "' and '" + enddate + "' " \
  "AND TradingPeriodNumber between '" + str(startprd) + "' and '" + str(endperd) + "' " \
  "AND PriceTypeShortName in ('Interim','Final')" \
  "ORDER BY TradingPeriodNumber"
else: # Using prices from FP caseID '81112021081200786' if counter factual
  sqlquery = "\
  SELECT Distinct TradingDate, TradingPeriodNumber, PointOfConnectionCode, \
  SPDDollarsPerMegawattHour as DollarsPerMegawattHour, \
  'Non-Scarcity' as PriceTypeCode FROM sensitive.spdnodal \
  WHERE TradingDate between '" + startdate + "' and '" + enddate + "' " \
  "AND TradingPeriodNumber between '" + str(startprd) + "' and '" + str(endperd) + "' " \
  "AND CaseTypeCode = 'FP' AND CaseID = '81112021081200786'" \
  "ORDER BY TradingPeriodNumber"

df_enrg_prices = spark.sql(sqlquery)

if Testing:
  display(df_enrg_prices)
               

# COMMAND ----------

# DBTITLE 1,Get Physical Generation/Demand
sqlquery = "\
SELECT ParticipantShortName, TradingDate, TradingPeriodNumber,PointOfConnectionCode,FlowDirection, sum(KilowattHours)/500 as Megawatts \
FROM gold.reconciledinjectionandofftake \
WHERE TradingDate between '" + startdate + "' and '" + enddate + "' " \
"AND TradingPeriodNumber between '" + str(startprd) + "' and '" + str(endperd) + "' " \
"AND KilowattHours > 0 " \
"GROUP BY ParticipantShortName, TradingDate, TradingPeriodNumber,PointOfConnectionCode,FlowDirection " \
"ORDER BY TradingPeriodNumber"

df_reconciled = spark.sql(sqlquery)

# Note that MERI sold 400MW baseload contract to Smelter at TWI until 31 Dec 2024 -- let split 400 MW of TWI load to MERI
# Reference: NZAS-Contract-Consolidated-and-Redacted-v3.pdf
df_smelter_meri = (df_reconciled
                   .filter("PointOfConnectionCode == 'TWI2201' and ParticipantShortName = 'NZ Aluminium Smelters' \
                           and FlowDirection = 'Offtake' and TradingDate <= '2024-12-31'")
                   .withColumn("ParticipantShortName", F.lit('Meridian Energy'))
                   .withColumn("Megawatts", F.when(F.col("Megawatts") > 400, 400).otherwise(F.col("Megawatts")))
                  )

df_reconciled = (df_reconciled
                 .withColumn("Megawatts", F.when(F.expr("PointOfConnectionCode == 'TWI2201' and ParticipantShortName = 'NZ Aluminium Smelters' \
                                                         and FlowDirection = 'Offtake' and TradingDate <= '2024-12-31'"), 
                                                 F.when(F.col("Megawatts") <= 400, 0).otherwise(F.col("Megawatts") - 400) 
                                                ).otherwise(F.col("Megawatts"))
                            )
                 .union(df_smelter_meri)
                )

df_reconciled = (df_reconciled
                 .withColumnRenamed("ParticipantShortName", "TraderName")
                 .withColumn("Market", F.lit("Physical"))
                 .withColumn("ContractType", F.when(F.col('FlowDirection')=='Injection',F.lit('Generation')).otherwise(F.lit('Load')))
                 .withColumn("Megawatts", F.when(F.col('FlowDirection')=='Injection',F.col('Megawatts')).otherwise(-F.col('Megawatts')))
                 .join(df_enrg_prices, on = ['TradingDate','TradingPeriodNumber','PointOfConnectionCode'], how = 'inner')
                 .select(['Market','TraderName','TradingDate','TradingPeriodNumber','ContractType','PointOfConnectionCode','Megawatts','DollarsPerMegawattHour'])
                )
if Testing:
  display(df_reconciled.filter("PointOfConnectionCode in ('TWI2201','MAN2201')"))
               

# COMMAND ----------

# DBTITLE 1,Calculate Physical Positions
df_physical_summary = (df_reconciled
                       .groupby(['Market','TraderName','ContractType'])
                       .agg(F.sum(F.col('Megawatts') * F.col('DollarsPerMegawattHour') / 2 )
                            .cast(T.DecimalType(15,2))
                            .alias('Dollars'))
                      )
if Testing:
  display(df_physical_summary)

# COMMAND ----------

# MAGIC %md #HEDGE DISCLOSURE DATA

# COMMAND ----------

sqlquery = "With L0 AS ( \
SELECT DisclosureCode, max(DisclosureUpdatedDatetime) as DisclosureUpdatedDatetime \
FROM  processing.copper_hedgecontracts GROUP BY DisclosureCode) \
SELECT DISTINCT a.*, ParticipantShortName as PartyShortName  \
FROM processing.copper_hedgecontracts a \
INNER JOIN L0 b \
ON (a.DisclosureCode = b.DisclosureCode \
and a.DisclosureUpdatedDatetime = b.DisclosureUpdatedDatetime) \
LEFT JOIN processing.silver_dimparticipant c On (a.PartyCode = c.ParticipantCode) \
WHERE ContractStatus <> 'Disputed' AND ContractEffectiveToDate >= '" + startdate + "' AND ContractEffectiveFromDate <= '" + enddate + "' \
AND a.DisclosureCode not in ('21478','21486','21522','21488')" # These are VAS contracts and will be handle seprately using the data from NZ-Gazette-Dec10-VAS-information

df_hedge = spark.sql(sqlquery)

# COMMAND ----------

# DBTITLE 1,Get Hedge Contract Data
sqlquery = "SELECT  a.*, ParticipantShortName as PartyShortName  \
FROM processing.copper_hedgecontracts a LEFT JOIN processing.silver_dimparticipant b On (a.PartyCode = b.ParticipantCode) \
WHERE ContractStatus <> 'Disputed' AND ContractEffectiveToDate >= '" + startdate + "' AND ContractEffectiveFromDate <= '" + enddate + "' \
AND DisclosureCode not in ('21478','21486','21522','21488')" # These are VAS contracts and will be handle seprately using the data from NZ-Gazette-Dec10-VAS-information

sqlquery = "With L0 AS ( \
SELECT DisclosureCode, max(DisclosureUpdatedDatetime) as DisclosureUpdatedDatetime \
FROM  processing.copper_hedgecontracts GROUP BY DisclosureCode) \
SELECT DISTINCT a.*, ParticipantShortName as PartyShortName  \
FROM processing.copper_hedgecontracts a \
INNER JOIN L0 b \
ON (a.DisclosureCode = b.DisclosureCode \
and a.DisclosureUpdatedDatetime = b.DisclosureUpdatedDatetime) \
LEFT JOIN processing.silver_dimparticipant c On (a.PartyCode = c.ParticipantCode) \
WHERE ContractStatus <> 'Disputed' AND ContractEffectiveToDate >= '" + startdate + "' AND ContractEffectiveFromDate <= '" + enddate + "' \
AND a.DisclosureCode not in ('21478','21486','21522','21488')" # These are VAS contracts and will be handle seprately using the data from NZ-Gazette-Dec10-VAS-information


df_hedge = spark.sql(sqlquery)
            
# Mannually filling the PartyShortName if not exist
df_hedge = (df_hedge
            .withColumn('PartyShortName', (F.when(F.col('PartyShortName').isNull() & (F.col('PartyCode')=='MQBK'), F.lit("Macquarie Bank Limited"))
                                           .when(F.col('PartyShortName').isNull() & F.col('PartyCode').isin(['PIGL','PIGE']), F.lit("Pioneer Energy"))
                                           .when(F.col('PartyShortName').isNull() & (F.col('PartyCode')=='NGGL'), F.lit("Top Energy"))
                                           .when(F.col('PartyShortName').isNull() & (F.col('PartyCode')=='NGTU'), F.lit("Tuwharetoa Kawerau"))
                                           .when(F.col('PartyShortName').isNull() & F.col('DisclosureCode').isin(['56729','56709']), F.lit("Nova Energy"))
                                           .when(F.col('PartyCode').isin(['NOVA','AGCL','TGTL']), F.lit("Nova Energy"))
                                           .when(F.col('PartyCode').isin(['NZAS','NZAP','NZAF','NZAB','ANSL']), F.lit("NZ Aluminium Smelters"))
                                           .otherwise(F.col('PartyShortName'))
                                          )
                       )           
           )

# Let's standardize the messy OtherPartyLegalName
partynamemapping = (spark.read.csv('file:/Workspace/Repos/tuong.nguyen@ea.govt.nz/NetPositionEstimation/PartyLegalNameMapping.csv',header=True)
                    .withColumnRenamed(existing='PartyLegalName',new='OtherPartyLegalName')
                    .withColumnRenamed(existing='PartyShortName',new='OtherPartyShortName')
                   )

df_hedge = (df_hedge
            .join(partynamemapping, on = 'OtherPartyLegalName', how='left')
            .select(['DisclosureCode','PartyCode', 'PartyShortName', 'PartyRole', 'OtherPartyShortName', 'ContractType', 
                     'ForAllTradingPeriodsFlag', 'VolumeType','ContractEffectiveFromDate', 'ContractEffectiveToDate', 
                     'TradingDate','GridZoneArea','MegawattHours', 'DollarsPerMegawattHour'])

           )

if Testing:
  display(df_hedge)

# COMMAND ----------

# DBTITLE 1,Remove contracts with non-marketparticipants unless it is ASX
if True:
  df_hedge = (df_hedge
              .withColumn('OtherPartyShortName',(F.when(F.col('OtherPartyShortName').startswith('Haast'),F.lit('Haast Energy Trading'))
                                                 # Meridian Energy
                                                 .when(F.col('OtherPartyShortName').startswith('Powershop'),F.lit('Meridian Energy'))

                                                 # Mercury NZ 
                                                 .when(F.col('OtherPartyShortName').startswith('Tuaropaki'),F.lit('Mercury NZ'))
                                                 .when(F.col('OtherPartyShortName').isin(['Tiny Mighty','Nga Awa Purua JV',
                                                                                          'Glo-Bug','Mighty River Power']),F.lit('Mercury NZ'))

                                                 # Contact Energy
                                                 .when(F.col('OtherPartyShortName').isin('Simply Energy','Empower'
                                                                                        ),F.lit('Contact Energy'))                                        

                                                 # Genesis Energy
                                                 .when(F.col('OtherPartyShortName').isin('Energy OnLine','Frank Energy'
                                                                                        ),F.lit('Genesis Energy'))

                                                 # Nova Energy                                        
                                                 .when(F.col('OtherPartyShortName').startswith('Todd'),F.lit('Nova Energy'))
                                                 .when(F.col('OtherPartyShortName').isin(['BOP Energy','Hunet Energy','Nova Energy (AGCL)',
                                                                                          'Todd Generation Taranaki','Wise Prepay Energy']
                                                                                        ),F.lit('Nova Energy'))

                                                 # Top Energy
                                                 .when(F.col('OtherPartyShortName').isin('Ngawha Generation'),F.lit('Top Energy'))

                                                 # Pulse Energy Alliance
                                                 .when(F.col('OtherPartyShortName').isin(['Blackbox Power','Electra Energy','Grey Power Electricity',
                                                                                          'Just Energy','Property Power','Pulse Community','Pulse Energy']
                                                                                        ),F.lit('Pulse Energy Alliance'))

                                                 .otherwise(F.col('OtherPartyShortName'))
                                               )
                                 )
             )

  participants = spark.sql("SELECT distinct rtrim(ParticipantShortName) as OtherPartyShortName, 'Y' as OtherPartyIsMarketParticipant FROM processing.silver_dimparticipant")
  df_hedge = (df_hedge
              .join(participants, on = 'OtherPartyShortName', how = 'left')
              .withColumn('OtherPartyIsMarketParticipant', (F.when(F.col('OtherPartyShortName')=='ASX',F.lit('Y'))
                                                            .otherwise(F.col('OtherPartyIsMarketParticipant'))))
              .filter(F.col('OtherPartyIsMarketParticipant').isNotNull())
             )
  display(df_hedge)


# COMMAND ----------

# DBTITLE 1,Let's standardize the MegawattHours -->  Megawatts for CFD
df_cfd = (df_hedge.filter("ContractType = 'CFD'")
          
          ##################################################################################################
          # Calculate number of hours in a cntratc effective period
          .withColumn('FrTimestamp', F.unix_timestamp(F.col('ContractEffectiveFromDate')))
          .withColumn('ToTimestamp', F.unix_timestamp(F.date_add('ContractEffectiveToDate',1)))
          .withColumn('NumOfHours', (F.col('ToTimestamp') - F.col('FrTimestamp')).cast(T.LongType()))
          .withColumn('NumOfHours', (F.col('NumOfHours')/3600).cast(T.LongType()))    
          ##################################################################################################
          
          # Calculate contract quantity (MW) by treating all contracts as baseload (We will deal with peak-load contract after this )
          .withColumn('Megawatts', (F.col('MegawattHours')/F.col('NumOfHours')))

          ##################################################################################################
          # There are inconsistency in the data. 
          # Some non-base contracts has kWh values divided by number of hours with no remainder
          # --> I will treat these as baseload contracts   
          .withColumn('IsBaseLoad', 
                       F.when(F.round('Megawatts',3) == F.col('Megawatts'), F.lit('Y'))
                       .when(F.col('ForAllTradingPeriodsFlag') == 'Y', F.lit('Y') )
                       .otherwise(F.lit('N'))
                      )    
          ##################################################################################################

          ##################################################################################################
          # If a contract is peak contract [ForAllTradingPeriodsFlag == N] 
          # Then we calculate contract quantity Megawatts as: MegawattHours / [NumberOfWeekDays * 15(hrs/day)]
          # Let's calculate the number of week days in a cntratc effective period
          .withColumn('days', F.expr('sequence(ContractEffectiveFromDate, ContractEffectiveToDate, interval 1 day)'))
          # keep only days where day of week (dow) <= 5 (Friday)
          .withColumn('weekdays', F.expr('filter(transform(days, day->(day, extract(dow_iso from day))), day -> day.col2 <=5).day')) 
          # count how many days are left
          .withColumn('NumberOfWeekDays', F.expr('size(weekdays)')) 
          # Let's estimate the contract quantity (MW) for peak contracts
          .withColumn('Megawatts', (F.when((F.col('ForAllTradingPeriodsFlag') == 'Y') | (F.col('IsBaseLoad') == 'Y'), F.col('Megawatts')
                                          ).otherwise(F.col('MegawattHours')/ (F.col('NumberOfWeekDays') * 15))
                                   )
                     )
          ##################################################################################################

          # Tidy up
          .withColumn('Megawatts', F.col('Megawatts').cast(T.DecimalType(10,3)))
          .select(['DisclosureCode','PartyCode', 'PartyShortName', 'PartyRole', 'OtherPartyShortName', 
                   'ContractType', 'ForAllTradingPeriodsFlag', 'IsBaseLoad','VolumeType',
                   'ContractEffectiveFromDate', 'ContractEffectiveToDate','GridZoneArea',
                   'Megawatts', 'DollarsPerMegawattHour', 'MegawattHours'])

          )
if Testing:
  display(df_cfd)



# COMMAND ----------

# DBTITLE 1,Let's standardize the MegawattHours -->  Megawatts for FPVV
df_fpvv = (df_hedge.filter("ContractType = 'FPVV'")
           .withColumn('FrTimestamp', F.unix_timestamp(F.col('ContractEffectiveFromDate')))
           .withColumn('ToTimestamp', F.unix_timestamp(F.date_add('ContractEffectiveToDate',1)))
           .withColumn('NumOfHours', (F.col('ToTimestamp') - F.col('FrTimestamp')).cast(T.LongType()))
           .withColumn('NumOfHours', (F.col('NumOfHours')/3600).cast(T.LongType()))    
           .withColumn('Megawatts', (F.col('MegawattHours')/F.col('NumOfHours')))
               
           ##################################################################################################
           # There are inconsistency in the data. 
           # Some non-base contracts has kWh values divided by number of hours with no remainder
           # --> I will treat these as baseload contracts   
           .withColumn('IsBaseLoad', 
                       F.when(F.round('Megawatts',3) == F.col('Megawatts'), F.lit('Y'))
                       .when(F.col('ForAllTradingPeriodsFlag') == 'Y', F.lit('Y') )
                       .otherwise(F.lit('N'))
                      )    
           ##################################################################################################

           ##################################################################################################
           # If a contract is peak contract [ForAllTradingPeriodsFlag == N] 
           # Then we calculate contract quantity Megawatts as: MegawattHours / [NumberOfWeekDays * 15(hrs/day)]
           # Let's calculate the number of week days in a cntratc effective period
           .withColumn('days', F.expr('sequence(ContractEffectiveFromDate, ContractEffectiveToDate, interval 1 day)'))
           # keep only days where day of week (dow) <= 5 (Friday)
           .withColumn('weekdays', F.expr('filter(transform(days, day->(day, extract(dow_iso from day))), day -> day.col2 <=5).day')) 
           # count how many days are left
           .withColumn('NumberOfWeekDays', F.expr('size(weekdays)')) 
           # Let's estimate the contract quantity (MW) for peak contracts
           .withColumn('Megawatts', (F.when(F.col('IsBaseLoad') == 'N'
                                            ,F.col('MegawattHours')/ (F.col('NumberOfWeekDays') * 15)
                                           ).otherwise(F.col('Megawatts'))
                                    )
                      )
           ##################################################################################################

           .withColumn('Megawatts', F.col('Megawatts').cast(T.DecimalType(10,3)))
           .select(['DisclosureCode','PartyCode', 'PartyShortName', 'PartyRole', 'OtherPartyShortName', 
                    'ContractType', 'ForAllTradingPeriodsFlag', 'IsBaseLoad', 'VolumeType',
                    'ContractEffectiveFromDate', 'ContractEffectiveToDate','GridZoneArea',
                    'Megawatts', 'DollarsPerMegawattHour', 'MegawattHours'])

              )
if Testing:
  display(df_fpvv)




# COMMAND ----------

# DBTITLE 1,Let's standardize the MegawattHours -->  Megawatts for Options
df_opt =  (df_hedge.filter("ContractType = 'OPT'")
           .withColumn('FrTimestamp', F.unix_timestamp(F.col('ContractEffectiveFromDate')))
           .withColumn('ToTimestamp', F.unix_timestamp(F.date_add('ContractEffectiveToDate',1)))
           .withColumn('NumOfHours', (F.col('ToTimestamp') - F.col('FrTimestamp')).cast(T.LongType()))
           .withColumn('NumOfHours', (F.col('NumOfHours')/3600).cast(T.LongType()))    
           .withColumn('Megawatts', (F.col('MegawattHours')/F.col('NumOfHours')))

           ##################################################################################################
           # There are inconsistency in the data. 
           # Some non-base contracts has kWh values divided by number of hours with no remainder
           # --> I will treat these as baseload contracts   
           .withColumn('IsBaseLoad', 
                       F.when(F.round('Megawatts',3) == F.col('Megawatts'), F.lit('Y'))
                       .when(F.col('ForAllTradingPeriodsFlag') == 'Y', F.lit('Y') )
                       .otherwise(F.lit('N'))
                      )  
           ##################################################################################################

           ##################################################################################################
           # If a contract is peak contract [ForAllTradingPeriodsFlag == N] 
           # Then we calculate contract quantity Megawatts as: MegawattHours / [NumberOfWeekDays * 15(hrs/day)]
           # Let's calculate the number of week days in a cntratc effective period
           .withColumn('days', F.expr('sequence(ContractEffectiveFromDate, ContractEffectiveToDate, interval 1 day)'))
           # keep only days where day of week (dow) <= 5 (Friday)
           .withColumn('weekdays', F.expr('filter(transform(days, day->(day, extract(dow_iso from day))), day -> day.col2 <=5).day')) 
           # count how many days are left
           .withColumn('NumberOfWeekDays', F.expr('size(weekdays)')) 
           # Let's estimate the contract quantity (MW) for peak contracts
           .withColumn('Megawatts', (F.when(F.col('IsBaseLoad') == 'N'
                                            ,F.col('MegawattHours')/ (F.col('NumberOfWeekDays') * 15)
                                           ).otherwise(F.col('Megawatts'))
                                    )
                      )
           ##################################################################################################

           .withColumn('Megawatts', F.col('Megawatts').cast(T.DecimalType(10,3)))
           .drop(*['days','weekdays','FrTimestamp','ToTimestamp','NumOfHours'])
           .select(['DisclosureCode','PartyCode', 'PartyShortName', 'PartyRole', 'OtherPartyShortName', 
                    'ContractType', 'ForAllTradingPeriodsFlag', 'IsBaseLoad','VolumeType',
                    'ContractEffectiveFromDate', 'ContractEffectiveToDate','GridZoneArea',
                    'Megawatts', 'DollarsPerMegawattHour', 'MegawattHours'])

          )
if Testing:
  display(df_opt.filter("PartyCode = 'CTCT'"))



# COMMAND ----------

# DBTITLE 1,Let's standardize the MegawattHours -->  Megawatts for the rest
df_oth =  (df_hedge.filter(~F.col('ContractType').isin('OPT','CFD','FPVV'))
           .withColumn('FrTimestamp', F.unix_timestamp(F.col('ContractEffectiveFromDate')))
           .withColumn('ToTimestamp', F.unix_timestamp(F.date_add('ContractEffectiveToDate',1)))
           .withColumn('NumOfHours', (F.col('ToTimestamp') - F.col('FrTimestamp')).cast(T.LongType()))
           .withColumn('NumOfHours', (F.col('NumOfHours')/3600).cast(T.LongType()))    
           .withColumn('Megawatts', (F.col('MegawattHours')/F.col('NumOfHours')))

           ##################################################################################################
           # There are inconsistency in the data. 
           # Some non-base contracts has kWh values divided by number of hours with no remainder
           # --> I will treat these as baseload contracts   
           .withColumn('IsBaseLoad', 
                       F.when(F.round('Megawatts',3) == F.col('Megawatts'), F.lit('Y'))
                       .when(F.col('ForAllTradingPeriodsFlag') == 'Y', F.lit('Y') )
                       .otherwise(F.lit('N'))
                      )   
           ##################################################################################################

           ##################################################################################################
           # If a contract is peak contract [ForAllTradingPeriodsFlag == N] 
           # Then we calculate contract quantity Megawatts as: MegawattHours / [NumberOfWeekDays * 15(hrs/day)]
           # Let's calculate the number of week days in a cntratc effective period
           .withColumn('days', F.expr('sequence(ContractEffectiveFromDate, ContractEffectiveToDate, interval 1 day)'))
           # keep only days where day of week (dow) <= 5 (Friday)
           .withColumn('weekdays', F.expr('filter(transform(days, day->(day, extract(dow_iso from day))), day -> day.col2 <=5).day')) 
           # count how many days are left
           .withColumn('NumberOfWeekDays', F.expr('size(weekdays)')) 
           # Let's estimate the contract quantity (MW) for peak contracts
           .withColumn('Megawatts', (F.when(F.col('IsBaseLoad') == 'N'
                                            ,F.col('MegawattHours')/ (F.col('NumberOfWeekDays') * 15)
                                           ).otherwise(F.col('Megawatts'))
                                    )
                      )
           ##################################################################################################

           .withColumn('Megawatts', F.col('Megawatts').cast(T.DecimalType(10,3)))
           .drop(*['days','weekdays','FrTimestamp','ToTimestamp','NumOfHours'])
           .select(['DisclosureCode','PartyCode', 'PartyShortName', 'PartyRole', 'OtherPartyShortName', 
                    'ContractType', 'ForAllTradingPeriodsFlag', 'IsBaseLoad','VolumeType',
                    'ContractEffectiveFromDate', 'ContractEffectiveToDate','GridZoneArea',
                    'Megawatts', 'DollarsPerMegawattHour', 'MegawattHours'])

          )
if Testing:
  display(df_oth)



# COMMAND ----------

# DBTITLE 1,Recombine hedge contract data
df_hedge_mw = (df_cfd.union(df_fpvv).union(df_opt).union(df_oth)
               .withColumn('Buyer', F.when(F.col('PartyRole') == "buyer", F.col('PartyShortName')).otherwise(F.col('OtherPartyShortName')))
               .withColumn('Seller', F.when(F.col('PartyRole') == "seller", F.col('PartyShortName')).otherwise(F.col('OtherPartyShortName')))
               .withColumn('PointOfConnectionCode', (F.when(F.col('DisclosureCode')=='767132', F.lit('TWI2201')) 
                                                     .when(F.col('DisclosureCode')=='301888', F.lit('TWI2201'))                                                      
                                                     .when(F.col('GridZoneArea')=='A', F.lit('OTA2201'))
                                                     .when(F.col('GridZoneArea')=='B', F.lit('WKM2201'))
                                                     .when(F.col('GridZoneArea')=='C', F.lit('HAY2201'))
                                                     .when(F.col('GridZoneArea')=='D', F.lit('ISL2201'))
                                                     .when(F.col('GridZoneArea')=='E', F.lit('BEN2201'))
#                                                      .otherwise(F.lit('Unknown'))
                                                     .otherwise(F.lit('BEN2201'))
                                                    )
                          )
              )
if Testing:
#   display(df_hedge_mw.filter("DisclosureCode = 767132"))
  display(df_hedge_mw.filter("PartyShortName like '%Electric Kiwi%' or OtherPartyShortName like '%Electric Kiwi%' "))

# COMMAND ----------

display(df_hedge_mw.filter("GridZoneArea is NULL "))

# COMMAND ----------

# MAGIC %md #HEDGE DISCLOSURE BY TRADING PERIODS

# COMMAND ----------

# DBTITLE 1,Populate hedge contract Megawatts to studied periods
df_hedge_period = (df_hedge_mw
                   .crossJoin(studied_periods)
                   .filter(F.col('TradingDate') <= F.col('ContractEffectiveToDate'))
                   .filter(F.col('TradingDate') >= F.col('ContractEffectiveFromDate'))
                   # Assign Megawatts to Periods
                   .withColumn("WeekDay",F.dayofweek('TradingDate')) #Sunday = 1 and Saturday = 7
                   .withColumn("Megawatts",(F.when(F.col('IsBaseLoad')=='Y',F.col('Megawatts'))
                                            .when(F.col('WeekDay').isin([1,7]), F.lit(0))
                                            .when(F.col('TradingPeriodNumber') < 15, F.lit(0))
                                            .when(F.col('TradingPeriodNumber') > 44, F.lit(0))
                                            .otherwise(F.col('Megawatts'))
                                           )
                              )
                   .withColumn("Market",F.when(F.col('OtherPartyShortName')=='ASX',F.lit('ASX')).otherwise(F.lit('OTC')))
                  )

if Testing:
  display(df_hedge_period)


# COMMAND ----------

# DBTITLE 1,Summarize Hedge contract position
df_hedge_trader = (df_hedge_period
                   .withColumn('TraderName', F.col('Buyer'))
                   .groupBy(['Market','TraderName','TradingDate','TradingPeriodNumber','ContractType','PointOfConnectionCode'])
                   .agg(F.sum('Megawatts').alias('Megawatts'))
                  ).union(df_hedge_period
                          .withColumn('TraderName', F.col('Seller'))
                          .groupBy(['Market','TraderName','TradingDate','TradingPeriodNumber','ContractType','PointOfConnectionCode'])
                          .agg(-F.sum('Megawatts').alias('Megawatts'))
                         )

df_hedge_trader = (df_hedge_trader
                   .groupBy(['Market','TraderName','TradingDate','TradingPeriodNumber','ContractType','PointOfConnectionCode'])
                   .agg(F.sum('Megawatts').alias('Megawatts'))
                   .join(df_enrg_prices, on = ['TradingDate','TradingPeriodNumber','PointOfConnectionCode'], how = 'inner')
                   .select(['Market','TraderName','TradingDate','TradingPeriodNumber','ContractType','PointOfConnectionCode','Megawatts','DollarsPerMegawattHour'])
                  ).orderBy(['TraderName','TradingDate','TradingPeriodNumber','ContractType','PointOfConnectionCode'])


if Testing:
  display(df_hedge_trader.filter("TraderName = 'Electric Kiwi'"))

# COMMAND ----------

if True:
  df_hedge_C300 = (df_hedge_trader

                      ###################################################################################################
                      # if a contract is a call option Ex: C300 --> we take  'strike price' into account for DollarsPerMegawattHour 
                      .withColumn('StrikePrice', F.regexp_extract(F.col('ContractType'), 'C[0-9]+', 0))
                      .withColumn('StrikePrice', F.when(F.col('StrikePrice').startswith('C'), 
                                                        F.regexp_extract(F.col('ContractType'), '[0-9]+', 0).cast(T.IntegerType())
                                                       ).otherwise(F.lit(0)))
                      .withColumn('DollarsPerMegawattHour', F.when(F.col('DollarsPerMegawattHour') > F.col('StrikePrice')
                                                                   ,F.col('DollarsPerMegawattHour') - F.col('StrikePrice')).otherwise(F.lit(0)))
                     ).filter("ContractType = 'C300'")
  display(df_hedge_C300)

# COMMAND ----------

# DBTITLE 1,Calculate Hedge Positions
df_hedge_summary = (df_hedge_trader
                    
                    ###################################################################################################
                    # if a contract is a call option Ex: C300 --> we take  'strike price' into account for DollarsPerMegawattHour 
                    .withColumn('StrikePrice', F.regexp_extract(F.col('ContractType'), 'C[0-9]+', 0))
                    .withColumn('StrikePrice', F.when(F.col('StrikePrice').startswith('C'), 
                                                      F.regexp_extract(F.col('ContractType'), '[0-9]+', 0).cast(T.IntegerType())
                                                     ).otherwise(F.lit(0)))
                    .withColumn('DollarsPerMegawattHour', F.when(F.col('DollarsPerMegawattHour') > F.col('StrikePrice')
                                                                 ,F.col('DollarsPerMegawattHour') - F.col('StrikePrice')).otherwise(F.lit(0)))
                    .drop('StrikePrice')
                    ###################################################################################################
                    
                    .groupby(['Market','TraderName','ContractType'])
                    .agg(F.sum(F.col('Megawatts') * F.col('DollarsPerMegawattHour') / 2 )
                         .cast(T.DecimalType(15,2))
                         .alias('Dollars'))
                   )
if Testing:
  display(df_hedge_summary)

# COMMAND ----------

# MAGIC %md # VAS Contract

# COMMAND ----------

df_trader = spark.sql("""SELECT Distinct ParticipantCode as party_code, ParticipantShortName as TraderName 
                         FROM processing.silver_dimparticipant WHERE isCurrentFlag = 'Y' 
                      """)
df_vas = (spark.read.csv('file:/Workspace/Repos/tuong.nguyen@ea.govt.nz/NetPositionEstimation/vas_contract.csv',header=True)
          .join(df_trader, on = 'party_code', how = 'inner')
          .withColumn('effective_date', F.to_date('effective_date','d/MM/yyyy'))
          .withColumn('end_date', F.to_date('end_date','d/MM/yyyy'))
          .crossJoin(studied_periods)
          .filter(F.col('TradingDate') <= F.col('end_date'))
          .filter(F.col('TradingDate') >= F.col('effective_date'))
          .withColumn("Market", F.lit('VAS'))
          .withColumn("ContractType", F.lit('CFD'))
          .withColumn("PointOfConnectionCode", F.col('Node'))
          .withColumn("Megawatts",F.col('MW').cast(T.DecimalType(10,3)))
          .join(df_enrg_prices, on = ['TradingDate','TradingPeriodNumber','PointOfConnectionCode'], how = 'inner')
          .select(['Market','TraderName','TradingDate','TradingPeriodNumber','ContractType','PointOfConnectionCode','Megawatts','DollarsPerMegawattHour'])
         )
if Testing:
  display(df_vas)

# COMMAND ----------

# DBTITLE 1,Calculate VAS positions
df_vas_summary = (df_vas
                  .groupby(['Market','TraderName','ContractType'])
                  .agg(F.sum(F.col('Megawatts') * F.col('DollarsPerMegawattHour') / 2 )
                       .cast(T.DecimalType(15,2))
                       .alias('Dollars'))
                   )
if Testing:
  display(df_vas_summary)

# COMMAND ----------

# MAGIC %md #FTR contracts

# COMMAND ----------

sqlquery = ("\
SELECT FTRContractOwnerParticipantShortName as TraderName, FTRContractStartDate, FTRContractEndDate, \
FTRContractTypeCode, SourcePointOfConnectionCode, SinkPointOfConnectionCode, sum(FTRContractMegawatts) as Megawatts \
FROm gold.ftrcontracthistory \
WHERE FTRContractStartDate <= '" + enddate + "' and FTRContractEndDate >= '" + startdate + "' \
GROUP BY FTRContractOwnerParticipantShortName , FTRContractStartDate, FTRContractEndDate, FTRContractTypeCode, \
SourcePointOfConnectionCode, SinkPointOfConnectionCode  \
HAVING sum(FTRContractMegawatts) <> 0 \
")
df_ftr = (spark.sql(sqlquery)
          .withColumnRenamed('SourcePointOfConnectionCode','PointOfConnectionCode')
          .join(df_enrg_prices, on = 'PointOfConnectionCode', how = 'inner')
          .withColumnRenamed('DollarsPerMegawattHour','SourceDollarsPerMegawattHour')
          .withColumnRenamed('PointOfConnectionCode','SourcePointOfConnectionCode')
          .filter("TradingDate between FTRContractStartDate and FTRContractEndDate")
          .withColumnRenamed('SinkPointOfConnectionCode','PointOfConnectionCode')
          .join(df_enrg_prices, on = ['TradingDate','TradingPeriodNumber','PointOfConnectionCode'], how = 'inner')
          .withColumnRenamed('DollarsPerMegawattHour','SinkDollarsPerMegawattHour')
          .withColumnRenamed('PointOfConnectionCode','SinkPointOfConnectionCode')
          .withColumnRenamed('FTRContractTypeCode','ContractType')
          .drop(*['PriceTypeCode'])
          .withColumn('Market',F.lit('FTR'))
          .withColumn('DollarsPerMegawattHour',F.col('SinkDollarsPerMegawattHour') - F.col('SourceDollarsPerMegawattHour'))
          .withColumn('DollarsPerMegawattHour',(F.when(F.col('ContractType')=='OBL', F.col('DollarsPerMegawattHour'))
                                                .when(F.col('DollarsPerMegawattHour') > 0, F.col('DollarsPerMegawattHour'))
                                                .otherwise(F.lit(0))
                                               )
                     )
          .select(['Market','TraderName','TradingDate','TradingPeriodNumber','ContractType',
                   'SourcePointOfConnectionCode','SinkPointOfConnectionCode','Megawatts', 'DollarsPerMegawattHour'])
         )


if Testing:
  display(df_ftr)

# COMMAND ----------

# DBTITLE 1,Calculate FTR position
df_ftr_summary = (df_ftr
                  .groupby(['Market','TraderName','ContractType'])
                  .agg(F.sum(F.col('Megawatts') * F.col('DollarsPerMegawattHour') / 2 )
                       .cast(T.DecimalType(15,2))
                       .alias('Dollars'))
                   )
if Testing:
  display(df_ftr_summary)

# COMMAND ----------

# MAGIC %md #SUMMARY

# COMMAND ----------

df_summary = (df_physical_summary
              .union(df_hedge_summary)
              .union(df_vas_summary)
              .union(df_ftr_summary)
              .withColumn('TraderName',(F.when(F.col('TraderName').startswith('Haast'),F.lit('Haast Energy Trading'))
                                       .otherwise(F.col('TraderName'))
                                       )
                         )
             )

if Testing:
  display(df_summary)

# COMMAND ----------

df_summary = (df_physical_summary
              .union(df_hedge_summary)
              .union(df_vas_summary)
              .union(df_ftr_summary)
              .withColumn('TraderName',(F.when(F.col('TraderName').startswith('Haast'),F.lit('Haast Energy Trading'))
                                        
                                        # Meridian Energy
                                        .when(F.col('TraderName').startswith('Powershop'),F.lit('Meridian Energy'))
                                        
                                        # Mercury NZ 
                                        .when(F.col('TraderName').startswith('Tuaropaki'),F.lit('Mercury NZ'))
                                        .when(F.col('TraderName').isin(['Tiny Mighty','Nga Awa Purua JV',
                                                                        'Glo-Bug','Mighty River Power']),F.lit('Mercury NZ'))
                                                                               
                                        # Contact Energy
                                        .when(F.col('TraderName').isin('Simply Energy','Empower'
                                                                      ),F.lit('Contact Energy'))                                        
                                        
                                        # Genesis Energy
                                        .when(F.col('TraderName').isin('Energy OnLine','Frank Energy'
                                                                      ),F.lit('Genesis Energy'))
                                        
                                        # Nova Energy                                        
                                        .when(F.col('TraderName').startswith('Todd'),F.lit('Nova Energy'))
                                        .when(F.col('TraderName').isin(['BOP Energy','Hunet Energy','Nova Energy (AGCL)',
                                                                        'Todd Generation Taranaki','Wise Prepay Energy']
                                                                      ),F.lit('Nova Energy'))
                                        
                                        # Top Energy
                                        .when(F.col('TraderName').isin('Ngawha Generation'),F.lit('Top Energy'))
                                        
                                        # Pulse Energy Alliance
                                        .when(F.col('TraderName').isin(['Blackbox Power','Electra Energy','Grey Power Electricity',
                                                                        'Just Energy','Property Power','Pulse Community','Pulse Energy']
                                                                      ),F.lit('Pulse Energy Alliance'))
                                        
                                        .otherwise(F.rtrim('TraderName'))
                                       )
                         )
              .groupBy(['Market','TraderName','ContractType'])
              .agg(F.sum('Dollars').alias('Dollars'))
              .orderBy('Market','TraderName','ContractType')
             )

display(df_summary)

# COMMAND ----------

# participants = spark.sql("SELECT distinct rtrim(ParticipantShortName) as TraderName, 'Y' as MarketParticipant FROM processing.silver_dimparticipant")
# df_participant_summary = df_summary.join(participants, on = 'TraderName', how = 'left')
# display(df_participant_summary)
