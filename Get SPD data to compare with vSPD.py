# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("caseid","121302023040630708","CaseID")
dbutils.widgets.text("interval","12-APR-2023 19:00","Trading Interval")

# COMMAND ----------

# DBTITLE 1,Get case parameters
import pyspark.sql.types as T
import pyspark.sql.functions as F
import pyspark.sql.window as w
from pyspark.sql import SparkSession

caseid = dbutils.widgets.get("caseid")
tradinginterval = dbutils.widgets.get("interval")
df = spark.sql("select * from silver.dimspdcase where caseID = '" + caseid + "'").toPandas()
casetype = df["CaseTypeCode"].iloc[0]

# COMMAND ----------

# DBTITLE 1,Get branch data
df = (spark.read.format('csv').option("header", "true").option("inferSchema", "true")
      .load('/mnt/eadatalakeprd/datalake/spd/'+ casetype + '/NETDATA/BRANCHNODE/MSS_' + caseid + '_0X.MSSNET') 
      ).drop(*['I','NETDATA','BRANCHNODE','1.0'])   
df = (df
      .withColumn('KEY1', F.trim('KEY1')).withColumn('KEY2', F.trim('KEY2')).withColumn('KEY3', F.trim('KEY3')).withColumn('KEY4', F.trim('KEY4'))
      .withColumn('BranchID', F.regexp_replace(F.col('ID_BRANCH'),' ',''))
      .withColumn('BranchName', F.when(F.col('KEY4')=='LN', F.concat(F.col('KEY2'),F.lit('.'),F.col('KEY3'))
                                       ).otherwise(F.concat(F.col('KEY1'),F.lit('_'),F.col('KEY2'),F.lit('.'),F.col('KEY3'))))
      ).select(['BranchID','BranchName']).distinct()

df_branch = (spark.read.format('csv').option("header", "true").option("inferSchema", "true")
             .load('/mnt/eadatalakeprd/datalake/spd/'+ casetype + '/SOLUTION/BRANCH/MSS_' + caseid + '_0X.SPDSOLVED') 
             .withColumn('BranchID', F.regexp_replace(F.col('BRANCHNAME'),' ',''))
             .withColumnRenamed('BRANCHNAME','BRANCHNAME1')
            ).drop(*['I','SOLUTION','BRANCH','1.0'])

df_branch = df_branch.join(df,'BranchID','left')

df_branch = (df_branch
            .withColumn('BranchName', F.when(F.col('BranchName').isNull(),F.trim(F.col('BRANCHNAME1'))).otherwise(F.col('BranchName')))
            .withColumn('CaseID', F.lit(caseid))
            .withColumnRenamed('INTERVAL','Interval')
            .withColumn('Flow_MW',F.col('FROM_MW').cast(T.DecimalType(10,5)))
            .withColumn('Flow_MW',F.when(F.col('Flow_MW')<=0 ,F.col('TO_MW').cast(T.DecimalType(10,5))).otherwise(F.col('Flow_MW')))
            .withColumn('Capacity_MW',F.col('MWMAX').cast(T.DecimalType(10,5)))
            .withColumn('DynamicLoss_MW',F.col('BRANCHLOSSES').cast(T.DecimalType(10,5)))
            .withColumn('FixedLoss_MW',F.when(F.col('DISCONNECTED')==1, F.lit(0)).otherwise(F.col('FIXEDLOSS').cast(T.DecimalType(10,5))))
            .withColumn('DynamicLoss_MW',F.col('DynamicLoss_MW') - F.col('FixedLoss_MW'))
            .withColumn('BranchMarginalPrice',F.col('MARGINALPRICE').cast(T.DecimalType(10,5)))
          ).select(['CaseID','Interval','BranchName','Flow_MW','Capacity_MW','DynamicLoss_MW','FixedLoss_MW','BranchMarginalPrice'])
          
df = df_branch.filter("`Interval` = '"+ tradinginterval +"'")
if df.count() > 0: df_branch = df
display(df_branch)

# COMMAND ----------

# DBTITLE 1,Get SPD bids' results
df_bidload = spark.sql("select CaseID, `Interval`, b.IslandCode as Island, -sum(SPDGenerationMegawatts) as SPD_BidLoad from copper.spd_" + casetype + "_solution_traderperiod a inner join silver.dimpointofconnection b on (a.PointOfConnectionCode = b.PointOfConnectionCode and b.IsCurrentFlag = 'Y') where caseID = '" + caseid + "' and OfferTypeCode not in ('ENOF','ILRO','PLRO','TWRO') GROUP BY `Interval`, CaseID, TradingPeriodNumber , b.IslandCode ORDER BY CaseID, `Interval`, b.IslandCode")

df = df_bidload.filter("`Interval` = '"+ tradinginterval +"'")
if df.count() > 0: df_bidload = df
display(df_bidload)

# COMMAND ----------

# DBTITLE 1,Get HVDC loss
df1 = (df_branch.filter("BranchName in ('HAY_BEN1.1','HAY_BEN2.1','HAY_BEN3.1','BEN_HAY1.1','BEN_HAY2.1','BEN_HAY3.1')")
     .withColumn('Island',F.when(F.col('BranchName').isin(['BEN_HAY1.1','BEN_HAY2.1']),F.lit('NI')).otherwise(F.lit('SI')))
     .groupBy("CaseID","Interval","Island").agg(F.sum("DynamicLoss_MW").alias("DynamicLoss_MW")))
df2 = (df_branch.filter("BranchName in ('HAY_BEN1.1','HAY_BEN2.1','HAY_BEN3.1','BEN_HAY1.1','BEN_HAY2.1','BEN_HAY3.1')")
     .withColumn('Island',F.when(F.col('BranchName').isin(['BEN_HAY1.1','BEN_HAY2.1']),F.lit('NI')).otherwise(F.lit('SI')))
     .groupBy("CaseID","Interval").agg((F.sum("FixedLoss_MW")/2).alias("FixedLoss_MW"))     )
df_hvdcloss = df1.join(df2,["CaseID","Interval"],'inner').withColumn('SPD_HVDC_Loss',F.col('DynamicLoss_MW')+F.col('FixedLoss_MW')).drop(*['DynamicLoss_MW','FixedLoss_MW']) 
# display(df1)
# display(df2)
display(df_hvdcloss)       

# COMMAND ----------

# DBTITLE 1,System benefit
df_bid = spark.sql("select CaseID, `Interval`, TradingPeriodNumber as Period, PointOfConnectionCode, SPDTraderID, TrancheNumber, SPDMaximumOutputMegawatt,SPDOfferPriceDollarsPerMegawattHour from copper.spd_" + casetype + "_mssdata_bidsandoffers where CaseID = '" + caseid + "' and OfferTypeCode not in ('ENOF','ILRO','PLRO','TWRO') ")

window_spec = w.Window.partitionBy(['CaseID','Interval','Period','PointOfConnectionCode','SPDTraderID']).orderBy(F.col("SPDOfferPriceDollarsPerMegawattHour").desc())
df_bid  = df_bid.withColumn("rank", F.rank().over(window_spec))
max_rank = df_bid.agg({"rank": "max"}).collect()[0][0]

df_bid_cleared = spark.sql("select CaseID, `Interval`, TradingPeriodNumber as Period, PointOfConnectionCode, a.SPDTraderID, -sum(SPDGenerationMegawatts) as ClearedBidMW from copper.spd_" + casetype + "_solution_traderperiod a inner join silver.dimparticipant b on (a.SPDTraderID = b.SPDTraderID and b.IsCurrentFlag = 'Y') where caseID = '" + caseid + "' and OfferTypeCode not in ('ENOF','ILRO','PLRO','TWRO') GROUP BY CaseID, `Interval`, TradingPeriodNumber, PointOfConnectionCode, a.SPDTraderID ORDER BY Period,PointOfConnectionCode")
df_bid_tranches = None
for i in range(1, max_rank+1):
  df = (df_bid.filter(F.col("rank") == i).join(df_bid_cleared, on = ['CaseID','Interval','Period','PointOfConnectionCode','SPDTraderID'], how = 'inner')
       .withColumn("SPDClearedMW", F.when(F.col("ClearedBidMW") > F.col("SPDMaximumOutputMegawatt"), F.col("SPDMaximumOutputMegawatt")).otherwise(F.col("ClearedBidMW")))
       .withColumn("ClearedBidMW", F.col("ClearedBidMW") - F.col("SPDClearedMW"))
       )
  df_bid_cleared = df.select(['CaseID','Interval','Period','PointOfConnectionCode','SPDTraderID','ClearedBidMW'])  
  df = df.select(['CaseID','Interval','Period','PointOfConnectionCode','SPDTraderID','TrancheNumber','SPDMaximumOutputMegawatt','SPDOfferPriceDollarsPerMegawattHour','SPDClearedMW'])   
  if df_bid_tranches == None: 
    df_bid_tranches = df  
  else: 
    df_bid_tranches = df_bid_tranches.union(df)
# display(df_bid_tranches)

df_benefit = df_bid_tranches.groupBy(['CaseID','Interval','Period']).agg(F.sum(F.col("SPDOfferPriceDollarsPerMegawattHour")*F.col("SPDClearedMW")).alias("SPD_Benefit"))
display(df_benefit)

# COMMAND ----------

# DBTITLE 1,System IntervalCost
df = spark.sql(
  "select CaseID, `Interval`, TradingPeriodNumber as Period,SPDIntervalCostForAllNZDollarsPerHour as SPD_IntervalCost from copper.spd_" + casetype + "_solution_island where CaseID = '" + caseid + "' and IslandCode = 'NI' ORDER BY Period, Island")

if df_benefit.count()>0:
  df_system = (df_benefit.join(df,['CaseID', 'Interval','Period'],'inner')
              .withColumn("SPD_Cost",F.col('SPD_Benefit') + F.col('SPD_IntervalCost'))
              .select(['CaseID', 'Interval','Period','SPD_Cost','SPD_Benefit','SPD_IntervalCost'])
              ).orderBy(['Period'])
else:
  df_system = (df.withColumn("SPD_Cost",F.col('SPD_IntervalCost'))
              .withColumn("SPD_Benefit",F.lit(0))
              .select(['CaseID', 'Interval','Period','SPD_Cost','SPD_Benefit','SPD_IntervalCost'])
              ).orderBy(['Period'])


display(df_system)


# COMMAND ----------

# DBTITLE 1,Get SPD islands' result
df_island = spark.sql(
  "select CaseID, `Interval`, TradingPeriodNumber as Period, IslandCode as Island,SPDClearedEnergyMegawatts as SDP_Gen, SPDLoadMegawatts as SPD_Load, SPDTotalNetworkLossesMegawatts as SPDIslandLoss, SPDNetHVDCInterchangeMegawatts as SPD_HVDC_Flow,SPDReferencePriceDollarsPerMegawattHour as SPD_ReferencePrice,SPDFIRRequiredMegawatts as SPD_FIR_req,SPDSIRRequiredMegawatts as SPD_SIR_req,SPDFIRDollarsPerMegawattHour as SPD_FIRPrice, SPDSIRDollarsPerMegawattHour as SPD_SIRPrice,SPDClearedFIRMegawatts as SPD_FIR_Clear,SPDClearedSIRMegawatts as SPD_SIR_Clear,SPDFIRReserveSentFromMegawatts as SPD_FIR_Share,SPDSIRReserveSentFromMegawatts as SPD_SIR_Share,SPDFIRReserveReceivedMegawatts as SPD_FIR_Receive,SPDSIRReserveReceivedMegawatts as SPD_SIR_Receive,SPDFIRReserveEffectiveCEMegawatts as FIR_Effective_CE,SPDSIRReserveEffectiveCEMegawatts as SIR_Effective_CE,SPDFIRReserveEffectiveECEMegawatts as FIR_Effective_ECE,SPDSIRReserveEffectiveECEMegawatts as SIR_Effective_ECE,SPDIntervalCostForAllNZDollarsPerHour as SPD_IntervalCost from copper.spd_" + casetype + "_solution_island where CaseID = '" + caseid + "' ORDER BY Period, Island")

df_island = (df_island.join(df_hvdcloss,["CaseID","Interval","Island"],'inner'))
if df_bidload.count() > 0:
  df_island = (df_island.join(df_bidload,["CaseID","Interval","Island"],'inner'))
else:  
  df_island = df_island.withColumn("SPD_BidLoad",F.lit(0))

df_island = (df_island
            .withColumn("SPDIslandLoss", F.col("SPDIslandLoss") - F.col("SPD_HVDC_Loss"))
            .withColumn("SPD_HVDC_Flow", F.when(F.col("SPD_HVDC_Flow")<0,0).otherwise(F.col("SPD_HVDC_Flow")))  
            .select(["CaseID","Interval","Period","Island","SDP_Gen","SPD_Load","SPD_BidLoad","SPDIslandLoss","SPD_HVDC_Flow","SPD_HVDC_Loss","SPD_ReferencePrice","SPD_FIR_req","SPD_SIR_req","SPD_FIRPrice","SPD_SIRPrice","SPD_FIR_Clear","SPD_SIR_Clear","SPD_FIR_Share","SPD_SIR_Share","SPD_FIR_Receive","SPD_SIR_Receive","FIR_Effective_CE","SIR_Effective_CE","FIR_Effective_ECE","SIR_Effective_ECE","SPD_IntervalCost"])
            .orderBy(["Period","Island"])
            )
display(df_island)  

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sensitive.spdisland where caseID  = ${caseid}

# COMMAND ----------

# DBTITLE 1,Get SPD offers' results
df_traderperiod = spark.sql("select CaseID, `Interval`, TradingPeriodNumber as Period, a.PNodeCode as Offer, b.ParticipantCode as Trader, sum(SPDGenerationMegawatts) as SPD_Genetation, sum(SPDClearedFIRMegawatts) as SPD_FIR, sum(SPDClearedSIRMegawatts) as SPD_SIR from copper.spd_" + casetype + "_solution_traderperiod a inner join silver.dimparticipant b on (a.SPDTraderID = b.SPDTraderID and b.IsCurrentFlag = 'Y') where caseID = '" + caseid + "' and OfferTypeCode in ('ENOF','ILRO','PLRO','TWRO') GROUP BY `Interval`, CaseID, TradingPeriodNumber , a.PNodeCode, b.ParticipantCode ORDER BY Period, Offer")

df = df_traderperiod.filter("`Interval` = '"+ tradinginterval +"'")
if df.count() > 0: df_traderperiod = df
display(df_traderperiod)

# COMMAND ----------

# DBTITLE 1,Get SPD Pnodes' result
df_node = spark.sql("select CaseID, `Interval`, TradingPeriodNumber as Period, PNodeCode as Pnode,SPDGenerationMegawatts as SPD_Generation,SPDLoadMegawatts as SPD_Load, SPDDollarsPerMegawattHour as SPD_price, SPDEnergyShortfallMegawatts as SPD_deficit, SPDEnergyShortfallTransferredFromMegawatts as Enrgy_Transferred_From, SPDEnergyShortfallTransferredToMegawatts as Enrgy_Transferred_To from copper.spd_" + casetype + "_solution_pnode where CaseID = '" + caseid + "' ORDER BY `Interval`, Pnode")

df = df_node.filter("`Interval` = '"+ tradinginterval +"'")
if df.count() > 0: df_node = df
display(df_node)  


# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from sensitive.spdnodal where caseID  = ${caseid}  and PointOfConnectionCode like 'TMK0331'

# COMMAND ----------

# DBTITLE 1,Get SPD Branch Group Constraints' result
df_Brcnstr = spark.sql("select CaseID, `Interval`, TradingPeriodNumber as Period, trim(SPDConstraintName) as ConstraintName, SPDConstraintMegawatts as Value, SPDConstraintUpperLimitMegawatts + SPDConstraintLowerLimitMegawatts as Limit, SPDConstraintUpperLimitMarginalPrice + SPDConstraintLowerLimitMarginalPrice as MarginalPrice, * from copper.spd_" + casetype + "_solution_constraint where SPDConstraintType = 'Branch group' and CaseID = '" + caseid + "' ORDER BY `Interval`, Period, ConstraintName")

df = df_Brcnstr .filter("`Interval` = '"+ tradinginterval +"'")
if df.count() > 0: df_Brcnstr = df
display(df_Brcnstr.orderBy('Period','ConstraintName'))

display(df_Brcnstr.filter("ConstraintName like '%P2max'").orderBy('Period','ConstraintName'))

# COMMAND ----------

# DBTITLE 1,Get SPD Mnode Constraints' result
df_MNcnstr = spark.sql("select CaseID, `Interval`, TradingPeriodNumber as Period, trim(SPDConstraintName) as ConstraintName, SPDConstraintMegawatts as Value, SPDConstraintUpperLimitMegawatts + SPDConstraintLowerLimitMegawatts as Limit, SPDConstraintUpperLimitMarginalPrice as MarginalPrice from copper.spd_" + casetype + "_solution_constraint where SPDConstraintType = 'Market node' and CaseID = '" + caseid + "' ORDER BY `Interval`, Period, ConstraintName")

df = df_MNcnstr .filter("`Interval` = '"+ tradinginterval +"'")
if df.count() > 0: df_MNcnstr = df
display(df_MNcnstr)

# COMMAND ----------

# DBTITLE 1,Get Bus result
# df_Bus = spark.sql("select CaseID, `Interval`, TradingPeriodNumber as Period, * from copper.spd_" + casetype + "_solution_bus where CaseID = '" + caseid + "' ORDER BY `Interval`, Period")

# df = df_Bus .filter("`Interval` = '"+ tradinginterval +"'")
# if df.count() > 0: df_Bus = df
# display(df_Bus)


# COMMAND ----------

# %sql
# select SUBSTRING(FileName,CHARINDEX('MSS_', FileName) + 4, CHARINDEX('_0X', FileName) - (CHARINDEX('MSS_', FileName) + 4)) AS CaseID, to_date(`INTERVAL`, "dd-MMM-yyyy HH:mm") AS TRADINGDATE,`INTERVAL`,CONSTRAINTNAME,* from bronze.spd_prss_solution_constraint where CONSTRAINTTYPE = 'BrCnst' and SENSITIVITY <> 0
# and to_date(`INTERVAL`, "dd-MMM-yyyy HH:mm") >= '2022-10-31'
# ORDER BY TRADINGDATE,`INTERVAL`

# COMMAND ----------

# %sql
# select * from silver.dimspdcase
