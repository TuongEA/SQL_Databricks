-- Databricks notebook source
SELECT 
year(FTRContractStartDate) as `Year`, FTRContractStartDate as Start_Date, FTRContractEndDate as End_Date, 
case  when FTRPaymentScalingFactor < 1 then FTRHedgeAvailableFundDollars else FTRNetHedgeValuePayableDollars end as `Payment to FTR participants at Settlement`,

FTRAuctionRevenueDollars as `Payment from FTR participants as Auction Revenue`,
FTRFinalRentalDollars as `FTR rentals (L&C avaiable for FTR market)`,
case  when FTRPaymentScalingFactor < 1 then FTRAuctionRevenueDollars else FTRPayableFundedFromAuctionRevenueDollars end as`Auction Revenue used for FTR Settlement`,
case  when FTRPaymentScalingFactor < 1 then FTRFinalRentalDollars  else FTRPayableFundedFromFTRRentalDollars end as `FTR rental used for FTR Settlement`,
FTRAuctionRevenueDollars - FTRPayableFundedFromAuctionRevenueDollars as `Residual of Auction Revenue go to TransmissionCustomer`,
FTRFinalRentalDollars - FTRPayableFundedFromFTRRentalDollars as `Residual of FTR rental go to TransmissionCustomer`,
NonFTRLossAndConstraintExcessDollars as `non-FTR rental`
FROM ea_prd.gold.factftrrental
ORDER BY FTRContractStartDate

-- COMMAND ----------

SELECT 
year(FTRContractStartDate) as `Year`, FTRContractStartDate as Start_Date, FTRContractEndDate as End_Date, 
case  when FTRPaymentScalingFactor < 1 then FTRHedgeAvailableFundDollars else FTRNetHedgeValuePayableDollars end as `Payment to FTR participants at Settlement`,

FTRAuctionRevenueDollars as `Payment from FTR participants as Auction Revenue`,
FTRFinalRentalDollars as `FTR rentals (L&C avaiable for FTR market)`,
case  when FTRPaymentScalingFactor < 1 then FTRAuctionRevenueDollars else FTRPayableFundedFromAuctionRevenueDollars end as`Auction Revenue used for FTR Settlement`,
case  when FTRPaymentScalingFactor < 1 then FTRFinalRentalDollars  else FTRPayableFundedFromFTRRentalDollars end as `FTR rental used for FTR Settlement`,
FTRAuctionRevenueDollars - FTRPayableFundedFromAuctionRevenueDollars as `Residual of Auction Revenue go to TransmissionCustomer`,
FTRFinalRentalDollars - FTRPayableFundedFromFTRRentalDollars as `Residual of FTR rental go to TransmissionCustomer`,
NonFTRLossAndConstraintExcessDollars as `non-FTR rental`
FROM datahub.forwardmarkets.ftrrental
ORDER BY FTRContractStartDate
