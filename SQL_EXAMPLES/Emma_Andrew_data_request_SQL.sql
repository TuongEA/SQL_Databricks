-- Databricks notebook source
select * from gold.ftr_monthly_rental --WHERE Start_Date between '2021-01-01' and '2021-12-31'

-- COMMAND ----------

-- DBTITLE 1,Get FTR total cash flow by financial year
select 
  -- CASE 
  --   WHEN month(Start_Date) >= 7 then cast(year(Start_Date) as STRING) || '/' || cast(year(Start_Date)+1 as STRING) 
  --   ELSE cast(year(Start_Date)-1 as STRING) || '/' || cast(year(Start_Date) as STRING) 
  -- END as FinancialYear, 
  year(Start_Date) as FinancialYear, 
  min(Start_Date) as StartDate, max(End_Date) as EndDate, 
  round(sum(FTR_Auction_Revenue_Dollars)/1e6,3) as Total_Auction_Revenue_Mil,
  round(sum(FTR_Payable_Funded_From_Auction_Revenue_Dollars)/1e6,3) as Total_Auction_Revenue_Payback_to_FTR_Participant,
  round(sum(FTR_Payable_Funded_From_FTR_Rental_Dollars)/1e6,3) as Total_FTR_Rental_Pay_to_FTR_Participant,
  round((sum(FTR_Payable_Funded_From_FTR_Rental_Dollars) + sum(FTR_Payable_Funded_From_Auction_Revenue_Dollars) - sum(FTR_Auction_Revenue_Dollars))/1e6,3)  as Total_Net_Pay_to_FTR_Participant_Mil
from gold.ftr_monthly_rental
GROUP BY 
  -- CASE 
  --   WHEN month(Start_Date) >= 7 then cast(year(Start_Date) as STRING) || '/' || cast(year(Start_Date)+1 as STRING) 
  --   ELSE cast(year(Start_Date)-1 as STRING) || '/' || cast(year(Start_Date) as STRING) 
  -- END
  year(Start_Date)
ORDER BY StartDate

-- COMMAND ----------

select 
  year(Start_Date) as FinancialYear, 
  min(Start_Date) as StartDate, max(End_Date) as EndDate, 
  round(sum(FTR_Auction_Revenue_Dollars)/1e6,3) as Total_Auction_Revenue_Mil,
  round(sum(FTR_Payable_Funded_From_Auction_Revenue_Dollars)/1e6,3) as Total_Auction_Revenue_Payback_to_FTR_Participant,
  round(sum(FTR_Payable_Funded_From_FTR_Rental_Dollars)/1e6,3) as Total_FTR_Rental_Pay_to_FTR_Participant,
  round((sum(FTR_Payable_Funded_From_FTR_Rental_Dollars) + sum(FTR_Payable_Funded_From_Auction_Revenue_Dollars) - sum(FTR_Auction_Revenue_Dollars))/1e6,3)  as Total_Net_Pay_to_FTR_Participant_Mil
from gold.ftr_monthly_rental
GROUP BY 
  -- CASE 
  --   WHEN month(Start_Date) >= 7 then cast(year(Start_Date) as STRING) || '/' || cast(year(Start_Date)+1 as STRING) 
  --   ELSE cast(year(Start_Date)-1 as STRING) || '/' || cast(year(Start_Date) as STRING) 
  -- END
  year(Start_Date)
ORDER BY StartDate
