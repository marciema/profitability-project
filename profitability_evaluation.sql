--Part 1
--Seller profitability trend throughout time by market
--Focus on US market only for now
create or replace table profitability_evaluation_etl as
select 
p.user_token
,p.country_code
,p.cohort_date
,du.business_category
,p.gpv_band
,sum(profit) as profit
,sum(TOT_ADJUSTED_REVENUE_LOCAL_PROCESSING-TOT_COGS_ESTIMATED_LOCAL_PROCESSING) as processing_profit
,sum(TOT_ADJUSTED_REVENUE_LOCAL_SAAS
-TOT_COGS_ESTIMATED_LOCAL_SAAS
+TOT_ADJUSTED_REVENUE_LOCAL_HW
-TOT_COGS_ESTIMATED_LOCAL_HW
+TOT_ADJUSTED_REVENUE_LOCAL_CAPITAL
-TOT_COGS_ESTIMATED_LOCAL_CAPITAL
+TOT_ADJUSTED_REVENUE_LOCAL_SC
-TOT_COGS_ESTIMATED_LOCAL_SC) as non_processing_profit
,sum(ESTIMATED_LOSS) as estimated_loss
,sum(trailing_180d_usd_gpv) as "180d_gpv_usd"
from app_risk.app_risk.seller_profit_snapshots_v2 p
left join app_bi.pentagon.dim_user du
on p.user_token = du.user_token
where p.country_code = 'US'
and cohort_date between '2021-01-04' and '2022-02-14'
group by 1,2,3,4,5
order by 3
;

--Item 1: Sum of seller profitability, profitability breakdown by processing and non-processing by business type and gpv band
select 
country_code
,cohort_date
,business_category
,gpv_band
,count(user_token) as seller_cnt
,sum(profit) as total_profit
,sum(processing_profit) as processing_profit
,sum(non_processing_profit) as non_processing_profit
,sum(ESTIMATED_LOSS) as estimated_loss
,sum("180d_gpv_usd") as "180d_gpv_usd"
from profitability_evaluation_etl
group by 1,2,3,4
order by 2,4
;

--add-on: seller with non processing profit
select 
cohort_date
--,business_category
,gpv_band
,sum(case when non_processing_profit>0 then 1 else 0 end) as seller_cnt_w_non_processing_profit
,count(user_token) as total_seller_cnt
from profitability_evaluation_etl
group by 1,2
order by 1,2
; 


--Item 2: Sum of seller profitability, profitability breakdown by processing and non-processing stats (4 stats to cover: mean, median, 10/90 percentile) 
SELECT DISTINCT
cohort_date
,business_category
,AVG(profit) OVER (PARTITION BY cohort_date, business_category) as total_profit_avg
,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY profit) OVER (PARTITION BY cohort_date,business_category) as total_profit_median
,PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY profit) OVER (PARTITION BY cohort_date,business_category) as total_profit_10_percentile
,PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY profit) OVER (PARTITION BY cohort_date,business_category) as total_profit_90_percentile
,AVG(processing_profit) OVER (PARTITION BY cohort_date, business_category) as processing_profit_avg
,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY processing_profit) OVER (PARTITION BY cohort_date,business_category) as processing_profit_median
,PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY processing_profit) OVER (PARTITION BY cohort_date,business_category) as processing_profit_10_percentile
,PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY processing_profit) OVER (PARTITION BY cohort_date,business_category) as processing_profit_90_percentile
,AVG(non_processing_profit) OVER (PARTITION BY cohort_date, business_category) as non_processing_profit_avg
,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY non_processing_profit) OVER (PARTITION BY cohort_date,business_category) as non_processing_profit_median
,PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY non_processing_profit) OVER (PARTITION BY cohort_date,business_category) as non_processing_profit_10_percentile
,PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY non_processing_profit) OVER (PARTITION BY cohort_date,business_category) as non_processing_profit_90_percentile
,AVG(estimated_loss) OVER (PARTITION BY cohort_date, business_category) as estimated_loss_avg
,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY estimated_loss) OVER (PARTITION BY cohort_date,business_category) as estimated_loss_median
,PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY estimated_loss) OVER (PARTITION BY cohort_date,business_category) as estimated_loss_10_percentile
,PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY estimated_loss) OVER (PARTITION BY cohort_date,business_category) as estimated_loss_90_percentile
from profitability_evaluation_etl
order by cohort_date, business_category
;

SELECT DISTINCT
cohort_date
,gpv_band
,AVG(profit) OVER (PARTITION BY cohort_date, gpv_band) as total_profit_avg
,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY profit) OVER (PARTITION BY cohort_date,gpv_band) as total_profit_median
,PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY profit) OVER (PARTITION BY cohort_date,gpv_band) as total_profit_10_percentile
,PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY profit) OVER (PARTITION BY cohort_date,gpv_band) as total_profit_90_percentile
,AVG(processing_profit) OVER (PARTITION BY cohort_date, gpv_band) as processing_profit_avg
,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY processing_profit) OVER (PARTITION BY cohort_date,gpv_band) as processing_profit_median
,PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY processing_profit) OVER (PARTITION BY cohort_date,gpv_band) as processing_profit_10_percentile
,PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY processing_profit) OVER (PARTITION BY cohort_date,gpv_band) as processing_profit_90_percentile
,AVG(non_processing_profit) OVER (PARTITION BY cohort_date, gpv_band) as non_processing_profit_avg
,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY non_processing_profit) OVER (PARTITION BY cohort_date,gpv_band) as non_processing_profit_median
,PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY non_processing_profit) OVER (PARTITION BY cohort_date,gpv_band) as non_processing_profit_10_percentile
,PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY non_processing_profit) OVER (PARTITION BY cohort_date,gpv_band) as non_processing_profit_90_percentile
,AVG(estimated_loss) OVER (PARTITION BY cohort_date, gpv_band) as estimated_loss_avg
,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY estimated_loss) OVER (PARTITION BY cohort_date,gpv_band) as estimated_loss_median
,PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY estimated_loss) OVER (PARTITION BY cohort_date,gpv_band) as estimated_loss_10_percentile
,PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY estimated_loss) OVER (PARTITION BY cohort_date,gpv_band) as estimated_loss_90_percentile
from profitability_evaluation_etl
order by cohort_date, gpv_band
;

--focus on sellers with non-procesing revenue
SELECT DISTINCT
business_category
,AVG(non_processing_profit) OVER (PARTITION BY cohort_date, business_category) as non_processing_profit_avg
,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY non_processing_profit) OVER (PARTITION BY cohort_date,business_category) as non_processing_profit_median
,PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY non_processing_profit) OVER (PARTITION BY cohort_date,business_category) as non_processing_profit_10_percentile
,PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY non_processing_profit) OVER (PARTITION BY cohort_date,business_category) as non_processing_profit_90_percentile
from profitability_evaluation_etl
where cohort_date = '2022-02-14'
and non_processing_profit > 0
order by 1
;

SELECT DISTINCT
gpv_band
,AVG(non_processing_profit) OVER (PARTITION BY cohort_date, gpv_band) as non_processing_profit_avg
,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY non_processing_profit) OVER (PARTITION BY cohort_date,gpv_band) as non_processing_profit_median
,PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY non_processing_profit) OVER (PARTITION BY cohort_date,gpv_band) as non_processing_profit_10_percentile
,PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY non_processing_profit) OVER (PARTITION BY cohort_date,gpv_band) as non_processing_profit_90_percentile
from profitability_evaluation_etl
where cohort_date = '2022-02-14'
and non_processing_profit > 0
order by 1
;

--total profile's stats, to define profitability bands
SELECT DISTINCT
cohort_date
,AVG(profit) OVER (PARTITION BY cohort_date) as total_profit_avg
,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY profit) OVER (PARTITION BY cohort_date) as total_profit_median
,PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY profit) OVER (PARTITION BY cohort_date) as total_profit_10_percentile
,PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY profit) OVER (PARTITION BY cohort_date) as total_profit_90_percentile
from profitability_evaluation_etl
order by cohort_date
;

--Item 3 (understanding negative profitability sellers’ behavior change):
--For sellers with negative profitability on cohort date 2021-01-04, track counts of sellers with different profitability bands throughout time until 2022-02-14
select 
cohort_date
,case when profit<0 then '1. <0'
when profit >= 0 and profit <40 then '2. 0-40'
when profit >= 40 and profit <250 then '3. 40-250'
when profit >= 250 and profit <600 then '4. 250-600'
when profit >= 600 then '5. >=600' end as profitability_band
,count(user_token)
from profitability_evaluation_etl
where user_token in (select distinct user_token
                     from profitability_evaluation_etl
                     where cohort_date = '2021-01-04' and profit >=0)
group by 1,2
order by 1,2
;

-- current status for sellers with negative profitability on cohort date 2021-01-04
select 
sum(case when unit_active_status= 1 then 1 else 0 end) as active_seller_cnt,
sum(case when is_currently_frozen= 1 then 1 else 0 end) as currently_fronzen_seller_cnt,
sum(case when has_been_frozen= 1 then 1 else 0 end) as has_been_fronzen_seller_cnt,
sum(case when is_currently_deactivated= 1 then 1 else 0 end) as currently_deactivated_seller_cnt,
sum(case when has_been_deactivated= 1 then 1 else 0 end) as has_been_deactivated_seller_cnt,
count(p.user_token) as total_seller_cnt
from (select distinct user_token
      from profitability_evaluation_etl
      where cohort_date = '2021-01-04' and profit<0) p
left join app_bi.pentagon.dim_user du
on p.user_token = du.user_token
;


--Part 2
--Population: sellers at end of 2021 Q1 cohort date (2021-03-29), payments in 2021 Q2
--divide profit into 10 bands
SELECT DISTINCT
PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY profit) OVER (PARTITION BY cohort_date) as total_profit_1
,PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY profit) OVER (PARTITION BY cohort_date) as total_profit_2
,PERCENTILE_CONT(0.3) WITHIN GROUP (ORDER BY profit) OVER (PARTITION BY cohort_date) as total_profit_3
,PERCENTILE_CONT(0.4) WITHIN GROUP (ORDER BY profit) OVER (PARTITION BY cohort_date) as total_profit_4
,PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY profit) OVER (PARTITION BY cohort_date) as total_profit_5
,PERCENTILE_CONT(0.6) WITHIN GROUP (ORDER BY profit) OVER (PARTITION BY cohort_date) as total_profit_6
,PERCENTILE_CONT(0.7) WITHIN GROUP (ORDER BY profit) OVER (PARTITION BY cohort_date) as total_profit_7
,PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY profit) OVER (PARTITION BY cohort_date) as total_profit_8
,PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY profit) OVER (PARTITION BY cohort_date) as total_profit_9
from profitability_evaluation_etl
where cohort_date = '2021-03-29'
;

create or replace table "21Q1_user_profit_band" as
select 
user_token
,profit
,case when profit <= 0.7115785646 then 'band_1'
when profit >0.7115785646 and profit <= 3.3529 then 'band_2'
when profit >3.3529 and profit <= 8.6836 then 'band_3'
when profit >8.6836 and profit <= 18.91858744 then 'band_4'
when profit >18.91858744 and profit <= 37.65054913 then 'band_5'
when profit >37.65054913 and profit <= 71.0726 then 'band_6'
when profit >71.0726 and profit <= 130.4869229 then 'band_7'
when profit >130.4869229 and profit <= 246.7119817 then 'band_8'
when profit >246.7119817 and profit <= 550.5771426 then 'band_9'
when profit >550.5771426 then 'band_10' end as profit_bands
from profitability_evaluation_etl
where cohort_date = '2021-03-29'
;

select profit_bands, count(user_token),sum(profit)
from "21Q1_USER_PROFIT_BAND"
group by 1
order by 1
;

-- cb and loss for 2021-03-29 cohort sellers
select
profit_bands
--gpv_band,
--business_category,
,count(distinct user_token) as total_cnt
,sum(cb_ind) as cb_cnt
,sum(loss_ind) as loss_cnt
,sum(cb_dllr) as sum_cb_dllr
,sum(loss_dllr) as sum_loss_dllr
,sum(qtly_gpv) as gpv
from (select
     p.user_token
    ,profit_bands
    ,case when cb_dllr>0 then 1 else 0 end as cb_ind
    ,case when loss_dllr>0 then 1 else 0 end as loss_ind
    ,cb_dllr
    ,loss_dllr
    ,qtly_gpv
    from "21Q1_USER_PROFIT_BAND" p
    left join (select distinct user_token, sum(gpv_payment_amount_base_unit_usd/100) as qtly_gpv
              from app_bi.pentagon.aggregate_seller_daily_payment_summary
               where payment_trx_recognized_date between '2021-04-01' and '2021-06-30'
              group by 1) gpv
    on p.user_token = gpv.user_token
    left join (select distinct user_token, sum(chargeback_cents/100) as cb_dllr, sum(loss_cents/100) as loss_dllr   
               from app_risk.app_risk.chargebacks
                where payment_created_at between '2021-04-01' and '2021-06-30'
               and taxonomy_category_name not in ('ATO Fraud', 'Fake Business Known Fraud')
               --and type in ('credit')
            group by 1) cb
    on p.user_token = cb.user_token)
group by 1
order by 1
;

-- profit for 2021-03-29 cohort sellers
select
profit_bands
,count(distinct p.user_token) as total_cnt
,sum(case when p1.user_token is null then 1 else 0 end) as inactive_seller_cnt
,sum(p1.profit)
from "21Q1_USER_PROFIT_BAND" p
left join (select user_token, profit
           from profitability_evaluation_etl
          where cohort_date = '2021-06-28') p1
on p.user_token = p1.user_token
group by 1
order by 1
;

select count(distinct user_token)
from app_bi.pentagon.aggregate_seller_daily_payment_summary
where payment_trx_recognized_date between '2021-04-01' and '2021-06-30'
and gpv_payment_amount_base_unit >0 
and COUNTRY_CODE = 'US'
;

select 
count(distinct user_token)
--from "21Q1_user_profit_band"
from app_risk.app_risk.seller_profit_snapshots_v2
where cohort_date = '2021-03-29'
and country_code = 'US'
--and profit < 0 
;

--non-processing adj revenue deepdive
select 
cohort_date
,du.business_category
,sum(TOT_ADJUSTED_REVENUE_LOCAL_SAAS)/count(p.user_token) as avg_saas_revenue
,sum(TOT_COGS_ESTIMATED_LOCAL_SAAS)/count(p.user_token) as avg_saas_cost
,sum(TOT_ADJUSTED_REVENUE_LOCAL_HW)/count(p.user_token) as avg_hw_revenue
,sum(TOT_COGS_ESTIMATED_LOCAL_HW)/count(p.user_token) as avg_hw_cost
,sum(TOT_ADJUSTED_REVENUE_LOCAL_CAPITAL)/count(p.user_token) as avg_capital_revenue
,sum(TOT_COGS_ESTIMATED_LOCAL_CAPITAL)/count(p.user_token) as avg_capital_cost
,sum(TOT_ADJUSTED_REVENUE_LOCAL_SC)/count(p.user_token) as avg_sc_revenue
,sum(TOT_COGS_ESTIMATED_LOCAL_SC)/count(p.user_token) as avg_sc_cost
,count(p.user_token)
from app_risk.app_risk.seller_profit_snapshots_v2 p
left join app_bi.pentagon.dim_user du
on p.user_token = du.user_token
where p.country_code = 'US'
and cohort_date in ('2022-02-14') --'2021-12-06','2021-12-13')
and business_category = 'beauty_and_person_care'
group by 1,2
order by 1,2
;

-- deepdive
select 
cohort_date, round(max(estimated_loss),0)
from profitability_evaluation_etl
where gpv_band = '04.1MM+'
--and cohort_date = '2022-02-14' 
--order by estimated_loss desc
--limit 100
group by 1
order by 2 desc
;
