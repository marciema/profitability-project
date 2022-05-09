--Start from the version 1 historical table app_risk.app_risk.seller_profit_snapshots
CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_cb_loading_2019_2020 AS
SELECT
cb.user_token
,ct.cohort_date
,SUM(CASE WHEN currency_code = 'JPY' THEN loss_cents ELSE loss_cents/100 END) AS realized_loss
,SUM(CASE WHEN currency_code = 'JPY' and type = 'credit' THEN loss_cents 
     WHEN type = 'credit' THEN loss_cents/100
     ELSE 0 END) AS realized_credit_loss
FROM (select * from app_risk.app_risk.hist_seller_profit_calculation_cohort_2019
      union all
      select * from app_risk.app_risk.hist_seller_profit_calculation_cohort_2020
     ) ct
INNER JOIN (SELECT user_token, currency_code,loss_cents,type,payment_created_at,snapshot_date
            FROM app_risk.reporting.chargebacks_snapshots_archive)cb ON ct.cohort_date = cb.snapshot_date AND payment_created_at > DATEADD(DAY,-180,ct.cohort_date) AND to_date(payment_created_at)<= ct.cohort_date
GROUP BY 1,2
;

-- min(snapshot_date) chargebacks_snapshots_archive is 2020-03-09, for dates before 2020-03-09, use 2020-03-09 values
create or replace table app_risk.app_risk.hist_seller_profit_calculation_cb_loading_2020_03_09 as
select *
from app_risk.app_risk.hist_seller_profit_calculation_cb_loading_2019_2020
where cohort_date = '2020-03-09'
;

CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_v2_loading_2019_2020 AS
SELECT 
r.user_token
,r.cohort_date
,r.currency_code
,r.trailing_180d_local_gpv
,r.trailing_180d_usd_gpv
,r.business_name
,r.business_type
,r.best_available_merchant_token
,r.country_code
,r.tot_adjusted_revenue_local_processing
,r.tot_cogs_estimated_local_processing
,r.tot_adjusted_revenue_local_saas
,r.tot_cogs_estimated_local_saas
,r.tot_adjusted_revenue_local_hw
,r.tot_cogs_estimated_local_hw
,r.tot_adjusted_revenue_local_capital
,r.tot_cogs_estimated_local_capital
,r.tot_adjusted_revenue_local_sc
,r.tot_cogs_estimated_local_sc
,r.probability_default
,case when r.cohort_date >= '2020-03-09' then cb.realized_loss else cb1.realized_loss end as realized_loss
,case when r.cohort_date >= '2020-03-09' then cb.realized_credit_loss else cb1.realized_credit_loss end as realized_credit_loss
,r.lgd_loss_dollar
,greatest(COALESCE(r.probability_default*r.lgd_loss_dollar,0), COALESCE(case when r.cohort_date >= '2020-03-09' then cb.realized_loss else cb1.realized_loss end,0)) AS estimated_loss
,tot_adjusted_revenue_local_processing
-tot_cogs_estimated_local_processing
+tot_adjusted_revenue_local_saas
-tot_cogs_estimated_local_saas
+tot_adjusted_revenue_local_hw
-tot_cogs_estimated_local_hw
+tot_adjusted_revenue_local_capital
-tot_cogs_estimated_local_capital
+tot_adjusted_revenue_local_sc
-tot_cogs_estimated_local_sc
-estimated_loss AS profit
,r.pd_score
,r.gpv_annualized_estimate
,r.gpv_band
,r.adjusted_revenue_local_processing
,r.cogs_estimated_local_processing
,r.adjusted_revenue_local_saas
,r.cogs_estimated_local_saas
,r.adjusted_revenue_local_hw
,r.cogs_estimated_local_hw
,r.adjusted_revenue_local_capital
,r.cogs_estimated_local_capital
,r.adjusted_revenue_local_sc
,r.cogs_estimated_local_sc
,r.mer_adjusted_revenue_local_saas
,r.mer_cogs_estimated_local_saas
,r.mer_adjusted_revenue_local_hw
,r.mer_cogs_estimated_local_hw
,r.mer_adjusted_revenue_local_capital
,r.mer_cogs_estimated_local_capital
,r.unit_cnt
,CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS etl_created_at
FROM app_risk.app_risk.seller_profit_snapshots_2019_2020 r
LEFT JOIN app_risk.app_risk.hist_seller_profit_calculation_cb_loading_2019_2020 cb ON r.user_token = cb.user_token AND r.cohort_date = cb.cohort_date
LEFT JOIN app_risk.app_risk.hist_seller_profit_calculation_cb_loading_2020_03_09 cb1 ON r.user_token = cb1.user_token AND r.cohort_date < cb1.cohort_date
;

select 
count(*), count(distinct user_token, cohort_date)
from app_risk.app_risk.hist_seller_profit_calculation_v2_loading_2019_2020
;

CREATE TABLE app_risk.app_risk.seller_profit_snapshots_2019_2020_v2 AS
SELECT * 
FROM app_risk.app_risk.hist_seller_profit_calculation_v2_loading_2019_2020
;



