-----------01.Daily GPV--------------------
CREATE OR REPLACE TABLE app_risk.app_risk_test.seller_profit_daily_calculation_gpv_2019_2020 AS
SELECT
    gpv.user_token
    , payment_trx_recognized_date AS report_date
    , gpv.currency_code
    , CASE WHEN gpv.currency_code = 'JPY' THEN gpv.gpv_payment_amount_base_unit ELSE gpv.gpv_payment_amount_base_unit/100 END AS local_gpv
    , CASE WHEN gpv.currency_code = 'JPY' THEN gpv.gpv_payment_amount_base_unit_usd ELSE gpv.gpv_payment_amount_base_unit_usd/100 END AS usd_gpv
FROM app_bi.pentagon.aggregate_seller_daily_payment_summary AS gpv
WHERE YEAR(gpv.payment_trx_recognized_date) in (2019, 2020)
    AND gpv_payment_count > 0
;

-----------02a.Daily Revenue and get their token type: merchant token vs unit token--------------------
CREATE OR REPLACE TABLE app_risk.app_risk_test.seller_profit_daily_calculation_rev_2019_2020 AS
SELECT 
    r.user_token
    , r.report_date
    , r.product_category
    , du.is_unit
    , du.is_merchant
    , du.country_code
    , IFNULL(SUM(r.adjusted_revenue_base_unit/100),0) AS adjusted_revenue_local
    , IFNULL(SUM(r.cogs_estimated_base_unit/100),0) AS cogs_estimated_local
    , IFNULL(SUM(r.costplus_adjustment_base_unit/100),0) AS costplus_adjustment_local
    , IFNULL(SUM(r.costplus_estimated_base_unit/100),0) AS costplus_estimated_local
FROM app_bi.app_bi_dw.vfact_revenue_summary r
LEFT JOIN app_bi.pentagon.dim_user du 
    ON r.user_token = du.user_token
WHERE YEAR(r.report_date) in (2019,2020)
GROUP BY 1,2,3,4,5,6
;

-----------02b.Daily Revenue - if token type is merchant token then get number of unit tokens that took a payment in the past year--------------------
CREATE OR REPLACE TABLE app_risk.app_risk_test.seller_profit_daily_calculation_rev_mer_2019_2020 AS
SELECT DISTINCT
    asdps.unit_token
    , r.*
    , COUNT(DISTINCT asdps.unit_token) OVER (PARTITION BY r.user_token, r.report_date) AS unit_cnt
FROM app_bi.pentagon.aggregate_seller_daily_payment_summary asdps
JOIN app_bi.pentagon.dim_user du 
    ON du.user_token = asdps.user_token
JOIN app_risk.app_risk_test.seller_profit_daily_calculation_rev_2019_2020 r
    ON r.user_token = du.best_available_merchant_token
    AND DATEADD(DAY,-365,r.report_date) < asdps.payment_trx_recognized_date
    AND r.report_date >= asdps.payment_trx_recognized_date
WHERE r.is_merchant = 1;

-----------03.Merge first two tables and get each item as variable in both unit token and merchant token vars--------------------
CREATE OR REPLACE TABLE app_risk.app_risk_test.seller_profit_daily_calculation_gpv_rev_2019_2020 AS
SELECT
    gpv.user_token AS unit_token_gpv
    , gpv.report_date AS report_date_gpv
    , COALESCE(gpv.local_gpv, 0) AS local_gpv
    , COALESCE(gpv.usd_gpv, 0) AS usd_gpv
    , r.user_token AS unit_token_rev
    , r.report_date AS report_date_rev
    , CASE WHEN r.country_code = 'JP' THEN COALESCE(r.adjusted_revenue_local_processing,0) *100 ELSE COALESCE(r.adjusted_revenue_local_processing,0)  END AS adjusted_revenue_local_processing
    , CASE WHEN r.country_code = 'JP' THEN COALESCE(r.cogs_estimated_local_processing,0)   *100 ELSE COALESCE(r.cogs_estimated_local_processing,0)    END AS cogs_estimated_local_processing
    , CASE WHEN r.country_code = 'JP' THEN COALESCE(r.adjusted_revenue_local_saas,0)       *100 ELSE COALESCE(r.adjusted_revenue_local_saas,0)        END AS adjusted_revenue_local_saas
    , CASE WHEN r.country_code = 'JP' THEN COALESCE(r.cogs_estimated_local_saas,0)         *100 ELSE COALESCE(r.cogs_estimated_local_saas,0)          END AS cogs_estimated_local_saas    
    , CASE WHEN r.country_code = 'JP' THEN COALESCE(r.adjusted_revenue_local_hw,0)         *100 ELSE COALESCE(r.adjusted_revenue_local_hw,0)          END AS adjusted_revenue_local_hw   
    , CASE WHEN r.country_code = 'JP' THEN COALESCE(r.cogs_estimated_local_hw,0)           *100 ELSE COALESCE(r.cogs_estimated_local_hw,0)            END AS cogs_estimated_local_hw     
    , CASE WHEN r.country_code = 'JP' THEN COALESCE(r.adjusted_revenue_local_capital,0)    *100 ELSE COALESCE(r.adjusted_revenue_local_capital,0)     END AS adjusted_revenue_local_capital
    , CASE WHEN r.country_code = 'JP' THEN COALESCE(r.cogs_estimated_local_capital,0)      *100 ELSE COALESCE(r.cogs_estimated_local_capital,0)       END AS cogs_estimated_local_capital
    , CASE WHEN r.country_code = 'JP' THEN COALESCE(r.adjusted_revenue_local_sc,0)         *100 ELSE COALESCE(r.adjusted_revenue_local_sc,0)          END AS adjusted_revenue_local_sc
    , CASE WHEN r.country_code = 'JP' THEN COALESCE(r.cogs_estimated_local_sc,0)           *100 ELSE COALESCE(r.cogs_estimated_local_sc,0)            END AS cogs_estimated_local_sc   
    ,  m.unit_token AS unit_token_mer_rev
    , m.user_token AS merchant_token_mer_rev
    , m.unit_cnt
    , m.report_date AS report_date_mer_rev
    , CASE WHEN m.country_code = 'JP' THEN COALESCE(m.mer_adjusted_revenue_local_saas,0)   *100 ELSE COALESCE(m.mer_adjusted_revenue_local_saas,0)    END AS mer_adjusted_revenue_local_saas
    , CASE WHEN m.country_code = 'JP' THEN COALESCE(m.mer_cogs_estimated_local_saas,0)     *100 ELSE COALESCE(m.mer_cogs_estimated_local_saas,0)      END AS mer_cogs_estimated_local_saas
    , CASE WHEN m.country_code = 'JP' THEN COALESCE(m.mer_adjusted_revenue_local_hw,0)     *100 ELSE COALESCE(m.mer_adjusted_revenue_local_hw,0)      END AS mer_adjusted_revenue_local_hw
    , CASE WHEN m.country_code = 'JP' THEN COALESCE(m.mer_cogs_estimated_local_hw,0)       *100 ELSE COALESCE(m.mer_cogs_estimated_local_hw,0)        END AS mer_cogs_estimated_local_hw  
    , CASE WHEN m.country_code = 'JP' THEN COALESCE(m.mer_adjusted_revenue_local_capital,0)*100 ELSE COALESCE(m.mer_adjusted_revenue_local_capital,0) END AS mer_adjusted_revenue_local_capital
    , CASE WHEN m.country_code = 'JP' THEN COALESCE(m.mer_cogs_estimated_local_capital,0)  *100 ELSE COALESCE(m.mer_cogs_estimated_local_capital,0)   END AS mer_cogs_estimated_local_capital
FROM app_risk.app_risk_test.seller_profit_daily_calculation_gpv_2019_2020 gpv
FULL JOIN 
    (SELECT 
         user_token
         , report_date
         , country_code
         , SUM(CASE WHEN product_category = 'Processing' THEN adjusted_revenue_local - costplus_estimated_local ELSE 0 END) AS adjusted_revenue_local_processing
         , SUM(CASE WHEN product_category = 'Processing' THEN cogs_estimated_local ELSE 0 END) AS cogs_estimated_local_processing
         , SUM(CASE WHEN product_category = 'SaaS' THEN adjusted_revenue_local ELSE 0 END) AS adjusted_revenue_local_saas
         , SUM(CASE WHEN product_category = 'SaaS' THEN cogs_estimated_local ELSE 0 END) AS cogs_estimated_local_saas
         , SUM(CASE WHEN product_category = 'Hardware' THEN adjusted_revenue_local ELSE 0 END) AS adjusted_revenue_local_hw
         , SUM(CASE WHEN product_category = 'Hardware' THEN cogs_estimated_local ELSE 0 END) AS cogs_estimated_local_hw
         , SUM(CASE WHEN product_category = 'Capital' THEN adjusted_revenue_local ELSE 0 END) AS adjusted_revenue_local_capital
         , SUM(CASE WHEN product_category = 'Capital' THEN cogs_estimated_local ELSE 0 END) AS cogs_estimated_local_capital
         , SUM(CASE WHEN product_category = 'Square Card' THEN adjusted_revenue_local ELSE 0 END) AS adjusted_revenue_local_sc
         , SUM(CASE WHEN product_category = 'Square Card' THEN cogs_estimated_local ELSE 0 END) AS cogs_estimated_local_sc
     FROM app_risk.app_risk_test.seller_profit_daily_calculation_rev_2019_2020
     WHERE is_unit = 1
     GROUP BY 1,2,3) r 
    ON gpv.user_token = r.user_token
    AND gpv.report_date = r.report_date
FULL JOIN 
    (SELECT 
         unit_token
         , user_token
         , unit_cnt
         , report_date
         , country_code
         , SUM(CASE WHEN product_category = 'SaaS' THEN adjusted_revenue_local ELSE 0 END) AS mer_adjusted_revenue_local_saas
         , SUM(CASE WHEN product_category = 'SaaS' THEN cogs_estimated_local ELSE 0 END) AS mer_cogs_estimated_local_saas
         , SUM(CASE WHEN product_category = 'Hardware' THEN adjusted_revenue_local ELSE 0 END) AS mer_adjusted_revenue_local_hw
         , SUM(CASE WHEN product_category = 'Hardware' THEN cogs_estimated_local ELSE 0 END) AS mer_cogs_estimated_local_hw
         , SUM(CASE WHEN product_category = 'Capital' THEN adjusted_revenue_local ELSE 0 END) AS mer_adjusted_revenue_local_capital
         , SUM(CASE WHEN product_category = 'Capital' THEN cogs_estimated_local ELSE 0 END) AS mer_cogs_estimated_local_capital
     FROM app_risk.app_risk_test.seller_profit_daily_calculation_rev_mer_2019_2020
     WHERE is_merchant = 1
     GROUP BY 1,2,3,4,5) m 
    ON gpv.user_token = m.unit_token
    AND gpv.report_date = m.report_date
;

-----------04.Sum the revenue and cost for the product with both merchant token level and unit token level--------------------
------------Use merchant level total / # of active unit counts to allocate to unit token level---------------
CREATE OR REPLACE TABLE app_risk.app_risk_test.seller_profit_daily_calculation_gpv_rev_clean_2019_2020 AS
SELECT
    COALESCE(r.unit_token_gpv, r.unit_token_rev, r.unit_token_mer_rev) AS user_token
    , COALESCE(r.report_date_gpv, r.report_date_rev, r.report_date_mer_rev) AS report_date
    , du.business_name
    , du.business_type
    , du.best_available_merchant_token
    , du.country_code
    , SUM(r.local_gpv) AS local_gpv
    , SUM(r.usd_gpv) AS usd_gpv
    , SUM(r.adjusted_revenue_local_processing) AS tot_adjusted_revenue_local_processing
    , SUM(r.cogs_estimated_local_processing) AS tot_cogs_estimated_local_processing
    , SUM(COALESCE(r.adjusted_revenue_local_saas, 0) + COALESCE(mer_adjusted_revenue_local_saas/unit_cnt, 0)) AS tot_adjusted_revenue_local_saas
    , SUM(COALESCE(r.cogs_estimated_local_saas, 0) + COALESCE(mer_cogs_estimated_local_saas/unit_cnt, 0)) AS tot_cogs_estimated_local_saas
    , SUM(COALESCE(r.adjusted_revenue_local_hw, 0) + COALESCE(mer_adjusted_revenue_local_hw/unit_cnt, 0)) AS tot_adjusted_revenue_local_hw
    , SUM(COALESCE(r.cogs_estimated_local_hw, 0) + COALESCE(mer_cogs_estimated_local_hw/unit_cnt, 0)) AS tot_cogs_estimated_local_hw
    , SUM(COALESCE(r.adjusted_revenue_local_capital, 0) + COALESCE(mer_adjusted_revenue_local_capital/unit_cnt, 0)) AS tot_adjusted_revenue_local_capital
    , SUM(COALESCE(r.cogs_estimated_local_capital, 0) + COALESCE(mer_cogs_estimated_local_capital/unit_cnt, 0)) AS tot_cogs_estimated_local_capital
    , SUM(r.adjusted_revenue_local_sc) AS tot_adjusted_revenue_local_sc
    , SUM(r.cogs_estimated_local_sc) AS tot_cogs_estimated_local_sc
    , SUM(r.adjusted_revenue_local_processing) AS adjusted_revenue_local_processing
    , SUM(r.cogs_estimated_local_processing) AS cogs_estimated_local_processing
    , SUM(r.adjusted_revenue_local_saas) AS adjusted_revenue_local_saas
    , SUM(r.cogs_estimated_local_saas) AS cogs_estimated_local_saas
    , SUM(r.adjusted_revenue_local_hw) AS adjusted_revenue_local_hw
    , SUM(r.cogs_estimated_local_hw) AS cogs_estimated_local_hw
    , SUM(r.adjusted_revenue_local_capital) AS adjusted_revenue_local_capital
    , SUM(r.cogs_estimated_local_capital) AS cogs_estimated_local_capital
    , SUM(r.adjusted_revenue_local_sc) AS adjusted_revenue_local_sc
    , SUM(r.cogs_estimated_local_sc) AS cogs_estimated_local_sc
    , SUM(r.mer_adjusted_revenue_local_saas) AS mer_adjusted_revenue_local_saas
    , SUM(r.mer_cogs_estimated_local_saas) AS mer_cogs_estimated_local_saas
    , SUM(r.mer_adjusted_revenue_local_hw) AS mer_adjusted_revenue_local_hw
    , SUM(r.mer_cogs_estimated_local_hw) AS mer_cogs_estimated_local_hw
    , SUM(r.mer_adjusted_revenue_local_capital) AS mer_adjusted_revenue_local_capital
    , SUM(r.mer_cogs_estimated_local_capital) AS mer_cogs_estimated_local_capital
    , MAX(r.unit_cnt) AS unit_cnt
FROM app_risk.app_risk_test.seller_profit_daily_calculation_gpv_rev_2019_2020 r
LEFT JOIN app_bi.pentagon.dim_user du 
    ON COALESCE(r.unit_token_gpv, r.unit_token_rev, r.unit_token_mer_rev) = du.user_token 
GROUP BY 1,2,3,4,5,6
;

----------05.Pull realized loss ---------------
CREATE OR REPLACE TABLE app_risk.app_risk_test.seller_profit_daily_calculation_loss_2019_2020 AS
SELECT 
    cb.user_token
    , cb.payment_created_at::DATE AS report_date
    , SUM(CASE WHEN cb.currency_code = 'JPY' THEN cb.loss_cents ELSE cb.loss_cents/100 END) AS realized_loss
    , SUM(CASE WHEN cb.currency_code = 'JPY' AND cb.type = 'credit' THEN cb.loss_cents 
               WHEN cb.type = 'credit' THEN cb.loss_cents/100
               ELSE 0 END) AS realized_credit_loss
    , SUM(CASE WHEN cb.currency_code = 'JPY' AND cb.type = 'fraud' THEN cb.loss_cents 
               WHEN cb.type = 'fraud' THEN cb.loss_cents/100
               ELSE 0 END) AS realized_fraud_loss
    , SUM(CASE WHEN cb.currency_code = 'JPY' AND (cb.type = 'other' OR cb.type IS NULL) THEN cb.loss_cents 
               WHEN cb.type = 'other' OR cb.type IS NULL THEN cb.loss_cents/100
               ELSE 0 END) AS realized_other_loss
FROM app_risk.app_risk.chargebacks cb
WHERE YEAR(cb.payment_created_at) in (2019,2020)
GROUP BY 1, 2
;

----------06.Merge revenue and realized loss----------------
CREATE OR REPLACE TABLE app_risk.app_risk_test.seller_profit_daily_calculation_2019_2020 AS
SELECT 
    r.user_token
    , r.report_date
    , r.business_name
    , r.business_type
    , r.best_available_merchant_token
    , r.country_code
    , r.local_gpv
    , r.usd_gpv
    , r.tot_adjusted_revenue_local_processing
    , r.tot_cogs_estimated_local_processing
    , r.tot_adjusted_revenue_local_saas
    , r.tot_cogs_estimated_local_saas
    , r.tot_adjusted_revenue_local_hw
    , r.tot_cogs_estimated_local_hw
    , r.tot_adjusted_revenue_local_capital
    , r.tot_cogs_estimated_local_capital
    , r.tot_adjusted_revenue_local_sc
    , r.tot_cogs_estimated_local_sc
    , COALESCE(cb.realized_loss, 0) AS realized_loss
    , COALESCE(cb.realized_credit_loss, 0) AS realized_credit_loss
    , COALESCE(cb.realized_fraud_loss, 0) AS realized_fraud_loss
    , COALESCE(cb.realized_other_loss, 0) AS realized_other_loss
    , tot_adjusted_revenue_local_processing
        -tot_cogs_estimated_local_processing
        +tot_adjusted_revenue_local_saas
        -tot_cogs_estimated_local_saas
        +tot_adjusted_revenue_local_hw
        -tot_cogs_estimated_local_hw
        +tot_adjusted_revenue_local_capital
        -tot_cogs_estimated_local_capital
        +tot_adjusted_revenue_local_sc
        -tot_cogs_estimated_local_sc
        -COALESCE(cb.realized_loss, 0) AS profit
    , r.adjusted_revenue_local_processing
    , r.cogs_estimated_local_processing
    , r.adjusted_revenue_local_saas
    , r.cogs_estimated_local_saas
    , r.adjusted_revenue_local_hw
    , r.cogs_estimated_local_hw
    , r.adjusted_revenue_local_capital
    , r.cogs_estimated_local_capital
    , r.adjusted_revenue_local_sc
    , r.cogs_estimated_local_sc
    , r.mer_adjusted_revenue_local_saas
    , r.mer_cogs_estimated_local_saas
    , r.mer_adjusted_revenue_local_hw
    , r.mer_cogs_estimated_local_hw
    , r.mer_adjusted_revenue_local_capital
    , r.mer_cogs_estimated_local_capital
    , r.unit_cnt
    , CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS etl_created_at
FROM app_risk.app_risk_test.seller_profit_daily_calculation_gpv_rev_clean_2019_2020 r
LEFT JOIN app_risk.app_risk_test.seller_profit_daily_calculation_loss_2019_2020 cb 
    ON r.user_token = cb.user_token
    AND r.report_date = cb.report_date
;

CREATE TABLE IF NOT EXISTS app_risk.app_risk.seller_profit_daily_2019_2020 LIKE app_risk.app_risk_test.seller_profit_daily_calculation_2019_2020;
ALTER TABLE app_risk.app_risk.seller_profit_daily_2019_2020 SWAP WITH app_risk.app_risk_test.seller_profit_daily_calculation_2019_2020;

DROP TABLE IF EXISTS app_risk.app_risk_test.seller_profit_daily_calculation_gpv CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk_test.seller_profit_daily_calculation_rev CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk_test.seller_profit_daily_calculation_rev_mer CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk_test.seller_profit_daily_calculation_gpv_rev CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk_test.seller_profit_daily_calculation_gpv_rev_clean CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk_test.seller_profit_daily_calculation_loss CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk_test.seller_profit_daily_calculation CASCADE;


select report_date, count(distinct user_token), count(*)
from app_risk.app_risk.seller_profit_daily_2019_2020
group by 1
order by 1
;
