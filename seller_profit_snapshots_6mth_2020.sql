DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv01_loading_2020 CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv02_loading_2020 CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv03_loading_2020 CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv04_loading_2020 CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv04_pd1_loading_2020 CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv04_pd2_loading_2020 CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv04_pd3_gpv_loading_2020 CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv05_pd_loading_2020 CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv05_lgd_loading_2020 CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv06_loading_2020 CASCADE;

--create app_risk.app_risk.historical cohort table
CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_cohort_2020
(
cohort_date DATE
);

SELECT * FROM app_risk.app_risk.hist_seller_profit_calculation_cohort_2020;

INSERT INTO app_risk.app_risk.hist_seller_profit_calculation_cohort_2020 (cohort_date)
VALUES
('2020-01-06'),
('2020-01-13'),
('2020-01-20'),
('2020-01-27'),
('2020-02-03'),
('2020-02-10'),
('2020-02-17'),
('2020-02-24'),
('2020-03-02'),
('2020-03-09'),
('2020-03-16'),
('2020-03-23'),
('2020-03-30'),
('2020-04-06'),
('2020-04-13'),
('2020-04-20'),
('2020-04-27'),
('2020-05-04'),
('2020-05-11'),
('2020-05-18'),
('2020-05-25'),
('2020-06-01'),
('2020-06-08'),
('2020-06-15'),
('2020-06-22'),
('2020-06-29'),
('2020-07-06'),
('2020-07-13'),
('2020-07-20'),
('2020-07-27'),
('2020-08-03'),
('2020-08-10'),
('2020-08-17'),
('2020-08-24'),
('2020-08-31'),
('2020-09-07'),
('2020-09-14'),
('2020-09-21'),
('2020-09-28'),
('2020-10-05'),
('2020-10-12'),
('2020-10-19'),
('2020-10-26'),
('2020-11-02'),
('2020-11-09'),
('2020-11-16'),
('2020-11-23'),
('2020-11-30'),
('2020-12-07'),
('2020-12-14'),
('2020-12-21'),
('2020-12-28')
;
SELECT * FROM app_risk.app_risk.hist_seller_profit_calculation_cohort_2020;

SELECT COUNT(*), COUNT(distinct user_token, cohort_date) FROM app_risk.app_risk.hist_seller_profit_calculation_drv01_loading_2020;
SELECT COUNT(*), COUNT(distinct user_token, cohort_date) FROM seller_profit_snapshots_2020;
COUNT(*)
217,062,824
;

SELECT * FROM app_risk.app_risk.seller_profit_snapshots WHERE user_token = 'HT8TBW4ZCAP7R';
SELECT * FROM app_risk.app_risk.seller_profit WHERE user_token = 'HT8TBW4ZCAP7R';

--Sellers with 6 months GPV
-----------01.Trailing 6 months GPV and Merchant Information--------------------
CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv01_loading_2020 AS
SELECT
gpv.user_token
,gpv.cohort_date
,gpv.currency_code
,CASE WHEN gpv.currency_code = 'JPY' THEN gpv.trailing_180d_local_gpv*100 ELSE gpv.trailing_180d_local_gpv END AS trailing_180d_local_gpv
,CASE WHEN gpv.currency_code = 'JPY' THEN gpv.trailing_180d_usd_gpv*100 ELSE gpv.trailing_180d_usd_gpv END AS trailing_180d_usd_gpv
,du.business_name
,du.business_type
,du.best_available_merchant_token
,du.country_code
FROM (SELECT
vf.user_token
,ct.cohort_date
,vf.currency_code
,IFNULL(SUM(CASE WHEN vf.payment_trx_recognized_date > DATEADD(DAY,-180,ct.cohort_date) AND vf.payment_trx_recognized_date <= ct.cohort_date THEN vf.gpv_payment_amount_base_unit/100 END),0) AS trailing_180d_local_gpv
,IFNULL(SUM(CASE WHEN vf.payment_trx_recognized_date > DATEADD(DAY,-180,ct.cohort_date) AND vf.payment_trx_recognized_date <= ct.cohort_date THEN vf.gpv_payment_amount_base_unit_usd/100 END),0) AS trailing_180d_usd_gpv
FROM app_risk.app_risk.hist_seller_profit_calculation_cohort_2020 ct
INNER JOIN app_bi.pentagon.aggregate_seller_daily_payment_summary AS vf ON payment_trx_recognized_date >= DATEADD(DAY,-180,ct.cohort_date)
WHERE payment_trx_recognized_date >= DATEADD(DAY,-180,ct.cohort_date)
GROUP BY vf.user_token,ct.cohort_date, vf.currency_code
HAVING trailing_180d_local_gpv > 0) gpv
LEFT OUTER JOIN app_bi.pentagon.dim_user du ON gpv.user_token = du.user_token and du.is_unit = 1
;

-----------02.Trailing 6 months Revenue and get their token type: merchant token vs unit token--------------------
CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv02_loading_2020 AS
SELECT r.user_token
,ct.cohort_date
,r.product_category
,du.is_unit
,du.is_merchant
,IFNULL(SUM(CASE WHEN r.report_date >= DATEADD(DAY,-180,ct.cohort_date) AND r.report_date <= ct.cohort_date THEN r.adjusted_revenue_base_unit/100 END),0) AS adjusted_revenue_local
,IFNULL(SUM(CASE WHEN r.report_date >= DATEADD(DAY,-180,ct.cohort_date) AND r.report_date <= ct.cohort_date THEN r.cogs_estimated_base_unit/100 END),0) AS cogs_estimated_local
,IFNULL(SUM(CASE WHEN r.report_date >= DATEADD(DAY,-180,ct.cohort_date) AND r.report_date <= ct.cohort_date THEN r.costplus_adjustment_base_unit/100 END),0) AS costplus_adjustment_local
,IFNULL(SUM(CASE WHEN r.report_date >= DATEADD(DAY,-180,ct.cohort_date) AND r.report_date <= ct.cohort_date THEN r.costplus_estimated_base_unit/100 END),0) AS costplus_estimated_local
FROM app_risk.app_risk.hist_seller_profit_calculation_cohort_2020 ct
INNER JOIN App_bi.app_bi_dw.vfact_revenue_summary r ON r.report_date>= DATEADD(DAY,-180,ct.cohort_date)
LEFT OUTER JOIN app_bi.pentagon.dim_user du ON r.user_token = du.user_token
GROUP BY 1,2,3,4,5
;
-----------03.Merge first two tables and get each item as variable in both unit token and merchant token vars--------------------
CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv03_loading_2020 AS
SELECT gpv.*
,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(r.adjusted_revenue_local_processing,0) *100 ELSE COALESCE(r.adjusted_revenue_local_processing,0)  END AS adjusted_revenue_local_processing
,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(r.cogs_estimated_local_processing,0)   *100 ELSE COALESCE(r.cogs_estimated_local_processing,0)    END AS cogs_estimated_local_processing
,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(r.adjusted_revenue_local_saas,0)       *100 ELSE COALESCE(r.adjusted_revenue_local_saas,0)        END AS adjusted_revenue_local_saas
,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(r.cogs_estimated_local_saas,0)         *100 ELSE COALESCE(r.cogs_estimated_local_saas,0)          END AS cogs_estimated_local_saas
,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(r.adjusted_revenue_local_hw,0)         *100 ELSE COALESCE(r.adjusted_revenue_local_hw,0)          END AS adjusted_revenue_local_hw
,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(r.cogs_estimated_local_hw,0)           *100 ELSE COALESCE(r.cogs_estimated_local_hw,0)            END AS cogs_estimated_local_hw
,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(r.adjusted_revenue_local_capital,0)    *100 ELSE COALESCE(r.adjusted_revenue_local_capital,0)     END AS adjusted_revenue_local_capital
,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(r.cogs_estimated_local_capital,0)      *100 ELSE COALESCE(r.cogs_estimated_local_capital,0)       END AS cogs_estimated_local_capital
,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(r.adjusted_revenue_local_sc,0)         *100 ELSE COALESCE(r.adjusted_revenue_local_sc,0)          END AS adjusted_revenue_local_sc
,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(r.cogs_estimated_local_sc,0)           *100 ELSE COALESCE(r.cogs_estimated_local_sc,0)            END AS cogs_estimated_local_sc
,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(m.mer_adjusted_revenue_local_saas,0)   *100 ELSE COALESCE(m.mer_adjusted_revenue_local_saas,0)    END AS mer_adjusted_revenue_local_saas
,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(m.mer_cogs_estimated_local_saas,0)     *100 ELSE COALESCE(m.mer_cogs_estimated_local_saas,0)      END AS mer_cogs_estimated_local_saas
,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(m.mer_adjusted_revenue_local_hw,0)     *100 ELSE COALESCE(m.mer_adjusted_revenue_local_hw,0)      END AS mer_adjusted_revenue_local_hw
,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(m.mer_cogs_estimated_local_hw,0)       *100 ELSE COALESCE(m.mer_cogs_estimated_local_hw,0)        END AS mer_cogs_estimated_local_hw
,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(m.mer_adjusted_revenue_local_capital,0)*100 ELSE COALESCE(m.mer_adjusted_revenue_local_capital,0) END AS mer_adjusted_revenue_local_capital
,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(m.mer_cogs_estimated_local_capital,0)  *100 ELSE COALESCE(m.mer_cogs_estimated_local_capital,0)   END AS mer_cogs_estimated_local_capital
,mer.unit_cnt
FROM app_risk.app_risk.hist_seller_profit_calculation_drv01_loading_2020 gpv
LEFT OUTER JOIN
(SELECT user_token
,cohort_date
,SUM(CASE WHEN product_category = 'Processing' THEN adjusted_revenue_local - costplus_adjustment_local ELSE 0 END) AS adjusted_revenue_local_processing
,SUM(CASE WHEN product_category = 'Processing' THEN cogs_estimated_local ELSE 0 END) AS cogs_estimated_local_processing
,SUM(CASE WHEN product_category = 'SaaS' THEN adjusted_revenue_local ELSE 0 END) AS adjusted_revenue_local_saas
,SUM(CASE WHEN product_category = 'SaaS' THEN cogs_estimated_local ELSE 0 END) AS cogs_estimated_local_saas
,SUM(CASE WHEN product_category = 'Hardware' THEN adjusted_revenue_local ELSE 0 END) AS adjusted_revenue_local_hw
,SUM(CASE WHEN product_category = 'Hardware' THEN cogs_estimated_local ELSE 0 END) AS cogs_estimated_local_hw
,SUM(CASE WHEN product_category = 'Capital' THEN adjusted_revenue_local ELSE 0 END) AS adjusted_revenue_local_capital
,SUM(CASE WHEN product_category = 'Capital' THEN cogs_estimated_local ELSE 0 END) AS cogs_estimated_local_capital
,SUM(CASE WHEN product_category = 'Square Card' THEN adjusted_revenue_local ELSE 0 END) AS adjusted_revenue_local_sc
,SUM(CASE WHEN product_category = 'Square Card' THEN cogs_estimated_local ELSE 0 END) AS cogs_estimated_local_sc
FROM app_risk.app_risk.hist_seller_profit_calculation_drv02_loading_2020
WHERE is_unit = 1
GROUP BY 1,2
) r ON gpv.user_token = r.user_token and gpv.cohort_date = r.cohort_date
LEFT OUTER JOIN
(SELECT user_token
,cohort_date
,SUM(CASE WHEN product_category = 'SaaS' THEN adjusted_revenue_local ELSE 0 END) AS mer_adjusted_revenue_local_saas
,SUM(CASE WHEN product_category = 'SaaS' THEN cogs_estimated_local ELSE 0 END) AS mer_cogs_estimated_local_saas
,SUM(CASE WHEN product_category = 'Hardware' THEN adjusted_revenue_local ELSE 0 END) AS mer_adjusted_revenue_local_hw
,SUM(CASE WHEN product_category = 'Hardware' THEN cogs_estimated_local ELSE 0 END) AS mer_cogs_estimated_local_hw
,SUM(CASE WHEN product_category = 'Capital' THEN adjusted_revenue_local ELSE 0 END) AS mer_adjusted_revenue_local_capital
,SUM(CASE WHEN product_category = 'Capital' THEN cogs_estimated_local ELSE 0 END) AS mer_cogs_estimated_local_capital
FROM app_risk.app_risk.hist_seller_profit_calculation_drv02_loading_2020
WHERE is_merchant = 1
GROUP BY 1,2
) m ON gpv.best_available_merchant_token = m.user_token and gpv.cohort_date = m.cohort_date
LEFT OUTER JOIN
(SELECT
cohort_date
,best_available_merchant_token
,COUNT(*) AS unit_cnt
FROM app_risk.app_risk.hist_seller_profit_calculation_drv01_loading_2020
GROUP BY 1,2
) mer ON gpv.best_available_merchant_token = mer.best_available_merchant_token and gpv.cohort_date = mer.cohort_date
;

-----------04.Sum the revenue and cost for the product with both merchant token level and unit token level--------------------
------------Use merchant level total / # of active unit counts to allocate to unit token level---------------
CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv04_loading_2020 AS
SELECT
user_token
,cohort_date
,currency_code
,trailing_180d_local_gpv
,trailing_180d_usd_gpv
,business_name
,business_type
,best_available_merchant_token
,country_code
,adjusted_revenue_local_processing AS tot_adjusted_revenue_local_processing
,cogs_estimated_local_processing AS tot_cogs_estimated_local_processing
,adjusted_revenue_local_saas + mer_adjusted_revenue_local_saas/unit_cnt  AS tot_adjusted_revenue_local_saas
,cogs_estimated_local_saas + mer_cogs_estimated_local_saas/unit_cnt AS tot_cogs_estimated_local_saas
,adjusted_revenue_local_hw + mer_adjusted_revenue_local_hw/unit_cnt AS tot_adjusted_revenue_local_hw
,cogs_estimated_local_hw + mer_cogs_estimated_local_hw/unit_cnt AS tot_cogs_estimated_local_hw
,adjusted_revenue_local_capital + mer_adjusted_revenue_local_capital/unit_cnt AS tot_adjusted_revenue_local_capital
,cogs_estimated_local_capital + mer_cogs_estimated_local_capital/unit_cnt AS tot_cogs_estimated_local_capital
,adjusted_revenue_local_sc AS tot_adjusted_revenue_local_sc
,cogs_estimated_local_sc AS tot_cogs_estimated_local_sc
,adjusted_revenue_local_processing
,cogs_estimated_local_processing
,adjusted_revenue_local_saas
,cogs_estimated_local_saas
,adjusted_revenue_local_hw
,cogs_estimated_local_hw
,adjusted_revenue_local_capital
,cogs_estimated_local_capital
,adjusted_revenue_local_sc
,cogs_estimated_local_sc
,mer_adjusted_revenue_local_saas
,mer_cogs_estimated_local_saas
,mer_adjusted_revenue_local_hw
,mer_cogs_estimated_local_hw
,mer_adjusted_revenue_local_capital
,mer_cogs_estimated_local_capital
,unit_cnt
FROM app_risk.app_risk.hist_seller_profit_calculation_drv03_loading_2020
;

-----------05.Pull the PD calibration in the past 6 months--------------------
CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv04_pd1_loading_2020 AS
SELECT unit_token, ct.cohort_date, max(payment_trx_recognized_date) AS latest_trx_date
FROM app_risk.app_risk.hist_seller_profit_calculation_cohort_2020 ct
INNER JOIN app_bi.pentagon.fact_payment_transactions AS vf ON payment_trx_recognized_date >= DATEADD(DAY,-180,ct.cohort_date)
WHERE payment_trx_recognized_date>=DATEADD(day, -180, ct.cohort_date) AND payment_trx_recognized_date<=ct.cohort_date AND vf.is_gpv = 1
GROUP BY 1,2
;
CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv04_pd2_loading_2020 AS
SELECT fpt.unit_token
,u.cohort_date
,avg(rs.score) AS pd_score
FROM app_risk.app_risk.hist_seller_profit_calculation_drv04_pd1_loading_2020 u
JOIN app_bi.pentagon.fact_payment_transactions fpt ON fpt.unit_token = u.unit_token AND fpt.payment_trx_recognized_date >= DATEADD(day, -15, latest_trx_date)
AND fpt.payment_trx_recognized_date <= latest_trx_date
JOIN app_risk.app_risk.riskarbiter_scored_event rs ON rs.eventkey = fpt.payment_token
WHERE fpt.payment_trx_recognized_date >= DATEADD(day, -15, latest_trx_date)
AND fpt.payment_trx_recognized_date <= latest_trx_date
AND modelname IN ('ml__default_v2_dumbo_novalue_noinstant_score_all_20200904')
AND fpt.is_gpv = 1
GROUP BY 1,2
;
CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv04_pd3_gpv_loading_2020 AS
SELECT
vf.user_token
,ct.cohort_date
,IFNULL(SUM(CASE WHEN vf.payment_trx_recognized_date > DATEADD(DAY,-90,ct.cohort_date) AND vf.payment_trx_recognized_date <= ct.cohort_date THEN vf.gpv_payment_amount_base_unit_usd/100 END),0) AS trailing_90d_gpv
,IFNULL(SUM(CASE WHEN vf.payment_trx_recognized_date > DATEADD(DAY,-365,ct.cohort_date) AND vf.payment_trx_recognized_date <= ct.cohort_date THEN vf.gpv_payment_amount_base_unit_usd/100 END),0) AS trailing_365d_gpv
,GREATEST(trailing_365d_gpv, COALESCE(trailing_90d_gpv * 4,0)) AS gpv_annualized_estimate
FROM app_risk.app_risk.hist_seller_profit_calculation_cohort_2020 ct
INNER JOIN app_bi.pentagon.aggregate_seller_daily_payment_summary AS vf
WHERE payment_trx_recognized_date >= DATEADD(DAY,-365,ct.cohort_date)
GROUP BY vf.user_token,ct.cohort_date
;

CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv05_pd_loading_2020 AS
SELECT drv.unit_token
,drv.cohort_date
,drv.pd_score
,gpv.gpv_annualized_estimate
,CASE WHEN gpv.gpv_annualized_estimate is null or gpv.gpv_annualized_estimate < 100000 THEN '01.<100K'
WHEN gpv.gpv_annualized_estimate < 500000 THEN '02.100K-500K'
WHEN gpv.gpv_annualized_estimate < 1000000 THEN '03.500K-1MM'
ELSE '04.1MM+' END AS gpv_band
,CASE WHEN gpv.gpv_annualized_estimate is null or gpv.gpv_annualized_estimate < 100000 THEN 0.01499172 * drv.pd_score + 0.02299767 * square(drv.pd_score) + 0.00011414723667305354
WHEN gpv.gpv_annualized_estimate < 500000 THEN 0.05548777 * drv.pd_score + 0.00303788 * square(drv.pd_score) + 0.000474303699369024
ELSE 0.04734929 * drv.pd_score + 0.19263468 * square(drv.pd_score) + 0.0012467489967264306 END AS probability_default
FROM app_risk.app_risk.hist_seller_profit_calculation_drv04_pd2_loading_2020 drv
INNER JOIN app_risk.app_risk.hist_seller_profit_calculation_drv04_pd3_gpv_loading_2020 gpv ON drv.unit_token = gpv.user_token and drv.cohort_date = gpv.cohort_date
;
SELECT COUNT(*), COUNT(distinct unit_token, cohort_date) FROM app_risk.app_risk.hist_seller_profit_calculation_drv05_pd_loading_2020;
-----------06.Pull the LGD--------------------
CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv05_lgd_loading_2020 AS
SELECT fpt.unit_token
,u.cohort_date
,avg(rs.score) AS lgd_loss
FROM app_risk.app_risk.hist_seller_profit_calculation_drv04_pd1_loading_2020 u
JOIN app_bi.pentagon.fact_payment_transactions fpt ON fpt.unit_token = u.unit_token AND fpt.payment_trx_recognized_date >= DATEADD(day, -15, latest_trx_date)
AND fpt.payment_trx_recognized_date <= latest_trx_date
JOIN app_risk.app_risk.riskarbiter_scored_event rs ON rs.eventkey = fpt.payment_token
WHERE fpt.payment_trx_recognized_date >= DATEADD(day, -15, latest_trx_date)
AND fpt.payment_trx_recognized_date <= latest_trx_date
AND modelname IN ('ml__credit_risk__LGD_loss_score_all_20210218')
AND fpt.is_gpv = 1
GROUP BY 1,2
;
----------07.merge revenue and lgd loss----------------
CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv06_loading_2020 AS
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
,pd.probability_default
,lgd.lgd_loss/100 AS lgd_loss_dollar
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
-COALESCE(pd.probability_default*lgd_loss_dollar,0) AS profit
,pd.pd_score
,pd.gpv_annualized_estimate
,CASE WHEN gpv.gpv_annualized_estimate is null or gpv.gpv_annualized_estimate < 100000 THEN '01.<100K'
WHEN gpv.gpv_annualized_estimate < 500000 THEN '02.100K-500K'
WHEN gpv.gpv_annualized_estimate < 1000000 THEN '03.500K-1MM'
ELSE '04.1MM+' END AS gpv_band
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
FROM app_risk.app_risk.hist_seller_profit_calculation_drv04_loading_2020 r
LEFT OUTER JOIN app_risk.app_risk.hist_seller_profit_calculation_drv05_pd_loading_2020 pd ON r.user_token = pd.unit_token and r.cohort_date = pd.cohort_date
LEFT OUTER JOIN app_risk.app_risk.hist_seller_profit_calculation_drv05_lgd_loading_2020 lgd ON r.user_token = lgd.unit_token and r.cohort_date = lgd.cohort_date
LEFT OUTER JOIN app_risk.app_risk.hist_seller_profit_calculation_drv04_pd3_gpv_loading_2020 gpv ON r.user_token = gpv.user_token and r.cohort_date = gpv.cohort_date
;

CREATE TABLE IF NOT EXISTS app_risk.app_risk.seller_profit_snapshots_2020 lIKE app_risk.app_risk.hist_seller_profit_calculation_drv06_loading_2020;
ALTER TABLE app_risk.app_risk.seller_profit_snapshots_2020 SWAP WITH app_risk.app_risk.hist_seller_profit_calculation_drv06_loading_2020;

DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv01_loading_2020 CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv02_loading_2020 CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv03_loading_2020 CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv04_loading_2020 CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv04_pd1_loading_2020 CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv04_pd2_loading_2020 CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv04_pd3_gpv_loading_2020 CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv05_pd_loading_2020 CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv05_lgd_loading_2020 CASCADE;
DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv06_loading_2020 CASCADE;

CREATE TABLE app_risk.app_risk.seller_profit_snapshots_6mth as
select * from app_risk.app_risk.seller_profit_snapshots
union
select * from app_risk.app_risk.seller_profit_snapshots_2020
;
