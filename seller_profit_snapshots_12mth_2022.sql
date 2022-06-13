DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv01_loading_2022 CASCADE;	
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv02_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv03_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv04_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv04_pd1_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv04_pd2_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv04_pd3_gpv_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv04_pd4_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv05_pd_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv05_lgd_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv05_presale_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv06_loss_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv07_loading_2022 CASCADE;
	
	--create app_risk.app_risk.historical cohort table
	CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_cohort_2022
	(
	cohort_date DATE
	);
	
	SELECT * FROM app_risk.app_risk.hist_seller_profit_calculation_cohort_2022;
	
	INSERT INTO app_risk.app_risk.hist_seller_profit_calculation_cohort_2022 (cohort_date)
	VALUES
	('2022-01-03'),
    ('2022-01-10'),
    ('2022-01-17'),
    ('2022-01-24'),
    ('2022-01-31'),
    ('2022-02-07'),
    ('2022-02-14'),
    ('2022-02-21'),
    ('2022-02-28'),
    ('2022-03-07'),
    ('2022-03-14'),
    ('2022-03-21'),
    ('2022-03-28'),
    ('2022-04-04'),
    ('2022-04-11'),
    ('2022-04-18'),
    ('2022-04-25'),
    ('2022-05-02'),
    ('2022-05-09'),
    ('2022-05-16'),
    ('2022-05-23'),
    ('2022-05-30'),
    ('2022-06-06')
	;
	SELECT * FROM app_risk.app_risk.hist_seller_profit_calculation_cohort_2022;
	
	--Sellers with 12 months GPV
	-----------01.Trailing 12 months GPV and Merchant Information--------------------
	CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv01_loading_2022 AS
	SELECT
	gpv.user_token
	,gpv.cohort_date
	,gpv.currency_code
	,CASE WHEN gpv.currency_code = 'JPY' THEN gpv.trailing_365d_local_gpv*100 ELSE gpv.trailing_365d_local_gpv END AS trailing_365d_local_gpv
	,CASE WHEN gpv.currency_code = 'JPY' THEN gpv.trailing_365d_usd_gpv*100 ELSE gpv.trailing_365d_usd_gpv END AS trailing_365d_usd_gpv
	,du.business_name
	,du.business_type
	,du.business_category
	,CASE WHEN du.business_type IN
	('travel_agencies_and_tour_operators'
	, 'travel_tourism'
	, 'tourism'
	, 'hotels_and_lodging'
	, 'rv_parks_and_campgrounds'
	, 'amusement_parks'
	, 'aquariums'
	, 'cultural_attractions'
	, 'recreation_services'
	, 'sporting_events'
	, 'ticket_sales'
	, 'sports_facilities'
	, 'music_and_entertainment'
	, 'theatrical_arts'
	, 'movies_film'
	) THEN '3.travel_events_related'
	WHEN du.business_category ='home_and_repair' THEN '1.home_and_repair'
	WHEN du.business_category ='professional_services' THEN '2.professional_services'
	WHEN du.business_category = 'retail' THEN '4.Retail'
	WHEN du.business_category = 'health_care_and_fitness' THEN '5.health_care_and_fitness'
	WHEN du.business_category = 'charities_education_and_membership' THEN '6.charities_education_and_membership'
	WHEN du.business_category = 'beauty_and_personal_care' THEN '7.beauty_and_personal_care'
	WHEN du.business_category = 'casual_use' THEN '8.casual_use'
	WHEN du.business_category = 'food_and_drink' THEN '9.food_and_drink'
	WHEN du.business_category = 'transportation' THEN '10.transportation'
	ELSE '11.Other' END AS mcc_group
	,du.best_available_merchant_token
	,du.country_code
	FROM (SELECT
	vf.user_token
	,ct.cohort_date
	,vf.currency_code
	,IFNULL(SUM(CASE WHEN vf.payment_trx_recognized_date > DATEADD(DAY,-365,ct.cohort_date) AND vf.payment_trx_recognized_date <= ct.cohort_date THEN vf.gpv_payment_amount_base_unit/100 END),0) AS trailing_365d_local_gpv
	,IFNULL(SUM(CASE WHEN vf.payment_trx_recognized_date > DATEADD(DAY,-365,ct.cohort_date) AND vf.payment_trx_recognized_date <= ct.cohort_date THEN vf.gpv_payment_amount_base_unit_usd/100 END),0) AS trailing_365d_usd_gpv
	FROM app_risk.app_risk.hist_seller_profit_calculation_cohort_2022 ct
	INNER JOIN app_bi.pentagon.aggregate_seller_daily_payment_summary AS vf ON payment_trx_recognized_date >= DATEADD(DAY,-365,ct.cohort_date)
	WHERE payment_trx_recognized_date >= DATEADD(DAY,-365,ct.cohort_date)
	GROUP BY vf.user_token,ct.cohort_date, vf.currency_code
	HAVING trailing_365d_local_gpv > 0) gpv
	LEFT OUTER JOIN app_bi.pentagon.dim_user du ON gpv.user_token = du.user_token and du.is_unit = 1
	;
	
	-----------02.Trailing 12 months Revenue and get their token type: merchant token vs unit token--------------------
	CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv02_loading_2022 AS
	SELECT r.user_token
	,ct.cohort_date
	,r.product_category
	,du.is_unit
	,du.is_merchant
	,IFNULL(SUM(CASE WHEN r.report_date >= DATEADD(DAY,-365,ct.cohort_date) AND r.report_date <= ct.cohort_date THEN r.adjusted_revenue_base_unit/100 END),0) AS adjusted_revenue_local
	,IFNULL(SUM(CASE WHEN r.report_date >= DATEADD(DAY,-365,ct.cohort_date) AND r.report_date <= ct.cohort_date THEN r.cogs_estimated_base_unit/100 END),0) AS cogs_estimated_local
	,IFNULL(SUM(CASE WHEN r.report_date >= DATEADD(DAY,-365,ct.cohort_date) AND r.report_date <= ct.cohort_date THEN r.costplus_adjustment_base_unit/100 END),0) AS costplus_adjustment_local
	,IFNULL(SUM(CASE WHEN r.report_date >= DATEADD(DAY,-365,ct.cohort_date) AND r.report_date <= ct.cohort_date THEN r.costplus_estimated_base_unit/100 END),0) AS costplus_estimated_local
	FROM app_risk.app_risk.hist_seller_profit_calculation_cohort_2022 ct
	INNER JOIN App_bi.app_bi_dw.vfact_revenue_summary r ON r.report_date>= DATEADD(DAY,-365,ct.cohort_date)
	LEFT OUTER JOIN app_bi.pentagon.dim_user du ON r.user_token = du.user_token
	GROUP BY 1,2,3,4,5
	;
	-----------03.Merge first two tables and get each item as variable in both unit token and merchant token vars--------------------
	CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv03_loading_2022 AS
	SELECT gpv.*
	,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(r.adjusted_revenue_local_processing,0) *100 ELSE COALESCE(r.adjusted_revenue_local_processing,0) END AS adjusted_revenue_local_processing
	,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(r.cogs_estimated_local_processing,0) *100 ELSE COALESCE(r.cogs_estimated_local_processing,0) END AS cogs_estimated_local_processing
	,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(r.adjusted_revenue_local_saas,0) *100 ELSE COALESCE(r.adjusted_revenue_local_saas,0) END AS adjusted_revenue_local_saas
	,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(r.cogs_estimated_local_saas,0) *100 ELSE COALESCE(r.cogs_estimated_local_saas,0) END AS cogs_estimated_local_saas
	,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(r.adjusted_revenue_local_hw,0) *100 ELSE COALESCE(r.adjusted_revenue_local_hw,0) END AS adjusted_revenue_local_hw
	,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(r.cogs_estimated_local_hw,0) *100 ELSE COALESCE(r.cogs_estimated_local_hw,0) END AS cogs_estimated_local_hw
	,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(r.adjusted_revenue_local_capital,0) *100 ELSE COALESCE(r.adjusted_revenue_local_capital,0) END AS adjusted_revenue_local_capital
	,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(r.cogs_estimated_local_capital,0) *100 ELSE COALESCE(r.cogs_estimated_local_capital,0) END AS cogs_estimated_local_capital
	,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(r.adjusted_revenue_local_sc,0) *100 ELSE COALESCE(r.adjusted_revenue_local_sc,0) END AS adjusted_revenue_local_sc
	,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(r.cogs_estimated_local_sc,0) *100 ELSE COALESCE(r.cogs_estimated_local_sc,0) END AS cogs_estimated_local_sc
	,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(m.mer_adjusted_revenue_local_saas,0) *100 ELSE COALESCE(m.mer_adjusted_revenue_local_saas,0) END AS mer_adjusted_revenue_local_saas
	,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(m.mer_cogs_estimated_local_saas,0) *100 ELSE COALESCE(m.mer_cogs_estimated_local_saas,0) END AS mer_cogs_estimated_local_saas
	,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(m.mer_adjusted_revenue_local_hw,0) *100 ELSE COALESCE(m.mer_adjusted_revenue_local_hw,0) END AS mer_adjusted_revenue_local_hw
	,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(m.mer_cogs_estimated_local_hw,0) *100 ELSE COALESCE(m.mer_cogs_estimated_local_hw,0) END AS mer_cogs_estimated_local_hw
	,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(m.mer_adjusted_revenue_local_capital,0)*100 ELSE COALESCE(m.mer_adjusted_revenue_local_capital,0) END AS mer_adjusted_revenue_local_capital
	,CASE WHEN gpv.currency_code = 'JPY' THEN COALESCE(m.mer_cogs_estimated_local_capital,0) *100 ELSE COALESCE(m.mer_cogs_estimated_local_capital,0) END AS mer_cogs_estimated_local_capital
	,mer.unit_cnt
	FROM app_risk.app_risk.hist_seller_profit_calculation_drv01_loading_2022 gpv
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
	FROM app_risk.app_risk.hist_seller_profit_calculation_drv02_loading_2022
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
	FROM app_risk.app_risk.hist_seller_profit_calculation_drv02_loading_2022
	WHERE is_merchant = 1
	GROUP BY 1,2
	) m ON gpv.best_available_merchant_token = m.user_token and gpv.cohort_date = m.cohort_date
	LEFT OUTER JOIN
	(SELECT
	cohort_date
	,best_available_merchant_token
	,COUNT(*) AS unit_cnt
	FROM app_risk.app_risk.hist_seller_profit_calculation_drv01_loading_2022
	GROUP BY 1,2
	) mer ON gpv.best_available_merchant_token = mer.best_available_merchant_token and gpv.cohort_date = mer.cohort_date
	;
	
	-----------04.Sum the revenue and cost for the product with both merchant token level and unit token level--------------------
	------------Use merchant level total / # of active unit counts to allocate to unit token level---------------
	CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv04_loading_2022 AS
	SELECT
	user_token
	,cohort_date
	,currency_code
	,trailing_365d_local_gpv
	,trailing_365d_usd_gpv
	,business_name
	,business_type
	,business_category
	,mcc_group
	,best_available_merchant_token
	,country_code
	,adjusted_revenue_local_processing AS tot_adjusted_revenue_local_processing
	,cogs_estimated_local_processing AS tot_cogs_estimated_local_processing
	,adjusted_revenue_local_saas + mer_adjusted_revenue_local_saas/unit_cnt AS tot_adjusted_revenue_local_saas
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
	FROM app_risk.app_risk.hist_seller_profit_calculation_drv03_loading_2022
	;
	
	-----------05.Pull the PD calibration in the past 12 months--------------------
	CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv04_pd1_loading_2022 AS
	SELECT unit_token, ct.cohort_date, max(payment_trx_recognized_date) AS latest_trx_date
	FROM app_risk.app_risk.hist_seller_profit_calculation_cohort_2022 ct
	INNER JOIN app_bi.pentagon.fact_payment_transactions AS vf ON payment_trx_recognized_date >= DATEADD(DAY,-365,ct.cohort_date)
	WHERE payment_trx_recognized_date>=DATEADD(day, -365, ct.cohort_date) AND payment_trx_recognized_date<=ct.cohort_date AND vf.is_gpv = 1
	GROUP BY 1,2
	;
	CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv04_pd2_loading_2022 AS
	SELECT fpt.unit_token
	,u.cohort_date
	,avg(rs.score) AS pd_score
	FROM app_risk.app_risk.hist_seller_profit_calculation_drv04_pd1_loading_2022 u
	JOIN app_bi.pentagon.fact_payment_transactions fpt ON fpt.unit_token = u.unit_token AND fpt.payment_trx_recognized_date >= DATEADD(day, -15, latest_trx_date)
	AND fpt.payment_trx_recognized_date <= latest_trx_date
	JOIN app_risk.app_risk.riskarbiter_scored_event rs ON rs.eventkey = fpt.payment_token
	WHERE fpt.payment_trx_recognized_date >= DATEADD(day, -15, latest_trx_date)
	AND fpt.payment_trx_recognized_date <= latest_trx_date
	AND modelname IN ('ml__default_v2_dumbo_novalue_noinstant_score_all_20200904')
	AND fpt.is_gpv = 1
	GROUP BY 1,2
	;
	CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv04_pd3_gpv_loading_2022 AS
	SELECT
	vf.user_token
	,ct.cohort_date
	,IFNULL(SUM(CASE WHEN vf.payment_trx_recognized_date > DATEADD(DAY,-90,ct.cohort_date) AND vf.payment_trx_recognized_date <= ct.cohort_date THEN vf.gpv_payment_amount_base_unit_usd/100 END),0) AS trailing_90d_gpv
	,IFNULL(SUM(CASE WHEN vf.payment_trx_recognized_date > DATEADD(DAY,-365,ct.cohort_date) AND vf.payment_trx_recognized_date <= ct.cohort_date THEN vf.gpv_payment_amount_base_unit_usd/100 END),0) AS trailing_365d_gpv
	,GREATEST(trailing_365d_gpv, COALESCE(trailing_90d_gpv * 4,0)) AS gpv_annualized_estimate
	FROM app_risk.app_risk.hist_seller_profit_calculation_cohort_2022 ct
	INNER JOIN app_bi.pentagon.aggregate_seller_daily_payment_summary AS vf
	WHERE payment_trx_recognized_date >= DATEADD(DAY,-365,ct.cohort_date)
	GROUP BY vf.user_token,ct.cohort_date
	;
	
	CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv04_pd4_sqrr_loading_2022 AS
	SELECT
	distinct gpv.USER_TOKEN
	,gpv.cohort_date
	,CASE
	WHEN credit_risk_rating = 'MINIMAL' THEN 1
	WHEN credit_risk_rating = 'LOW' THEN 2
	WHEN credit_risk_rating = 'MEDIUM' THEN 3
	WHEN credit_risk_rating = 'HIGH' THEN 4
	WHEN credit_risk_rating = 'CRITICAL' THEN 5
	ELSE NULL END AS SQRR
	,CASE
	WHEN credit_risk_rating = 'MINIMAL' THEN 0.002
	WHEN credit_risk_rating = 'LOW' THEN 0.012
	WHEN credit_risk_rating = 'MEDIUM' THEN 0.025
	WHEN credit_risk_rating = 'HIGH' THEN 0.040
	WHEN credit_risk_rating = 'CRITICAL' THEN 0.070
	ELSE NULL END AS pd_sqrr
	FROM app_risk.app_risk.hist_seller_profit_calculation_drv01_loading_2022 gpv
	left join regulator.raw_oltp.credit_risk_review_case_infos r
	on gpv.user_token = r.user_token and gpv.cohort_date = substring(r.created_at,1,10)
	QUALIFY row_number() over (partition by r.user_token,substring(r.created_at,1,10) order by r.created_at desc) = 1
	;
	
	CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv05_pd_loading_2022 AS
	SELECT drv.unit_token
	,drv.cohort_date
	,drv.pd_score
	,gpv.gpv_annualized_estimate
	,CASE WHEN gpv.gpv_annualized_estimate is null or gpv.gpv_annualized_estimate < 100000 THEN '01.<100K'
	WHEN gpv.gpv_annualized_estimate < 500000 THEN '02.100K-500K'
	WHEN gpv.gpv_annualized_estimate < 1000000 THEN '03.500K-1MM'
	ELSE '04.1MM+' END AS gpv_band
	/*,CASE WHEN gpv.gpv_annualized_estimate is null or gpv.gpv_annualized_estimate < 100000 THEN 0.01499172 * drv.pd_score + 0.02299767 * square(drv.pd_score) + 0.00011414723667305354
	WHEN gpv.gpv_annualized_estimate < 500000 THEN 0.05548777 * drv.pd_score + 0.00303788 * square(drv.pd_score) + 0.000474303699369024
	ELSE 0.04734929 * drv.pd_score + 0.19263468 * square(drv.pd_score) + 0.0012467489967264306 END AS probability_default*/
	,CASE WHEN mcc_group = '1.home_and_repair' THEN 0.45* pd_score
	WHEN mcc_group = '10.transportation' THEN 0.55* pd_score
	WHEN mcc_group = '2.professional_services' THEN 0.40* pd_score
	WHEN mcc_group = '3.travel_events_related' THEN 0.40* pd_score
	WHEN mcc_group = '4.Retail' THEN 0.70* pd_score
	WHEN mcc_group = '5.health_care_and_fitness' THEN 0.40* pd_score
	WHEN mcc_group = '6.charities_education_and_membership' THEN 0.45* pd_score
	WHEN mcc_group = '7.beauty_and_personal_care' THEN 0.75* pd_score
	WHEN mcc_group = '8.casual_use' THEN 0.70* pd_score
	WHEN mcc_group = '9.food_and_drink' THEN 0.55* pd_score
	ELSE pd_score END as pd_financial_wo_cap
	,CASE WHEN sqrr.user_token is not null THEN pd_sqrr
	WHEN pd_financial_wo_cap > 0.07 THEN 0.07
	ELSE pd_financial_wo_cap END AS probability_default
	FROM app_risk.app_risk.hist_seller_profit_calculation_drv04_pd2_loading_2022 drv
	INNER JOIN app_risk.app_risk.hist_seller_profit_calculation_drv04_pd3_gpv_loading_2022 gpv ON drv.unit_token = gpv.user_token and drv.cohort_date = gpv.cohort_date
	INNER JOIN app_risk.app_risk.hist_seller_profit_calculation_drv04_loading_2022 bu ON drv.unit_token = bu.user_token and drv.cohort_date = bu.cohort_date
	LEFT OUTER JOIN app_risk.app_risk.hist_seller_profit_calculation_drv04_pd4_sqrr_loading_2022 sqrr ON drv.unit_token = sqrr.user_token and drv.cohort_date = sqrr.cohort_date
	;
	/*FROM app_risk.app_risk.hist_seller_profit_calculation_drv04_pd2_loading_2022 drv
	INNER JOIN app_risk.app_risk.hist_seller_profit_calculation_drv04_pd3_gpv_loading_2022 gpv ON drv.unit_token = gpv.user_token and drv.cohort_date = gpv.cohort_date
	;*/
	
	SELECT COUNT(*), COUNT(distinct unit_token, cohort_date) FROM app_risk.app_risk.hist_seller_profit_calculation_drv05_pd_loading_2022;
	
	-----------06.Pull the LGD--------------------
	CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv05_lgd_loading_2022 AS
	SELECT fpt.unit_token
	,u.cohort_date
	,avg(rs.score) AS lgd_loss
	FROM app_risk.app_risk.hist_seller_profit_calculation_drv04_pd1_loading_2022 u
	JOIN app_bi.pentagon.fact_payment_transactions fpt ON fpt.unit_token = u.unit_token AND fpt.payment_trx_recognized_date >= DATEADD(day, -15, latest_trx_date)
	AND fpt.payment_trx_recognized_date <= latest_trx_date
	JOIN app_risk.app_risk.riskarbiter_scored_event rs ON rs.eventkey = fpt.payment_token
	WHERE fpt.payment_trx_recognized_date >= DATEADD(day, -15, latest_trx_date)
	AND fpt.payment_trx_recognized_date <= latest_trx_date
	AND modelname IN ('ml__credit_risk__LGD_loss_score_all_20210218')
	AND fpt.is_gpv = 1
	GROUP BY 1,2
	;
	
	CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv05_presale_loading_2022 AS
	SELECT drv.user_token, drv.cohort_date
	,NVL(SUM(CASE WHEN payment_trx_recognized_date >= DATEADD(DAY, -cnp_presale_days, drv.cohort_date) AND payment_trx_recognized_date <= drv.cohort_date THEN cnp_card_payment_amount_base_unit END), 0) AS cnp_gpv_base_unit
	,NVL(SUM(CASE WHEN payment_trx_recognized_date >= DATEADD(DAY, -cp_presale_days, drv.cohort_date) AND payment_trx_recognized_date <= drv.cohort_date THEN cp_card_payment_amount_base_unit END),0) AS cp_gpv_base_unit
	,cnp_gpv_base_unit+cp_gpv_base_unit as pre_sale_exposure_base_unit
	FROM app_risk.app_risk.hist_seller_profit_calculation_drv04_loading_2022 drv
	LEFT OUTER JOIN fivetran.app_risk.policy_mcc_presale fv on drv.business_type = fv.business_type
	LEFT OUTER JOIN app_bi.pentagon.aggregate_seller_daily_payment_summary dps on drv.user_token = dps.unit_token
	GROUP BY 1,2
	;
	
	----------07.Pull realized loss ---------------
	CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv06_loss_loading_2022 AS
	select
	cb.user_token,
	ct.cohort_date,
	sum(realized_loss) as realized_loss,
	sum(realized_credit_loss) as realized_credit_loss
	from app_risk.app_risk.hist_seller_profit_calculation_cohort_2022 ct
	INNER JOIN (SELECT cb.user_token, substring(payment_created_at,1,10) as payment_date
	,SUM(CASE WHEN currency_code = 'JPY' THEN loss_cents ELSE loss_cents/100 END) AS realized_loss
	,SUM(CASE WHEN currency_code = 'JPY' and type = 'credit' THEN loss_cents
	WHEN type = 'credit' THEN loss_cents/100
	ELSE 0 END) AS realized_credit_loss
	FROM app_risk.app_risk.chargebacks cb
	GROUP BY 1,2) cb
	on cb.payment_date >= DATEADD(DAY,-365,ct.cohort_date) and cb.payment_date <= ct.cohort_date
	WHERE cb.payment_date >= DATEADD(DAY,-365,ct.cohort_date) and cb.payment_date <= ct.cohort_date
	group by 1,2
	;
	
	----------08.merge revenue and lgd loss----------------
	CREATE OR REPLACE TABLE app_risk.app_risk.hist_seller_profit_calculation_drv07_loading_2022 AS
	SELECT
	r.user_token
	,r.cohort_date
	,r.currency_code
	,r.trailing_365d_local_gpv
	,r.trailing_365d_usd_gpv
	,r.business_name
	,r.business_type
	,r.business_category
	,r.mcc_group
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
	,cb.realized_loss
	,cb.realized_credit_loss
	,CASE WHEN currency_code = 'JPY' THEN lgd.lgd_loss ELSE lgd.lgd_loss/100 END AS lgd_loss_dollar
	,CASE WHEN mcc_group = '1.home_and_repair' THEN 0.52
	WHEN mcc_group = '10.transportation' THEN 1.0
	WHEN mcc_group = '2.professional_services' THEN 0.43
	WHEN mcc_group = '3.travel_events_related' THEN 0.44
	WHEN mcc_group = '4.Retail' THEN 0.73
	WHEN mcc_group = '5.health_care_and_fitness' THEN 0.62
	WHEN mcc_group = '6.charities_education_and_membership' THEN 0.18
	WHEN mcc_group = '7.beauty_and_personal_care' THEN 0.99
	WHEN mcc_group = '8.casual_use' THEN 0.83
	WHEN mcc_group = '9.food_and_drink' THEN 0.65
	ELSE 0.50 END as lgd_percentage
	,CASE WHEN currency_code = 'JPY' THEN presale.pre_sale_exposure_base_unit ELSE presale.pre_sale_exposure_base_unit/100 END AS presale_exposure_dollar
	,COALESCE(pd.probability_default*presale_exposure_dollar*lgd_percentage,0) AS lgd_mpy_pd
	,greatest(COALESCE(pd.probability_default*presale_exposure_dollar*lgd_percentage,0), COALESCE(cb.realized_loss,0)) AS estimated_loss
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
	FROM app_risk.app_risk.hist_seller_profit_calculation_drv04_loading_2022 r
	LEFT OUTER JOIN app_risk.app_risk.hist_seller_profit_calculation_drv05_pd_loading_2022 pd ON r.user_token = pd.unit_token and r.cohort_date = pd.cohort_date
	LEFT OUTER JOIN app_risk.app_risk.hist_seller_profit_calculation_drv05_lgd_loading_2022 lgd ON r.user_token = lgd.unit_token and r.cohort_date = lgd.cohort_date
	LEFT OUTER JOIN app_risk.app_risk.hist_seller_profit_calculation_drv05_presale_loading_2022 presale ON r.user_token = presale.user_token and r.cohort_date = presale.cohort_date
	LEFT OUTER JOIN app_risk.app_risk.hist_seller_profit_calculation_drv04_pd3_gpv_loading_2022 gpv ON r.user_token = gpv.user_token and r.cohort_date = gpv.cohort_date
	LEFT OUTER JOIN app_risk.app_risk.hist_seller_profit_calculation_drv06_loss_loading_2022 cb ON r.user_token = cb.user_token and r.cohort_date = cb.cohort_date
	;
	
	
	CREATE TABLE IF NOT EXISTS app_risk.app_risk.seller_profit_snapshots_12mth_2022 lIKE app_risk.app_risk.hist_seller_profit_calculation_drv07_loading_2022;
	ALTER TABLE app_risk.app_risk.seller_profit_snapshots_12mth_2022 SWAP WITH app_risk.app_risk.hist_seller_profit_calculation_drv07_loading_2022;
	
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv01_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv02_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv03_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv04_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv04_pd1_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv04_pd2_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv04_pd3_gpv_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv04_pd4_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv05_pd_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv05_lgd_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv05_presale_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv06_loss_loading_2022 CASCADE;
	DROP TABLE IF EXISTS app_risk.app_risk.hist_seller_profit_calculation_drv07_loading_2022 CASCADE;
	
    
    select *
    from app_risk.app_risk.seller_profit_snapshots_12mth_2022
    limit 5
    ;
	
