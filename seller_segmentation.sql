-- create list of non-frozen non-deactivated sellers 
create or replace table accts as (
select 
    distinct du.user_token
    , ls.currency_code
    , du.business_category as mcc
    , du.business_type
    , datediff(month, du.user_created_at_date, current_date()) as onboard_tenure_mth
    , datediff(month, ls.first_card_payment_date, current_date()) as pmt_tenure_mth --payment tenure is indicated as time since first card transaction
from app_bi.pentagon.dim_user du
left join app_bi.pentagon.aggregate_seller_lifetime_summary ls
on du.user_token = ls.user_token
where du.is_currently_frozen != 1
and du.is_currently_deactivated != 1
);

-- get annual gpv for all sellers with payment processing
create or replace table gpv as (
select
    distinct user_token
    , sum (case when datediff(day, payment_trx_recognized_date, current_date()) <= 365 
           then (case when currency_code = 'JPY' THEN gpv_payment_amount_base_unit*100 ELSE gpv_payment_amount_base_unit end) else 0 end)/100 as ann_gpv_dllr
    , sum (case when datediff(day, payment_trx_recognized_date, current_date()) <= 90 
           then (case when currency_code = 'JPY' THEN gpv_payment_amount_base_unit*100 ELSE gpv_payment_amount_base_unit end) else 0 end)*4/100 as ann_qtly_gpv_dllr
from app_bi.pentagon.aggregate_seller_daily_payment_summary
group by 1
);

-- presale (non-delivery exposure) for each seller by business type
create or replace table presale1 as (
select 
accts.*
,NVL(SUM(CASE WHEN payment_trx_recognized_date >= DATEADD(DAY, -cnp_presale_days, current_date()) AND payment_trx_recognized_date <= current_date() 
         THEN (case when dps.currency_code = 'JPY' then cnp_card_payment_amount_base_unit_usd*100 ELSE cnp_card_payment_amount_base_unit_usd END) / 100 END), 0) AS cnp_gpv_us_dllr
,NVL(SUM(CASE WHEN payment_trx_recognized_date >= DATEADD(DAY, -cp_presale_days, current_date()) AND payment_trx_recognized_date <= current_date() 
         THEN (case when dps.currency_code = 'JPY' then cp_card_payment_amount_base_unit_usd*100 ELSE cp_card_payment_amount_base_unit_usd END) / 100 END),0) AS cp_gpv_us_dllr
,cnp_gpv_us_dllr+cp_gpv_us_dllr as presale_exposure_us_dllr
,NVL(SUM(CASE WHEN payment_trx_recognized_date >= DATEADD(DAY, -cnp_presale_days, current_date()) AND payment_trx_recognized_date <= current_date() 
         THEN (case when dps.currency_code = 'JPY' then cnp_card_payment_amount_base_unit*100 ELSE cnp_card_payment_amount_base_unit END) /100 END), 0) AS cnp_gpv_base_dllr
,NVL(SUM(CASE WHEN payment_trx_recognized_date >= DATEADD(DAY, -cp_presale_days, current_date()) AND payment_trx_recognized_date <= current_date() 
         THEN (case when dps.currency_code = 'JPY' then cp_card_payment_amount_base_unit*100 ELSE cp_card_payment_amount_base_unit END) / 100 END),0) AS cp_gpv_base_dllr
,cnp_gpv_base_dllr+cp_gpv_base_dllr as presale_exposure_base_dllr
from accts
left join fivetran.app_risk.policy_mcc_presale presale
on accts.business_type = presale.business_type
left join app_bi.pentagon.aggregate_seller_daily_payment_summary dps
on accts.user_token = dps.unit_token
group by 1,2,3,4,5,6)
;

-- create presale override table using agent data
CREATE OR REPLACE TABLE presale_override AS (
WITH 
trailing_365_gpv AS --calc trailing 1 year gpv value at user level
(SELECT asdsp.user_token
    , sum(CASE WHEN asdsp.currency_code = 'JPY' THEN asdsp.gpv_payment_amount_base_unit
            ELSE asdsp.gpv_payment_amount_base_unit/100 END) AS gpv_t365
    FROM app_bi.pentagon.aggregate_seller_daily_payment_summary asdsp
    WHERE asdsp.payment_trx_recognized_date >= dateadd(day, -365, current_date)
        AND asdsp.gpv_payment_count > 0
    GROUP BY 1)

, base AS --get presale values (tagged as CR:Exposure) from credit_risk_inputs
(SELECT a.user_token
    , a.created_at
    , a.long_value
    , du.best_available_merchant_token
    FROM app_risk.app_risk.credit_risk_inputs a
    LEFT JOIN app_bi.pentagon.dim_user du
        ON a.user_token = du.user_token
    WHERE a.name IN ('CR:Exposure')
    AND a.long_value IS NOT null)

, base2 AS --get last entered value for each merchant
(SELECT base.*
    FROM base
    INNER JOIN
    (SELECT best_available_merchant_token, max(created_at) AS max_date
        FROM base
        GROUP BY 1) b
    ON base.best_available_merchant_token = b.best_available_merchant_token
        AND base.created_at = b.max_date)

, base3 as --add merchant/user level trailing gpv data
(SELECT base2.*
    , du.user_token AS merchant_user_tokens
    , trailing_365_gpv.gpv_t365
    FROM base2
    LEFT JOIN app_bi.pentagon.dim_user du
        ON base2.best_available_merchant_token = du.best_available_merchant_token
        AND du.is_unit = 1
    LEFT JOIN trailing_365_gpv
        ON du.user_token = trailing_365_gpv.user_token)

, temp as --aggregate merchant level gpv
(SELECT best_available_merchant_token
    , sum(gpv_t365) AS merchant_gpv_t365
    FROM base3
    GROUP BY 1)
--agent overrides are entered at merchant level but linked to user_token
--we're allocating merchant level presale based on user gpv contribution to overall merchant gpv
--when there is no gpv data, use agent presale data
(SELECT base3.*
    , merchant_gpv_t365
    , CASE WHEN (gpv_t365 IS null AND user_token = merchant_user_tokens AND merchant_gpv_t365 IS null) THEN 1
           ELSE nvl(gpv_t365 / merchant_gpv_t365,0) END AS pct_exposure
    , long_value * pct_exposure AS user_presale
    FROM base3
    LEFT JOIN temp
        ON base3.best_available_merchant_token = temp.best_available_merchant_token
    --where gpv_t365 is null
)
)
;

--override calculated presale exposure with analyst tags and recalc total exposure
CREATE OR REPLACE TABLE presale AS (
SELECT a.user_token
    , a.currency_code
    , a.mcc
    , a.business_type
    , a.onboard_tenure_mth
    , a.pmt_tenure_mth
    , CASE WHEN b.user_presale IS NOT null
        THEN b.user_presale ELSE a.presale_exposure_base_dllr
        END AS non_delivery_exposure_dllr
FROM presale1 a
LEFT JOIN presale_override b
    ON a.user_token = b.merchant_user_tokens
)
;


create or replace table seller_segmentation as (
select 
accts.user_token
, accts.currency_code
, accts.mcc
, accts.business_type
, accts.onboard_tenure_mth
, case when accts.onboard_tenure_mth <3 then '0.onboard_tenure < 3mth'
    when accts.onboard_tenure_mth >=3 then '1.onboard_tenure >= 3mth' end as onboard_tenure_band
, accts.pmt_tenure_mth
, case when accts.pmt_tenure_mth <3 then '0.pmt_tenure < 3mth'
    when accts.pmt_tenure_mth >=3 then '1.pmt_tenure >= 3mth' end as pmt_tenure_band
, case when gpv.ann_gpv_dllr>gpv.ann_qtly_gpv_dllr then gpv.ann_gpv_dllr else gpv.ann_qtly_gpv_dllr end as ann_gpv_dllr
, case when ann_gpv_dllr < 25000 then '0.<25k'
    when ann_gpv_dllr <250000 then '1.25k-250k'
    when ann_gpv_dllr >=250000 then '2.>250k' end as gpv_band
,case when gpv.ann_qtly_gpv_dllr >0 then '0'
    when gpv.ann_qtly_gpv_dllr=0 then '1' end as churn_seller_ind
, presale.non_delivery_exposure_dllr
, case when presale.non_delivery_exposure_dllr <10000 then '0.presale < 10k'
    when presale.non_delivery_exposure_dllr >= 10000 then '1.preslae >= 10k' end as presale_band
from accts 
left join gpv
on accts.user_token = gpv.user_token
left join presale
on accts.user_token = presale.user_token
where gpv.ann_gpv_dllr > 0
)
;
