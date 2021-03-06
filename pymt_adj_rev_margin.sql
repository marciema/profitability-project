create or replace table app_risk.app_risk.payment_related_adj_revenue_margin as
select
distinct rm12m.user_token
,rm12m.currency_code
,rm12m.business_category
,"6_mth_margin_pct"
,"12_mth_margin_pct"
,case when "6_mth_margin_pct" > 0 then "6_mth_margin_pct"
when "6_mth_margin_pct" <= 0 and "12_mth_margin_pct" > 0 then "12_mth_margin_pct" --final margin based on 6-mth margin; if 6-mth margin<0 then 12-mth margin; if both 6-mth and 12-mth margin<0, then mcc and currency average
else avg_margin_pct end as pymt_adj_rev_margin_pct
from (select
    distinct user_token
    ,currency_code
    ,business_category
    ,(TOT_ADJUSTED_REVENUE_LOCAL_PROCESSING-TOT_COGS_ESTIMATED_LOCAL_PROCESSING)/trailing_365d_local_gpv*100 as "12_mth_margin_pct"
    from app_risk.app_risk.seller_profit_v3) rm12m --margin based on past 12 month payments
left join (select distinct user_token
        ,(TOT_ADJUSTED_REVENUE_LOCAL_PROCESSING-TOT_COGS_ESTIMATED_LOCAL_PROCESSING)/trailing_180d_local_gpv*100 as "6_mth_margin_pct"
        from app_risk.app_risk.seller_profit_v2) rm6m --margin based on past 6 month payments
        on rm12m.user_token = rm6m.user_token
left join (select
        du.business_category
        ,currency_code
        ,sum(TOT_ADJUSTED_REVENUE_LOCAL_PROCESSING-TOT_COGS_ESTIMATED_LOCAL_PROCESSING)/sum(trailing_180d_local_gpv)*100 as avg_margin_pct
        from app_risk.app_risk.seller_profit_v2 sp
        left join app_bi.pentagon.dim_user du
        on sp.user_token = du.user_token
        group by 1,2) rmavg --average margin by mcc and currency
        on rm12m.business_category = rmavg.business_category
        and rm12m.currency_code = rmavg.currency_code
;
