create or replace table case_cb as (
select 
    distinct case_id,
    ca.user_token,
    case_created_at,
    sum(case when datediff(month, ca.case_created_at,cb.chargeback_date) between 0 and 5
        then chargeback_cents else 0 end)/100 as cb_dllr_6mth,
    sum(case when datediff(month, ca.case_created_at,cb.chargeback_date) between 0 and 11
        then chargeback_cents else 0 end)/100 as cb_dllr_12mth,
    sum(case when datediff(month, ca.case_created_at,cb.chargeback_date) between 0 and 5
        then loss_cents else 0 end)/100 as loss_dllr_6mth,
    sum(case when datediff(month, ca.case_created_at,cb.chargeback_date) between 0 and 11
        then loss_cents else 0 end)/100 as loss_dllr_12mth
from app_risk.app_risk.shealth_fact_risk_case_actions ca
left join app_risk.app_risk.chargebacks cb
on ca.user_token = cb.user_token
where case_type = 'credit_risk_review'
and date(case_created_at)>= to_date('2019-01-01') and date(case_created_at)< to_date('2021-06-30')
group by 1,2,3)
;

create or replace table case_6mth_profit as (
select 
    distinct case_id,
    sum(case when datediff(month, case_created_at, report_date) between 0 and 5 then profit else 0 end) as profit_6mth
from app_risk.app_risk.shealth_fact_risk_case_actions ca
left join (select user_token, report_date, profit from app_risk.app_risk.seller_profit_daily
           union all 
           select user_token, report_date, profit from app_risk.app_risk.seller_profit_daily_2019_2020
           )p
on ca.user_token = p.user_token
where case_type = 'credit_risk_review'
and date(case_created_at)>= to_date('2019-01-01') and date(case_created_at)< to_date('2021-06-30')
group by 1)
;

create or replace table case_12mth_profit as (
select 
    distinct case_id,
    sum(case when datediff(month, case_created_at, report_date) between 0 and 11 then profit else 0 end) as profit_12mth
from app_risk.app_risk.shealth_fact_risk_case_actions ca
left join (select user_token, report_date, profit from app_risk.app_risk.seller_profit_daily
           union all 
           select user_token, report_date, profit from app_risk.app_risk.seller_profit_daily_2019_2020
           )p
on ca.user_token = p.user_token
where case_type = 'credit_risk_review'
and date(case_created_at)>= to_date('2019-01-01') and date(case_created_at)< to_date('2021-06-30')
group by 1)
;

select count(case_id)
from case_12mth_profit
;

create or replace table app_risk.app_risk.credit_action_cb_profit_label as (
select
    distinct cb.case_id,
    cb.user_token,
    cb.case_created_at,
    cb.cb_dllr_6mth,
    cb.cb_dllr_12mth,
    cb.loss_dllr_6mth,
    cb.loss_dllr_12mth,
    profit_6mth,
    profit_12mth
    from case_cb cb
    left join case_6mth_profit as cp
    on cb.case_id = cp.case_id
    left join  case_12mth_profit as cp2
    on cb.case_id = cp2.case_id
)
;

select count(*),
count(distinct case_id)
,count_if(cb_dllr_12mth >=100)
,count_if(cb_dllr_6mth >=100)
,count_if(loss_dllr_12mth >=100)
,count_if(loss_dllr_6mth >=100)
,count_if(profit_12mth <=-100)
,count_if(profit_6mth <=-100)
from app_risk.app_risk.credit_action_cb_profit_label
;

select *
from app_risk.app_risk.credit_action_cb_profit_label
limit 5
;

select *
from app_risk.app_risk.chargebacks
where user_token = '21G6G67M0FMZZ'
and chargeback_date between '2021-06-01' and '2022-05-31'
;

select user_token, report_date, profit
from app_risk.app_risk.seller_profit_daily
where user_token = '21G6G67M0FMZZ'
and report_date between '2021-06-01' and '2022-05-31'
order by 2
;

