create or replace table case_cb_loss as (
select 
    distinct case_id,
    ca.user_token,
    case_created_at,
    sum(case when datediff(day, ca.case_created_at,cb.chargeback_date) between 0 and 180
        then chargeback_cents else 0 end)/100 as cb_dllr_6mth,
    sum(case when datediff(day, ca.case_created_at,cb.chargeback_date) between 0 and 365
        then chargeback_cents else 0 end)/100 as cb_dllr_12mth,
    sum(case when datediff(day, ca.case_created_at,cb.chargeback_date) between 0 and 180
        then loss_cents else 0 end)/100 as loss_dllr_6mth,
    sum(case when datediff(day, ca.case_created_at,cb.chargeback_date) between 0 and 365
        then loss_cents else 0 end)/100 as loss_dllr_12mth,
    sum(case when (datediff(day, ca.case_created_at,cb.chargeback_date) between 0 and 180) and (datediff(day, to_date(payment_created_at), case_created_at)>180)
        then loss_cents else 0 end)/100 as loss_dllr_6mth_v2,
    sum(case when (datediff(day, ca.case_created_at,cb.chargeback_date) between 0 and 365) and (datediff(day, to_date(payment_created_at), case_created_at)>365)
        then loss_cents else 0 end)/100 as loss_dllr_12mth_v2
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
    sum(case when datediff(day, case_created_at, report_date) between 0 and 180 then profit else 0 end) as profit_6mth
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
    sum(case when datediff(day, case_created_at, report_date) between 0 and 365 then profit else 0 end) as profit_12mth
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

create or replace table app_risk.app_risk.credit_action_cb_profit_label_v3 as (
select
    distinct cl.case_id,
    cl.user_token,
    cl.case_created_at,
    cl.cb_dllr_6mth,
    cl.cb_dllr_12mth,
    cl.loss_dllr_6mth,
    cl.loss_dllr_12mth,
    profit_6mth,
    profit_6mth - cl.loss_dllr_6mth_v2 as profit_w_loss_6mth,
    profit_12mth, 
    profit_12mth - cl.loss_dllr_12mth_v2 as profit_w_loss_12mth
    from case_cb_loss cl
    left join case_6mth_profit as cp
    on cl.case_id = cp.case_id
    left join  case_12mth_profit as cp2
    on cl.case_id = cp2.case_id
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
,count_if(profit_w_loss_12mth <=-100)
,count_if(profit_w_loss_6mth <=-100)
from app_risk.app_risk.credit_action_cb_profit_label_v3
;

select count(*),
count(distinct case_id)
,sum(cb_dllr_12mth)
,sum(cb_dllr_6mth)
,sum(loss_dllr_12mth)
,sum(loss_dllr_6mth)
,sum(profit_12mth)
,sum(profit_6mth)
,sum(profit_w_loss_12mth)
,sum(profit_w_loss_6mth)
from app_risk.app_risk.credit_action_cb_profit_label_v3
;
