create or replace table loss_analysis as
select 
acct.user_token
,acct.currency_code
,du.business_category
,datediff(month, du.user_created_at,'2021-03-29') as tenure
,case when tenure >= 12 
,pre_6_mth_gpv
,pre_3_mth_gpv
,post_6_mth_gpv
,pre_6_mth_loss
,pre_3_mth_loss
,post_6_mth_loss
from (select 
      distinct user_token
      ,currency_code
    from app_risk.app_risk.seller_profit_snapshots_v2
    where cohort_date = '2021-03-29') as acct
left join app_bi.pentagon.dim_user du
on acct.user_token = du.user_token
left join (select 
           distinct user_token
           ,sum(case when payment_trx_recognized_date between '2020-10-01' and '2021-03-31' then gpv_payment_amount_base_unit else 0 end) as pre_6_mth_gpv
           ,sum(case when payment_trx_recognized_date between '2021-01-01' and '2021-03-31' then gpv_payment_amount_base_unit else 0 end) as pre_3_mth_gpv
           ,sum(case when payment_trx_recognized_date between '2021-04-01' and '2021-09-30' then gpv_payment_amount_base_unit else 0 end) as post_6_mth_gpv
           from app_bi.pentagon.aggregate_seller_daily_payment_summary
          where payment_trx_recognized_date between '2020-10-01' and '2021-09-30'
          group by 1) gpv
on acct.user_token = gpv.user_token
left join (select 
           distinct user_token
           ,sum(case when payment_created_at between '2020-10-01' and '2021-03-31' then loss_cents else 0 end) as pre_6_mth_loss
           ,sum(case when payment_created_at between '2021-01-01' and '2021-03-31' then loss_cents else 0 end) as pre_3_mth_loss
           ,sum(case when payment_created_at between '2021-04-01' and '2021-09-30' then loss_cents else 0 end) as post_6_mth_loss
          from app_risk.app_risk.chargebacks
          where payment_created_at between '2020-10-01' and '2021-09-30'
          group by 1) as cb
on acct.user_token = cb.user_token
;

select 
currency_code
,business_category
,tenure_bucket
,sum(pre_6_mth_gpv) as sum_pre_6_mth_gpv
,sum(pre_3_mth_gpv) as sum_pre_3_mth_gpv
,sum(post_6_mth_gpv) as sum_post_6_mth_gpv
,sum(pre_6_mth_loss) as sum_pre_6_mth_loss
,sum(pre_3_mth_loss) as sum_pre_3_mth_loss
,sum(post_6_mth_loss) as sum_post_6_mth_loss
from loss_analysis
group by 1,2,3
order by 1,2,3
;
