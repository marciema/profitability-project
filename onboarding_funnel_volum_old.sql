create or replace table cohort as(
select distinct payment_trx_recognized_date as cohort_date
from app_bi.pentagon.fact_payment_transactions
where mod(datediff(day, payment_trx_recognized_date, '2022-07-04'),7) = 0
and payment_trx_recognized_date >= '2020-01-01'
union all
select '2022-07-04' as cohort_date)
;

select * from cohort
order by cohort_date desc
;

/*
create or replace table users as(
select
distinct du.user_token,
du.country_code,
du.business_type,
du.business_category,
created_at as idv_complete_dt,
first_card_payment_date
from app_bi.pentagon.dim_user du
    left join app_bi.pentagon.aggregate_seller_lifetime_summary ls
    on du.user_token = ls.user_token
    left join (select distinct target_token, min(created_at) as created_at
               from web.raw_oltp.audit_logs 
               where action_name = 'can_accept_payment_cards'
               group by 1)al
    on du.user_token = al.target_token
where du.user_created_at_date >= '2020-01-01' 
)
;
*/

create or replace table target_biz_type_users as(
select
distinct du.user_token,
du.country_code,
du.business_type,
du.business_category,
created_at as idv_complete_dt,
first_card_payment_date
from app_bi.pentagon.dim_user du
    left join app_bi.pentagon.aggregate_seller_lifetime_summary ls
    on du.user_token = ls.user_token
    left join (select distinct target_token, min(created_at) as created_at
               from web.raw_oltp.audit_logs 
               where action_name = 'can_accept_payment_cards'
               group by 1)al
    on du.user_token = al.target_token
where du.user_created_at_date >= '2020-01-01' 
    and du.business_type in ('beauty_and_barber_shops',
'health_and_beauty_spas',
'charitible_orgs',
'education',
'membership_organizations',
'catering',
'restaurants',
'medical_services_and_health_practitioners',
'personal_services',
'architectural_and_surveying',
'automotive_services',
'carpentry_contractors',
'chemical_and_allied_products',
'concrete_work_contractors',
'construction_materials',
'contractors',
'furniture_repair_and_refinishing',
'home_and_repair',
'metal_service_centers',
'misc_commercial_equipment',
'misc_home_furnishing',
'special_trade_contractors',
'tire_retreading_and_repair_shops',
'cultural_attractions',
'hotels_and_lodging',
'movies_film',
'music_and_entertainment',
'recreation_services',
'sporting_events',
'tourism',
'travel_agencies_and_tour_operators',
'travel_tourism',
'business_services',
'consultant',
'printing_services',
'professional_services',
'real_estate',
'web_dev_design',
'electronics',
'furniture_home_goods',
'hardware_store',
'office_supply',
'taxicabs_and_limousines')
)
;

create or replace table payment_gt_5k as(
select
    distinct unit_token, 
    payment_trx_recognized_date
from app_bi.pentagon.fact_payment_transactions ftp
    left join target_biz_type_users u 
    on ftp.unit_token = u.user_token
where amount_base_unit > 500000
and is_gpv = 1
and payment_trx_recognized_date >= '2020-01-01'
and u.user_token is not null
)
;

create or replace table gpv_in_90days as(
select 
    distinct unit_token, 
    cohort_date,
    sum (case when currency_code = 'JPY' THEN gpv_payment_amount_base_unit*100 ELSE gpv_payment_amount_base_unit end)/100 as gpv_dllr_90day
from cohort
    left join app_bi.pentagon.aggregate_seller_daily_payment_summary sds
    on datediff(day, payment_trx_recognized_date, cohort_date) <= 90 
    and datediff(day, payment_trx_recognized_date, cohort_date) >= 0 
    left join target_biz_type_users u 
    on sds.unit_token = u.user_token
    where u.user_token is not null
    group by 1,2
    having gpv_dllr_90day >= 25000
)
;

create or replace table gpv_in_365days as(
select 
    distinct unit_token, 
    cohort_date,
    sum (case when currency_code = 'JPY' THEN gpv_payment_amount_base_unit*100 ELSE gpv_payment_amount_base_unit end)/100 as gpv_dllr_365day
from app_bi.pentagon.aggregate_seller_daily_payment_summary sds
    left join cohort
    on datediff(day, payment_trx_recognized_date, cohort_date) <= 365 
    and datediff(day, payment_trx_recognized_date, cohort_date) >= 0 
    left join target_biz_type_users u 
    on sds.unit_token = u.user_token
    where u.user_token is not null
    group by 1,2
    having gpv_dllr_365day >= 100000
)
;

select 
count(*),
count(distinct user_token),
count_if(idv_complete_dt is not null),
count_if(first_card_payment_date is not null),
count_if(idv_complete_dt is not null and first_card_payment_date is null),
count_if(idv_complete_dt is null and first_card_payment_date is not null)
from target_biz_type_users
where idv_complete_dt is null
;

select
idv.cohort_date,
IDV_COMPLETE_CNT,
FIRST_PMT_CNT,
PMT_GT_5K_CNT,
GPV_GT_25K_90DAY_CNT,
GPV_GT_25K_365DAY_CNT
from (select cohort_date,count(distinct user_token) as IDV_COMPLETE_CNT
      from target_biz_type_users u
      left join cohort c
       on datediff(day, u.idv_complete_dt, c.cohort_date) between 0 and 6
      group by 1
       order by cohort_date) idv
left join (select cohort_date,count(distinct user_token) as FIRST_PMT_CNT
           from target_biz_type_users u
           left join cohort c
            on datediff(day, u.first_card_payment_date, c.cohort_date) between 0 and 6
           group by 1
           order by cohort_date) fpd
on idv.cohort_date = fpd.cohort_date
left join (select cohort_date, count(distinct unit_token) as PMT_GT_5K_CNT
           from payment_gt_5k pmt
           left join cohort c
            on datediff(day, pmt.payment_trx_recognized_date, c.cohort_date) between 0 and 6
           group by 1
           order by cohort_date) gt5k
on idv.cohort_date = gt5k.cohort_date
left join (select cohort_date, count(distinct unit_token) as GPV_GT_25K_90DAY_CNT
           from gpv_in_90days gpv
           group by 1
           order by cohort_date) gpv_90
on idv.cohort_date = gpv_90.cohort_date
left join (select cohort_date, count(distinct unit_token) as GPV_GT_25K_365DAY_CNT
           from gpv_in_365days gpv
           group by 1
           order by cohort_date) gpv_365
on idv.cohort_date = gpv_365.cohort_date
;

select cohort_date,count(distinct user_token) as FIRST_PMT_CNT
           from target_biz_type_users u
           left join cohort c
            on datediff(day, u.first_card_payment_date, c.cohort_date) between 0 and 6
           group by 1
           order by cohort_date
;








select cohort_date,count(distinct user_token) as FIRST_PMT_CNT
           from target_biz_type_users u
           left join cohort c
            on datediff(day, u.first_card_payment_date, c.cohort_date) between 0 and 6
           group by 1
           order by cohort_date
           --where cohort_date is null
           --and first_card_payment_date is not null
           --limit 5
;

select * --count(*)
from target_biz_type_users
where first_card_payment_date > current_date()
limit 5
;



/*
casing, so there should not be duplicates 


