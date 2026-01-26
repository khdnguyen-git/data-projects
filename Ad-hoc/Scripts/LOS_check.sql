
sdfkasdfjakldfjadkf 
df;akljdhfaklsdjfh


select
	*
from tmp_1m.EC_IP_DATASET_LOC_11052025 
limit 200


select los_categories, sum(case_count) from tmp_1m.EC_IP_DATASET_LOC_11052025 
where mnr_total_ffs_flag = 1 and ADMIT_ACT_MONTH = '202505' and los_categories = '1'
group by los_categories
order by los_categories
;

select los_categories, sum(case_count) from tmp_1m.EC_IP_DATASET_LOC_10292025
where mnr_total_ffs_flag = 1 and ADMIT_ACT_MONTH = '202505' and los_categories = '1'
group by los_categories
order by los_categories
;

select los_categories, sum(case_count) from tmp_1m.EC_IP_DATASET_LOC_10222025
where mnr_total_ffs_flag = 1 and ADMIT_ACT_MONTH = '202505' and los_categories = '1'
group by los_categories 
order by los_categories
;



select los_categories, count(*) from 
(
select
	TO_VARCHAR(admit_dt_act, 'YYYYMM') as month
	, case when los = 1 then '1'
		 when los between 0 and 1 then '0 - 1'
		 when los < 0 then '< 0'
	else '> 1'
	end as los_categories
	, case when a.svc_setting ='Inpatient' and a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('17 - Medical') then 1
		when a.svc_setting ='Inpatient' and  a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('30 - Surgical') then 1
	else 0
	end as loc_flag
from HCE_OPS_FNL.HCE_ADR_AVTAR_LIKE_24_25_F as a
)
where loc_flag = 1 and month >= '202412'
group by los_categories
order by los_categories


select * from HCE_OPS_FNL.HCE_ADR_AVTAR_LIKE_24_25_F
limit 5

select TO_VARCHAR(admit_dt_act, 'YYYYMM') from HCE_OPS_FNL.HCE_ADR_AVTAR_LIKE_24_25_F
limit 5 

case when a.svc_setting ='Inpatient' and a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('17 - Medical') then 1
		when a.svc_setting ='Inpatient' and  a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('30 - Surgical') then 1