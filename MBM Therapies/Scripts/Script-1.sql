--1)	Can you create a table of all MBIs for M&R FFS members who have had at least one therapies auth in the system since 9/1/24. Eric has the logic and it should be about 1M unique members
--2)	Of these ~1M members, how many also have an auth since 9/1/24 for a surgery service?
--3)	Of these ~1M members, how many also have an auth since 9/1/24 for any service (not just surgery)?
--4)	If the difference between (2) and (3) is large, we might want to look into what those services are (generally)



drop table if exists tmp_1m.kn_pa_202401_2025;
create or replace table tmp_1m.kn_pa_202401_2025 as
with raw_auth as (
select distinct
     business_segment
    , medicare_id
    , case_id
    , notif_yrmonth
    , substring(notif_yrmonth, 1, 4) as notif_year
    , entity
    , proc_cd
    , proc_unit_cnt
    , pa_program
    , case_decn_stat_cd
    , migration_source
    , fin_brand
    , fin_source_name
    , sgr_source_name
    , nce_tadm_dec_risk_type
    , tfm_include_flag
    , global_cap
    , fin_market
    , fin_state
    , fin_plan_level_2
    , fin_product_level_3
    , fin_g_i
    , group_number
    , group_name
    , svc_setting
    , hce_category
    , plc_of_svc_cd
    , admit_cat_cd
    , admit_act_month
    , svc_cat_dtl_cd
    , case_cur_svc_cat_dtl_cd
    , prim_srvc_cat
    , prim_srvc_sub_cat
	, case when b.surgical_flag = 'Yes' then 'Surgical'
		else 'Not Surgical'
	end as surgical_ind
	, b.hce_program_new
	, b.proc_desc
	, case when proc_cd in ('98940','98941','98942') then 'Chiro'			
		   when proc_cd in ('G0281', 'G0282', 'G0283', '97012', '97016', '97018', '97022', '97024', '97026', '97028', '97032', 
	                           '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', '97124', '97139', 
	                           '97140', '97150', '97161', '97162', '97163', '97164', '97530', '97533', '97535', '97537', '97542', 
	                           '97750', '97755', '97760', '97761', '97762', '97799', 'G0281', 'G0282', 'G0283', '97012', '97016', 
	                           '97018', '97022', '97024', '97026', '97028', '97032', '97033', '97034', '97035', '97036', '97039', 
	                           '97110', '97112', '97113', '97116', '97124', '97139', '97140', '97150', '97165', '97166', '97167', 
	                           '97168', '97530', '97532', '97533', '97535', '97537', '97542', '97750', '97755', '97760', '97761', 
	                           '97762', '97799', -- PT/OT
	                           '92507', '92508', '92521', '92522', '92523', '92524', '92526', '92626', '92627', '96105') /* ST */ then 'PT/OT/ST'
		else 'Not Therapies' 
	end as therapies_category
	, case when case_decn_stat_cd = 'PD-Partially Adverse Determination' then 1 
		else 0 
	end as partially_adverse
	, case when case_decn_stat_cd = 'AD-Fully Adverse Determination' or case_decn_stat_cd = 'AD - Fully Adverse Determination' then 1 
		else 0 
	end as fully_adverse
	, case when business_segment = 'MnR' and fin_brand = 'M&R' and global_cap = 'NA'
	    and sgr_source_name = 'COSMOS' and tfm_include_flag = 1 and fin_product_level_3 <> 'INSTITUTIONAL' then 1 
		else 0 
	end as mnr_cosmos_ffs_flag
	, case when business_segment = 'MnR' and fin_brand = 'M&R' and sgr_source_name = 'NICE' 
	    and nce_tadm_dec_risk_type = 'FFS' then 1 
		else 0 
	end as mnr_nice_ffs_flag
	, case when (business_segment = 'MnR' and fin_brand = 'M&R' and global_cap = 'NA' 
	    		and sgr_source_name = 'COSMOS' and tfm_include_flag = 1 and fin_product_level_3 <> 'INSTITUTIONAL') 
	    or (business_segment = 'MnR' and fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type = 'FFS') then 1 
		else 0 
	end as mnr_total_ffs_flag
	, case when (substring(notif_yrmonth, 1, 4) = '2024' and business_segment = 'CnS' and fin_brand in ('M&R', 'C&S') 
	    		and global_cap = 'NA' and sgr_source_name in ('COSMOS', 'CSP') and migration_source = 'OAH' and fin_state = 'MD') 
	    or (business_segment = 'MnR' and fin_brand = 'M&R' and migration_source = 'OAH') 
	    or (business_segment = 'CnS' and fin_brand = 'C&S' and migration_source = 'OAH') then 1 
		else 0 
	end as oah_flag
	, case when ((business_segment = 'CnS' and fin_brand in ('M&R', 'C&S') and migration_source <> 'OAH' 
	   			 and global_cap = 'NA' and fin_product_level_3 = 'DUAL' and sgr_source_name in ('COSMOS', 'CSP') 
	    		 and fin_state not in ('OK', 'NC', 'NM', 'NV', 'OH', 'TX')) 
	   	or (substring(notif_yrmonth, 1, 4) = '2024' and business_segment = 'CnS' and fin_brand in ('M&R', 'C&S') 
	   	and global_cap = 'NA' and sgr_source_name in ('COSMOS', 'CSP') and migration_source = 'OAH' and fin_state = 'MD')) then 1 
		else 0 
	end as cns_dual_flag
	, case when business_segment = 'MnR' and fin_brand = 'M&R' and fin_product_level_3 = 'DUAL' then 1 
		else 0 
	end as mnr_dual_flag
	, case when business_segment = 'MnR' and fin_brand = 'M&R' and fin_product_level_3 = 'INSTITUTIONAL' then 1 
		else 0 
	end as isnp_flag
from hce_ops_fnl.hce_adr_avtar_like_24_25_f as a
left join tmp_1y.kn_epal_dict as b
	on a.proc_cd = b.proc_code
where business_segment not in ('EnI', 'ERR', 'null') 
    and avtar_mtch_ind = 1
    and pa_program not in ('Not EPAL-Prime', 'Non-EPAL')
    and notif_yrmonth between '202401' and '202512'
    and case_id is not null
)
select distinct
	*
	, case when mnr_cosmos_ffs_flag = 1 then 'MnR_COSMOS_FFS'
			when mnr_nice_ffs_flag = 1 then 'MnR_NICE_FFS'
			when cns_dual_flag = 1 then 'CnS_Dual'
			when isnp_flag = 1 then 'ISNP'
			when oah_flag = 1 then 'OAH' 
		else 'N/A' 
	end as population_flag
from raw_auth
;


select * from tmp_1y.kn_epal_dict limit 10;

select * from hce_ops_lkup.proc_desc limit 10;


select * from hce_ops_lkup.NCHS_TYPE_DESCRIPTION limit 10;

select * from hce_ops_lkup.AVTAR_PROC_CATG_SUBCATG_DESC limit 10;



select count(distinct medicare_id)
from tmp_1m.kn_pa_202401_2025
where therapies_category in ('PT/OT/ST', 'Chiro')
	and notif_yrmonth >= '202409'
	and population_flag in ('MnR_COSMOS_FFS', 'MnR_NICE_FFS')






drop table if exists tmp_1m.kn_pa_therapies_v3;
create or replace table tmp_1m.kn_pa_therapies_v3 as 
with mbi_therapies as (
select distinct
	medicare_id as mbi
from tmp_1m.kn_pa_202401_2025
where therapies_category in ('PT/OT/ST', 'Chiro')
	and notif_yrmonth >= '202409'
	and population_flag in ('MnR_COSMOS_FFS', 'MnR_NICE_FFS')
)
select distinct
    a.mbi
    , b.case_id
    , case when a.mbi is not null then 1 else 0 end as at_least_1_therapy_auth
    , case when b.therapies_category in ('PT/OT/ST', 'Chiro') then 1 else 0 end as therapy_auth_flag
    , case when b.surgical_ind = 'Surgical' then 1 else 0 end as surgery_auth_flag
    , case when b.surgical_ind != 'Surgical' and b.therapies_category not in ('PT/OT/ST', 'Chiro') then 1 else 0 end as other_auth_flag
	, 1 as any_auth_flag
	, b.business_segment
    , b.notif_yrmonth
    , b.notif_year
    , b.entity
    , b.proc_cd
    , b.proc_desc
    , b.surgical_ind
    , b.proc_unit_cnt
    , b.pa_program
    , b.hce_program_new
    , b.case_decn_stat_cd
    , b.migration_source
    , b.fin_brand
    , b.fin_source_name
    , b.sgr_source_name
    , b.nce_tadm_dec_risk_type
    , b.tfm_include_flag
    , b.global_cap
    , b.fin_market
    , b.fin_state
    , b.fin_plan_level_2
    , b.fin_product_level_3
    , b.fin_g_i
    , b.group_number
    , b.group_name
    , b.svc_setting
    , b.hce_category
    , b.plc_of_svc_cd
    , b.admit_cat_cd
    , b.admit_act_month
    , b.svc_cat_dtl_cd
    , b.case_cur_svc_cat_dtl_cd
    , b.prim_srvc_cat
    , b.prim_srvc_sub_cat
	, b.partially_adverse
	, b.fully_adverse
	, b.population_flag
from mbi_therapies as a
left join tmp_1m.kn_pa_202401_2025 as b
	on a.mbi = b.medicare_id
	and b.notif_yrmonth >= '202409'
	and b.population_flag in ('MnR_COSMOS_FFS', 'MnR_NICE_FFS')
	and b.case_id is not null
;

drop table if exists tmp_1m.kn_pa_therapies_sum ;
create or replace table tmp_1m.kn_pa_therapies_sum as
select
    notif_yrmonth
    , notif_year
    , proc_cd
    , proc_desc
    , surgical_ind
    , proc_unit_cnt
    , pa_program
    , hce_program_new
    , fin_market
    , group_number
    , group_name
    , svc_setting
    , hce_category
    , plc_of_svc_cd
    , admit_cat_cd
    , svc_cat_dtl_cd
    , prim_srvc_cat
	, partially_adverse
	, fully_adverse
	, population_flag
    , at_least_1_therapy_auth
    , therapy_auth_flag
    , surgery_auth_flag
    , other_auth_flag
	, count(distinct mbi) as n_mbi
	, count(distinct case_id) as n_auth
from tmp_1m.kn_pa_therapies_v3
group by
    notif_yrmonth
    , notif_year
    , proc_cd
    , proc_desc
    , surgical_ind
    , proc_unit_cnt
    , pa_program
    , hce_program_new
    , fin_market
    , fin_product_level_3
    , group_number
    , group_name
    , svc_setting
    , hce_category
    , plc_of_svc_cd
    , admit_cat_cd
    , svc_cat_dtl_cd
    , prim_srvc_cat
	, partially_adverse
	, fully_adverse
	, population_flag
    , at_least_1_therapy_auth
    , therapy_auth_flag
    , surgery_auth_flag
    , other_auth_flag
;


select count(*) from tmp_1m.KN_PA_THERAPIES_sum








select surgery_auth_flag, therapies_auth_flag, count(distinct mbi) from tmp_1m.kn_pa_therapies_v3
group by 1, 2

select * from tmp_1m.KN_PA_THERAPIES_v3
where mbi = '1A02V68PG84'

select * from tmp_1m.KN_PA_THERAPIES_v3
where mbi = '1A01RT8XE01'



select
	*
from tmp_1m.kn_pa_202401_2025
where medicare_id = '9P23442342'


select
	*
from hce_ops_fnl.hce_adr_avtar_like_24_25_f
where medicare_id = '1A12345675'


select
	*
from hce_ops_fnl.hce_adr_avtar_like_24_25_f_1
where medicare_id = '9P23442342'

select
	*
from tmp_1m.kn_pa_202401_2025
where medicare_id = '9P23442342'


select count(*) from tmp_1m.kn_pa_therapies_v3


	
select
	mbi
	, sum(therapy_auth_flag)
	, sum(surgery_auth_flag)
	, sum(other_auth_flag)
	, sum(therapy_auth_flag) + sum(surgery_auth_flag) + sum(other_auth_flag)
group by 
	mbi
order by
	sum(surgery_auth_flag) desc


select
	medicare_id
	, notif_yrmonth
	, proc_cd
	, hce_category
	, svc_setting
	, admit_cat_cd
	, prim_srvc_cat 
	, surgical_ind
from tmp_1m.kn_pa_202401_2025
where medicare_id = '1TJ8HC2KV42'
order by notif_yrmonth 
-- 12 auths, 2 therapy, 8 surgery,  2 others

select
	medicare_id
	, notif_yrmonth
	, proc_cd
	, hce_category
	, svc_setting
	, admit_cat_cd
	, prim_srvc_cat 
	, surgical_ind
from tmp_1m.kn_pa_202401_2025
where medicare_id = '5C33HY3JP92'
order by notif_yrmonth 
-- 12 auths, 3 therapy, 7 surgery, 1 others

select count(*) from 





create or replace table tmp_1m.kn_pa_therapies_v3_p1 as
select * 
from tmp_1m.kn_pa_therapies_v3 
limit 1000000 offset 0;

create or replace table tmp_1m.kn_pa_therapies_v3_p2 as
select * 
from tmp_1m.KN_PA_THERAPIES_v3 
limit 1000000 offset 1000000;

create or replace table tmp_1m.kn_pa_therapies_v3_p3 as
select * 
from tmp_1m.KN_PA_THERAPIES_v3 
limit 2000000 offset 2000000;

select count(*) from tmp_1m.kn_pa_therapies_v3_p3;
select count(*) from tmp_1m.kn_pa_therapies_v3_p2;
select count(*) from tmp_1m.kn_pa_therapies_v3_p1;


select max(notif_yrmonth) from tmp_1m.kn_pa_therapies_v3









	
select at_least_1_therapy_auth, therapy_auth_flag, surgery_auth_flag, other_auth_flag, count(distinct mbi) from tmp_1m.kn_pa_therapies_v3
group by 1,2,3,4

select count(distinct mbi) from tmp_1m.kn_pa_therapies_v3


select count(*) from tmp_1m.kn_pa_202401_2025


select * from tmp_1m.kn_pa_202401_2025
where medicare_id = '1A01RT8XE01'


sele

)
select
	notif_yrmonth
    , notif_year
    , proc_cd
    , proc_desc
    , pa_program
    , hce_program_new
    , fin_market
    , svc_setting
    , hce_category
    , plc_of_svc_cd
    , admit_cat_cd
    , admit_act_month
    , svc_cat_dtl_cd
    , case_cur_svc_cat_dtl_cd
    , prim_srvc_cat
    , prim_srvc_sub_cat
	, therapies_category
	, partially_adverse
	, fully_adverse
	, population_flag
	, max(surgery_auth_loc) as surgery_auth_loc
	, max(surgery_auth_epal) as surgery_auth_epal
	, max(any_auth) as any_auth
	, count(distinct case_id) as n_auth
	, count(distinct mbi) as n_mbi
from auths as a
group by 
	notif_yrmonth
	, notif_year
    , proc_cd
    , proc_desc
    , pa_program
    , hce_program_new
    , fin_market
    , svc_setting
    , hce_category
    , plc_of_svc_cd
    , admit_cat_cd
    , admit_act_month
    , svc_cat_dtl_cd
    , case_cur_svc_cat_dtl_cd
    , prim_srvc_cat
    , prim_srvc_sub_cat
	, therapies_category
	, partially_adverse
	, fully_adverse
	, population_flag
;



select count(distinct medicare_id)
from tmp_1m.kn_pa_202401_2025
where therapies_category != 'Others'
	and notif_yrmonth >= '202409'
	and population_flag in ('MnR_COSMOS_FFS', 'MnR_NICE_FFS')
	
	
	
select sum(n_mbi)
from tmp_1m.KN_PA_THERAPIES 
where notif_yrmonth >= '202409'
	and population_flag in ('MnR_COSMOS_FFS', 'MnR_NICE_FFS')
	
	
select therapies_category from tmp_1m.kn_pa_therapies
group by THERAPIES_CATEGORY
	
select notif_yrmonth, count(distinct case_id)
from tmp_1m.kn_pa_202401_2025
where therapies_category != 'Not Therapies'
	and notif_yrmonth >= '202501'
	and population_flag in ('MnR_COSMOS_FFS', 'MnR_NICE_FFS')
group by notif_yrmonth
order by notif_yrmonth


select notif_yrmonth, sum(n_auth)
from tmp_1m.KN_PA_THERAPIES
where therapies_category != 'Not Therapies'
	and notif_yrmonth >= '202501'
	and population_flag in ('MnR_COSMOS_FFS', 'MnR_NICE_FFS')
group by notif_yrmonth
order by notif_yrmonth


select notif_yrmonth, sum(n_auth)
from tmp_1m.KN_PA_THERAPIES_v2
where therapies_category != 'Not Therapies'
	and notif_yrmonth >= '202501'
	and population_flag in ('MnR_COSMOS_FFS', 'MnR_NICE_FFS')
group by notif_yrmonth
order by notif_yrmonth

drop table if exists tmp_1m.kn_pa_therapies_v2;
create or replace table tmp_1m.kn_pa_therapies_v2 as 
with mbi_therapies as (
select distinct
	medicare_id as mbi
from tmp_1m.kn_pa_202401_2025
where therapies_category != 'Others'
	and notif_yrmonth >= '202409'
	and population_flag in ('MnR_COSMOS_FFS', 'MnR_NICE_FFS')
)
,
auths as (
select distinct
    a.mbi
    , b.case_id
    , case when b.surgical_ind_loc = 'Surgical' then 1 else 0 end as surgery_auth_loc
    , case when b.surgical_ind_epal = 'Surgical' then 1 else 0 end as surgery_auth_epal
	, 1 as any_auth
	, b.business_segment
    , b.notif_yrmonth
    , b.notif_year
    , b.entity
    , b.proc_cd
    , b.proc_desc
    , b.proc_unit_cnt
    , b.pa_program
    , b.hce_program_new
    , b.case_decn_stat_cd
    , b.migration_source
    , b.fin_brand
    , b.fin_source_name
    , b.sgr_source_name
    , b.nce_tadm_dec_risk_type
    , b.tfm_include_flag
    , b.global_cap
    , b.fin_market
    , b.fin_state
    , b.fin_plan_level_2
    , b.fin_product_level_3
    , b.fin_g_i
    , b.group_number
    , b.group_name
    , b.svc_setting
    , b.hce_category
    , b.plc_of_svc_cd
    , b.admit_cat_cd
    , b.admit_act_month
    , b.svc_cat_dtl_cd
    , b.case_cur_svc_cat_dtl_cd
    , b.prim_srvc_cat
    , b.prim_srvc_sub_cat
	, b.surgical_ind_loc
	, b.surgical_ind_epal
	, b.therapies_category
	, b.partially_adverse
	, b.fully_adverse
	, b.population_flag
from mbi_therapies as a
left join tmp_1m.kn_pa_202401_2025 as b
	on a.mbi = b.medicare_id
	and b.notif_yrmonth >= '202409'
	and b.population_flag in ('MnR_COSMOS_FFS', 'MnR_NICE_FFS')
	and b.case_id is not null
)
select
	notif_yrmonth
    , notif_year
    , proc_cd
    , proc_desc
    , pa_program
    , hce_program_new
    , fin_market
    , svc_setting
    , hce_category
    , plc_of_svc_cd
    , admit_cat_cd
    , admit_act_month
    , svc_cat_dtl_cd
    , case_cur_svc_cat_dtl_cd
    , prim_srvc_cat
    , prim_srvc_sub_cat
	, therapies_category
	, partially_adverse
	, fully_adverse
	, population_flag
	, max(surgery_auth_loc) as surgery_auth_loc
	, max(surgery_auth_epal) as surgery_auth_epal
	, max(any_auth) as any_auth
	, count(distinct case_id) as n_auth
	, count(distinct mbi) as n_mbi
from auths as a
group by 
	notif_yrmonth
	, notif_year
    , proc_cd
    , proc_desc
    , pa_program
    , hce_program_new
    , fin_market
    , svc_setting
    , hce_category
    , plc_of_svc_cd
    , admit_cat_cd
    , admit_act_month
    , svc_cat_dtl_cd
    , case_cur_svc_cat_dtl_cd
    , prim_srvc_cat
    , prim_srvc_sub_cat
	, therapies_category
	, partially_adverse
	, fully_adverse
	, population_flag
;











select therapies_category, surgery_auth_loc, surgery_auth_loc, any_auth, sum(n_mbi)
from tmp_1m.kn_pa_therapies_v2
group by therapies_category, surgery_auth_loc, surgery_auth_loc, any_auth

select therapies_category, surgery_auth_loc, surgery_auth_loc, any_auth, sum(n_mbi)
from tmp_1m.kn_pa_therapies
group by therapies_category, surgery_auth_loc, surgery_auth_loc, any_auth

select therapies_category, surgery_auth_loc, surgery_auth_loc, any_auth, sum(n_auth)
from tmp_1m.kn_pa_therapies
group by therapies_category, surgery_auth_loc, surgery_auth_loc, any_auth
order by therapies_category desc
;

select therapies_category, surgery_auth_loc, surgery_auth_loc, any_auth, sum(n_auth)
from tmp_1m.kn_pa_therapies_v2
group by therapies_category, surgery_auth_loc, surgery_auth_loc, any_auth
order by therapies_category desc
;

select therapies_category, surgery_auth_loc, surgery_auth_loc, any_auth, sum(n_mbi)
from tmp_1m.kn_pa_therapies
group by therapies_category, surgery_auth_loc, surgery_auth_loc, any_auth
order by therapies_category desc
;

select therapies_category, surgery_auth_loc, surgery_auth_loc, any_auth, sum(n_mbi)
from tmp_1m.kn_pa_therapies_v2
group by therapies_category, surgery_auth_loc, surgery_auth_loc, any_auth
order by therapies_category desc
;




select count(*) from tmp_1m.kn_pa_therapies_v2;
select count(*) from tmp_1m.kn_pa_therapies;















select surgery_auth, sum(n_mbi) from tmp_1m.kn_pa_therapies
group by 1

select count(*) from tmp_1m.kn_pa_therapies_v2

select any_auth, sum(n_mbi) from tmp_1m.KN_PA_THERAPIES_v2
group by 1

select any_auth, sum(n_mbi) from tmp_1m.KN_PA_THERAPIES
group by 1



select any_auth, sum(n_auth) from tmp_1m.KN_PA_THERAPIES_v2
group by 1;

select any_auth, sum(n_auth) from tmp_1m.KN_PA_THERAPIES
group by 1;


select any_auth, sum(n_mbi) from tmp_1m.KN_PA_THERAPIES 
group by 1


select any_auth, sum(n_mbi) from tmp_1m.KN_PA_THERAPIES 
group by 1


select distinct population_flag from tmp_1m.kn_pa_therapies


select sum(n_mbi) from  tmp_1m.KN_PA_THERAPIES  


select any_auth, count(distinct mbi) from tmp_1m.KN_PA_THERAPIES 
group by 1


select SURGERY_AUTH_EPAL, SURGERY_AUTH_LOC, sum(n_mbi) from tmp_1m.KN_PA_THERAPIES
where therapies_category != 'Others'
	and notif_yrmonth >= '202409'
	and population_flag in ('MnR_COSMOS_FFS', 'MnR_NICE_FFS')
group by SURGERY_AUTH_EPAL, SURGERY_AUTH_LOC




select
	, count(distinct a.mbi) as n_mbi
from mbi as a
left join tmp_1m.kn_pa_202401_2025 as b
	on a.mbi = b.medicare_id
where b.notif_yrmonth >= '202409'
group by
	b.business_segment
    , b.notif_yrmonth
    , b.notif_year
    , b.entity
    , b.proc_cd
    , b.proc_unit_cnt
    , b.pa_program
    , b.case_decn_stat_cd
    , b.migration_source
    , b.fin_brand
    , b.fin_source_name
    , b.sgr_source_name
    , b.nce_tadm_dec_risk_type
    , b.tfm_include_flag
    , b.global_cap
    , b.fin_market
    , b.fin_state
    , b.fin_plan_level_2
    , b.fin_product_level_3
    , b.fin_g_i
    , b.group_number
    , b.group_name
    , b.svc_setting
    , b.hce_category
    , b.plc_of_svc_cd
    , b.admit_cat_cd
    , b.admit_act_month
    , b.svc_cat_dtl_cd
    , b.case_cur_svc_cat_dtl_cd
    , b.prim_srvc_cat
	, b.surgical_ind 
	, b.therapies_category
	, b.partially_adverse
	, b.fully_adverse
	, b.population_flag
;



select surgical_ind, THERAPIES_CATEGORY, sum(n_mbi) from tmp_1m.kn_pa_therapies
where therapies_category != 'Others'
group by surgical_ind, THERAPIES_CATEGORY
