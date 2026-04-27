/*==============================================================================
 * Pulling Rx CGM claims from Medicare Part D db
 * Dropped membership exclusion
 *==============================================================================*/
drop table tmp_1m.kn_cgm_pharmacy;
create table tmp_1m.kn_cgm_pharmacy as 
select distinct
    mbi_hicn_fnl as mbi 
    , date_of_service
    , cast(servicemonth as varchar(6)) as service_month
    , gpi 
    , gpi_class 
    , drug_name 
    , ndc_hcedtl
    , product_service_id 
    , market_fnl 
    , case 
        when group_ind = 'Y' then 'G' 
        else 'I' 
    end as group_ind 
    , product_level_1_fnl 
    , product_level_2_fnl 
    , product_level_3_fnl 
    , tadm_tfm_product_new 
    , tadm_allowed 
    , tadm_net_pd 
    , script_cnt 
    , rx31 
    , case 
        when brand_fnl = 'C&S' then 'C&S DUALS'
        when product_level_3_fnl = 'INSTITUTIONAL' then 'M&R Non-FFS'
        when tadm_source = 'COSMOS' and migration_source <> 'OAH' and tadm_global_cap = 'NA' then 'M&R FFS'
        when tadm_source = 'NICE' and nce_tadm_dec_risk_type <> 'GLOBAL' then 'M&R FFS' 
        else 'M&R Non-FFS' 
    end as ffs_flag 
    , case 
        when product_service_id 
            in ('08627009111', '08627001601', '08627005303', '57599080300', '57599080000', '57599000200', 
                '57599000101', '57599083500') then 'Covered'
        else 'Non-Covered' 
    end as cgm_coverage
from mard.m1_claims_f
where product_service_id 
    in ('08627009111', '08627001601', '08627005303', '08627007801', '08627007701', '57599080300', '57599080000', 
        '57599000200', '57599000101', '57599081800', '76300000805', '43169070405', '63000017962', '63000033698', 
        '63000035844', '43169095568', '63000028678', '63000031699', '63000035751', '76300000260', '63000028585', 
        '57599081800', '57599000021', '57599082000', '57599084400', '57599083500', '76300000260', '63000028585', 
        '43169095568', '63000028677', '63000028678', '63000031699', '63000035751', '76300023982', '76300070501', 
        '76300010002', '76300070601', '63000017962', '63000033698', '63000035844', '76300017962', '43169070405', 
        '63000041338', '63000051968', '63000044515', '63000044516')
    -- and brand_fnl = 'M&R'
    -- and tadm_tfm_include_flag = 1
    -- and product_level_3_fnl not in ('INSTITUTIONAL')
    -- and tadm_source = 'COSMOS'
    -- and tadm_partb_flag = 'Y'
    and year(date_of_service) >= '2023'
;

/*==============================================================================
 * Classifying member and getting count
 * If member has a claim within the 6-month time window -> Renewed
 * tmp_1m.kn_cgm_mbi is the slightly cleaned Auth dataset coming from Mindy
 *==============================================================================*/
drop table tmp_1m.kn_cgm_auth_claim_count;
create table tmp_1m.kn_cgm_auth_claim_count as
select
	a.auth_month
	, count(distinct case when b.service_month is not null then a.mbi end) as mm
from tmp_1m.kn_cgm_mbi as a
left join tmp_1m.kn_cgm_pharmacy as b
	on a.mbi = b.mbi
	and b.service_month between
		case
			when substr(a.auth_month, 5, 2) = '01' then concat(cast(substr(a.auth_month, 1, 4) as int) - 1, '08')
			when substr(a.auth_month, 5, 2) = '02' then concat(cast(substr(a.auth_month, 1, 4) as int) - 1, '09')
			when substr(a.auth_month, 5, 2) = '03' then concat(cast(substr(a.auth_month, 1, 4) as int) - 1, '10')
			when substr(a.auth_month, 5, 2) = '04' then concat(cast(substr(a.auth_month, 1, 4) as int) - 1, '11')
			when substr(a.auth_month, 5, 2) = '05' then concat(cast(substr(a.auth_month, 1, 4) as int) - 1, '12')
			when substr(a.auth_month, 5, 2) = '06' then concat(substr(a.auth_month, 1, 4), '01')
			when substr(a.auth_month, 5, 2) = '07' then concat(substr(a.auth_month, 1, 4), '02')
			when substr(a.auth_month, 5, 2) = '08' then concat(substr(a.auth_month, 1, 4), '03')
			when substr(a.auth_month, 5, 2) = '09' then concat(substr(a.auth_month, 1, 4), '04')
			when substr(a.auth_month, 5, 2) = '10' then concat(substr(a.auth_month, 1, 4), '05')
			when substr(a.auth_month, 5, 2) = '11' then concat(substr(a.auth_month, 1, 4), '06')
			when substr(a.auth_month, 5, 2) = '12' then concat(substr(a.auth_month, 1, 4), '07')
		end 
	and a.auth_month
group by a.auth_month
order by a.auth_month
;

select
    auth_month
	, count(distinct case when member_cat = 'renewed' then mbi end) as renewed
    , count(distinct case when member_cat = 'new' then mbi end) as new
from tmp_1m.kn_cgm_auth_claim_member_cat
group by auth_month
order by auth_month;
--			new		renewed
--202501	3782	3140
--202502	2924	2186
--202503	3092	2634
--202504	4557	8269
--202505	4496	6377
--202506	4455	5241
--202507	4872	6112

/*================================================================================================
 * Classifying member to get the member_cat variable for joining/lookup to the Mindy's Auth data
 * Same as above, just with variable member_cat
 *================================================================================================*/
drop table tmp_1m.kn_cgm_auth_claim_member_cat;
create table tmp_1m.kn_cgm_auth_claim_member_cat as
select
	a.mbi
	, a.case_id as cgm_auth_caseid
	, a.auth_month
	, case when count(b.service_month) > 0 then 'renewed'
		else 'new'
	end as member_cat
	, count(distinct case_id) as n_case
from tmp_1m.kn_cgm_mbi as a
left join tmp_1m.kn_cgm_pharmacy as b
	on a.mbi = b.mbi
	and b.service_month between
		case
			when substr(a.auth_month, 5, 2) = '01' then concat(cast(substr(a.auth_month, 1, 4) as int) - 1, '08')
			when substr(a.auth_month, 5, 2) = '02' then concat(cast(substr(a.auth_month, 1, 4) as int) - 1, '09')
			when substr(a.auth_month, 5, 2) = '03' then concat(cast(substr(a.auth_month, 1, 4) as int) - 1, '10')
			when substr(a.auth_month, 5, 2) = '04' then concat(cast(substr(a.auth_month, 1, 4) as int) - 1, '11')
			when substr(a.auth_month, 5, 2) = '05' then concat(cast(substr(a.auth_month, 1, 4) as int) - 1, '12')
			when substr(a.auth_month, 5, 2) = '06' then concat(substr(a.auth_month, 1, 4), '01')
			when substr(a.auth_month, 5, 2) = '07' then concat(substr(a.auth_month, 1, 4), '02')
			when substr(a.auth_month, 5, 2) = '08' then concat(substr(a.auth_month, 1, 4), '03')
			when substr(a.auth_month, 5, 2) = '09' then concat(substr(a.auth_month, 1, 4), '04')
			when substr(a.auth_month, 5, 2) = '10' then concat(substr(a.auth_month, 1, 4), '05')
			when substr(a.auth_month, 5, 2) = '11' then concat(substr(a.auth_month, 1, 4), '06')
			when substr(a.auth_month, 5, 2) = '12' then concat(substr(a.auth_month, 1, 4), '07')
		end 
	and a.auth_month
group by 
	a.mbi
	, a.case_id
	, a.auth_month
;

select member_cat, count(distinct mbi) from tmp_1m.kn_cgm_auth_claim_member_cat
group by member_cat;


--select * from tmp_1m.kn_cgm_auth_claim_mm_check

drop table if exists tmp_1m.kn_cgm_auth;

select member_cat, count(*) from tmp_1m.kn_cgm_auth_claim_member_cat
group by member_cat;
--new	28178
--renewed	33959

select member_cat, count(distinct mbi) from tmp_1m.kn_cgm_auth_claim_member_cat
group by member_cat;
--new	27273
--renewed	33087

select case_decn_stat_cd from hce_proj_bd.hce_adr_avtar_like_24_25_f 
group by case_decn_stat_cd 

-- Inner
drop table tmp_1m.kn_cgm_auth_new_renewed_v2;
create table tmp_1m.kn_cgm_auth_new_renewed_v2 as
select distinct
	a.mbi
	, a.member_cat
	, a.cgm_auth_caseid
    , a.auth_month
	, a.cgm_rxdate
    , a.gpi 
    , a.gpi_class 
    , a.drug_name
    , a.script_cnt
	, b.case_id as avtar_caseid
	, b.svc_setting
    , b.initialfulladr_cases
    , b.persistentfulladr_cases
    , b.case_decn_stat_cd
    , year(b.notif_recd_dttm) as notif_year
    , concat(substr(b.notif_recd_dttm, 1, 4), substr(b.notif_recd_dttm, 6, 2)) as notif_month
	, b.mnr_hce_drv_par_status as prov_parstatus
  	, b.fin_source_name
    , b.migration_source
    , b.fin_product_level_3
    , b.tfm_include_flag
    , b.global_cap
    , b.nce_tadm_dec_risk_type
    , b.fin_contractpbp
    , b.fin_contract_nbr
    , b.fin_pbp
    , b.fin_market
    , b.fin_region
    , b.fin_state
    , b.fin_plan_level_2
    , b.fin_g_i
    , b.fin_brand
    , b.group_number
    , b.group_name
from tmp_1m.kn_cgm_auth_claim_member_cat as a
inner join hce_proj_bd.hce_adr_avtar_like_24_25_f as b
	on a.mbi = b.medicare_id
	and year(b.notif_recd_dttm) >= 2024
	--and a.auth_month = concat(substr(b.notif_recd_dttm, 1, 4), substr(b.notif_recd_dttm, 6, 2))
;

select * from tmp_1m.kn_cgm_auth;

drop table tmp_1m.kn_cgm_auth_new_renewed;
create table tmp_1m.kn_cgm_auth_new_renewed as
select 
	a.mbi
	, a.member_cat
	, a.cgm_rxdate
    , a.gpi 
    , a.gpi_class 
    , a.drug_name
    , a.script_cnt
    , a.auth_month
	, b.case_id
	, b.svc_setting
    , b.initialfulladr_cases
    , b.persistentfulladr_cases
    , b.case_decn_stat_cd
    , year(b.notif_recd_dttm) as notif_year
    , concat(substr(b.notif_recd_dttm, 1, 4), substr(b.notif_recd_dttm, 6, 2)) as notif_month
	, b.mnr_hce_drv_par_status as prov_parstatus
  	, b.fin_source_name
    , b.migration_source
    , b.fin_product_level_3
    , b.tfm_include_flag
    , b.global_cap
    , b.nce_tadm_dec_risk_type
    , b.fin_contractpbp
    , b.fin_contract_nbr
    , b.fin_pbp
    , b.fin_market
    , b.fin_region
    , b.fin_state
    , b.fin_plan_level_2
    , b.fin_g_i
    , b.fin_brand
    , b.group_number
    , b.group_name
from tmp_1m.kn_cgm_auth_claim_member_cat as a
left join hce_proj_bd.hce_adr_avtar_like_24_25_f as b
	on a.mbi = b.medicare_id
	and year(b.notif_recd_dttm) >= 2024
	--and a.auth_month = concat(substr(b.notif_recd_dttm, 1, 4), substr(b.notif_recd_dttm, 6, 2))
;













-- In pharmacy auth but not avtar: 30,052
select
	count(distinct case when b.medicare_id is null then a.mbi end) as mm
from tmp_1m.kn_cgm_auth_claim_member_cat as a
left join hce_proj_bd.hce_adr_avtar_like_24_25_f as b
	on a.mbi = b.medicare_id
	and year(b.notif_recd_dttm) >= 2024
;

-- In pharmacy auth: 60,067
select
	count(distinct a.mbi) as mm
from tmp_1m.kn_cgm_auth_claim_member_cat as a
;







select notif_month, member_cat, count(distinct case_id) as n_case from tmp_1m.kn_cgm_auth_new_renewed
group by notif_month, member_cat;

select 
	notif_month
	, member_cat
	, count(distinct case_decn_stat_cd)
from tmp_1m.kn_cgm_auth_new_renewed
group by 
	notif_month
	, member_cat
;

select
	notif_month
	, count(distinct case when member_cat = 'renewed' then case_id end) as case_renewed
	, count(distinct case when member_cat = 'new' then case_id end) as case_new
	, count(distinct case when member_cat = 'renewed' then mbi end) as mm_renewed
	, count(distinct case when member_cat = 'new' then mbi end) as mm_new
	, count(distinct case when member_cat = 'renewed' and case_decn_stat_cd = 'AD - Fully Adverse Determination'
	then case_id end) as denied_renewed
	, count(distinct case when member_cat = 'new' and case_decn_stat_cd = 'AD - Fully Adverse Determination'
	then case_id end) as denied_new
	, count(distinct case when member_cat = 'renewed' and case_decn_stat_cd = 'FA - Fully Approved'
	then case_id end) as approved_renewed
	, count(distinct case when member_cat = 'new' and case_decn_stat_cd = 'FA - Fully Approved'
	then case_id end) as approved_new
from tmp_1m.kn_cgm_auth_new_renewed
group by
	notif_month
order by
	notif_month
;

drop table tmp_1m.kn_cgm_auth_new_renewed_v2;
create table tmp_1m.kn_cgm_auth_new_renewed_v2 as
select 
	a.mbi
	, a.member_cat
	, b.case_id
    , b.initialfulladr_cases
    , b.persistentfulladr_cases
    , b.case_decn_stat_cd
    , year(b.notif_recd_dttm) as notif_year
    , concat(substr(b.notif_recd_dttm, 1, 4), substr(b.notif_recd_dttm, 6, 2)) as notif_month
	, b.mnr_hce_drv_par_status as prov_parstatus
  	, b.fin_source_name
    , b.migration_source
    , b.fin_product_level_3
    , b.tfm_include_flag
    , b.global_cap
    , b.nce_tadm_dec_risk_type
    , b.fin_contractpbp
    , b.fin_contract_nbr
    , b.fin_pbp
    , b.fin_market
    , b.fin_region
    , b.fin_state
    , b.fin_plan_level_2
    , b.fin_g_i
    , b.fin_brand
    , b.group_number
    , b.group_name
from tmp_1m.kn_cgm_auth_claim_member_cat as a
left join hce_proj_bd.hce_adr_avtar_like_24_25_f as b
	on a.mbi = b.medicare_id
	and year(b.notif_recd_dttm) >= 2024
	--and a.auth_month = concat(substr(b.notif_recd_dttm, 1, 4), substr(b.notif_recd_dttm, 6, 2))
;

