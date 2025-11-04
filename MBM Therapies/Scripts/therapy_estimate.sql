/*==============================================================================
 * MBM Claims
 *==============================================================================*/
-- COSMOS
drop table if exists tmp_1m.kn_mbm_cosmos_claims_202510;
create or replace table tmp_1m.kn_mbm_cosmos_claims_202510 as
select
	clm_aud_nbr as clm_id
	, gal_mbi_hicn_fnl as mbi
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
    , fst_srvc_month
    , fst_srvc_year
    , proc_cd
	, case when proc_cd in ('98940','98941','98942', 'G0281', 'G0282', 'G0283', '97012', '97016', '97018', '97022', '97024', 
								'97026', '97028', '97032', 
	                           '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', '97124', '97139', 
	                           '97140', '97150', '97161', '97162', '97163', '97164', '97530', '97533', '97535', '97537', '97542', 
	                           '97750', '97755', '97760', '97761', '97762', '97799', 'G0281', 'G0282', 'G0283', '97012', '97016', 
	                           '97018', '97022', '97024', '97026', '97028', '97032', '97033', '97034', '97035', '97036', '97039', 
	                           '97110', '97112', '97113', '97116', '97124', '97139', '97140', '97150', '97165', '97166', '97167', 
	                           '97168', '97530', '97532', '97533', '97535', '97537', '97542', '97750', '97755', '97760', '97761', 
	                           '97762', '97799', -- PT/OT
	                           '92507', '92508', '92521', '92522', '92523', '92524', '92526', '92626', '92627', '96105') /* ST */ then 'PT/OT/ST/Chiro'
		else 'Not Therapies'
	end as therapy_flag
from fichsrv.cosmos_pr
where brand_fnl = 'M&R'
	and global_cap = 'NA'
	and product_level_3_fnl not in ('INSTITUTIONAL', 'DUAL')
	and tfm_include_flag = 1
	and plan_level_2_fnl not in ('PFFS')			
	and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'
	and clm_dnl_f = 'N'
	and (substring(coalesce(bil_typ_cd,'0'),0,1) != 3 or substring(coalesce(bil_typ_cd,'0'),0,1) != '3')
	and (ama_pl_of_srvc_cd != 12 or ama_pl_of_srvc_cd != '12')     
	and fst_srvc_year >= '2023'
union all
select
	clm_aud_nbr as clm_id
	, gal_mbi_hicn_fnl as mbi
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
    , fst_srvc_month
    , fst_srvc_year
    , proc_cd
	, case when proc_cd in ('98940','98941','98942', 'G0281', 'G0282', 'G0283', '97012', '97016', '97018', '97022', '97024', 
								'97026', '97028', '97032', 
	                           '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', '97124', '97139', 
	                           '97140', '97150', '97161', '97162', '97163', '97164', '97530', '97533', '97535', '97537', '97542', 
	                           '97750', '97755', '97760', '97761', '97762', '97799', 'G0281', 'G0282', 'G0283', '97012', '97016', 
	                           '97018', '97022', '97024', '97026', '97028', '97032', '97033', '97034', '97035', '97036', '97039', 
	                           '97110', '97112', '97113', '97116', '97124', '97139', '97140', '97150', '97165', '97166', '97167', 
	                           '97168', '97530', '97532', '97533', '97535', '97537', '97542', '97750', '97755', '97760', '97761', 
	                           '97762', '97799', -- PT/OT
	                           '92507', '92508', '92521', '92522', '92523', '92524', '92526', '92626', '92627', '96105') /* ST */ then 'PT/OT/ST/Chiro'
	else 'Not Therapies' 
	end as therapy_flag
from fichsrv.cosmos_op
where brand_fnl = 'M&R'
	and global_cap = 'NA'
	and product_level_3_fnl not in ('INSTITUTIONAL', 'DUAL')
	and tfm_include_flag = 1
	and plan_level_2_fnl not in ('PFFS')			
	and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'
	and clm_dnl_f = 'N'
	and (substring(coalesce(bil_typ_cd,'0'),0,1) != 3 or substring(coalesce(bil_typ_cd,'0'),0,1) != '3') -- Home health
	and (ama_pl_of_srvc_cd != 12 or ama_pl_of_srvc_cd != '12')     -- Home health
	and fst_srvc_year >= '2023'
;



select 
	fst_srvc_year
	, therapy_flag
	, count(distinct mbi) as n_members
from tmp_1m.kn_mbm_cosmos_claims_202510
group by 
	fst_srvc_year
	, therapy_flag
;
select * from tmp_1m.kn_mbm_cosmos_claims_202510






select
	fin_inc_year
	, count(distinct fin_mbi_hicn_fnl) as mm
from fichsrv.tre_membership
where global_cap = 'NA'
	and tfm_include_flag = 1
	and fin_brand = 'M&R'
	and fin_product_level_3 not in ('INSTITUTIONAL', 'DUAL')
	and fin_inc_year >= 2023
group by
	fin_inc_year
;


create or replace table tmp_1m.kn_mbm_cosmos_claims_2023 as
select
	clm_aud_nbr as clm_id
	, gal_mbi_hicn_fnl as mbi
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
    , fst_srvc_month
    , fst_srvc_year
    , proc_cd
	, case when proc_cd in ('98940','98941','98942', 'G0281', 'G0282', 'G0283', '97012', '97016', '97018', '97022', '97024', 
								'97026', '97028', '97032', 
	                           '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', '97124', '97139', 
	                           '97140', '97150', '97161', '97162', '97163', '97164', '97530', '97533', '97535', '97537', '97542', 
	                           '97750', '97755', '97760', '97761', '97762', '97799', 'G0281', 'G0282', 'G0283', '97012', '97016', 
	                           '97018', '97022', '97024', '97026', '97028', '97032', '97033', '97034', '97035', '97036', '97039', 
	                           '97110', '97112', '97113', '97116', '97124', '97139', '97140', '97150', '97165', '97166', '97167', 
	                           '97168', '97530', '97532', '97533', '97535', '97537', '97542', '97750', '97755', '97760', '97761', 
	                           '97762', '97799', -- PT/OT
	                           '92507', '92508', '92521', '92522', '92523', '92524', '92526', '92626', '92627', '96105') /* ST */ then 'PT/OT/ST/Chiro'
		else 'Not Therapies'
	end as therapy_flag
from fichsrv.cosmos_pr
where brand_fnl = 'M&R'
	and global_cap = 'NA'
	and product_level_3_fnl not in ('INSTITUTIONAL', 'DUAL')
	and tfm_include_flag = 1
	and plan_level_2_fnl not in ('PFFS')			
	and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'
	and clm_dnl_f = 'N'
	and (substring(coalesce(bil_typ_cd,'0'),0,1) != 3 or substring(coalesce(bil_typ_cd,'0'),0,1) != '3')
	and (ama_pl_of_srvc_cd != 12 or ama_pl_of_srvc_cd != '12')     
	and fst_srvc_year = '2023'
union all
select
	clm_aud_nbr as clm_id
	, gal_mbi_hicn_fnl as mbi
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
    , fst_srvc_month
    , fst_srvc_year
    , proc_cd
	, case when proc_cd in ('98940','98941','98942', 'G0281', 'G0282', 'G0283', '97012', '97016', '97018', '97022', '97024', 
								'97026', '97028', '97032', 
	                           '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', '97124', '97139', 
	                           '97140', '97150', '97161', '97162', '97163', '97164', '97530', '97533', '97535', '97537', '97542', 
	                           '97750', '97755', '97760', '97761', '97762', '97799', 'G0281', 'G0282', 'G0283', '97012', '97016', 
	                           '97018', '97022', '97024', '97026', '97028', '97032', '97033', '97034', '97035', '97036', '97039', 
	                           '97110', '97112', '97113', '97116', '97124', '97139', '97140', '97150', '97165', '97166', '97167', 
	                           '97168', '97530', '97532', '97533', '97535', '97537', '97542', '97750', '97755', '97760', '97761', 
	                           '97762', '97799', -- PT/OT
	                           '92507', '92508', '92521', '92522', '92523', '92524', '92526', '92626', '92627', '96105') /* ST */ then 'PT/OT/ST/Chiro'
	else 'Not Therapies' 
	end as therapy_flag
from fichsrv.cosmos_op
where brand_fnl = 'M&R'
	and global_cap = 'NA'
	and product_level_3_fnl not in ('INSTITUTIONAL', 'DUAL')
	and tfm_include_flag = 1
	and plan_level_2_fnl not in ('PFFS')			
	and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'
	and clm_dnl_f = 'N'
	and (substring(coalesce(bil_typ_cd,'0'),0,1) != 3 or substring(coalesce(bil_typ_cd,'0'),0,1) != '3') -- Home health
	and (ama_pl_of_srvc_cd != 12 or ama_pl_of_srvc_cd != '12')     -- Home health
	and fst_srvc_year = '2023'
;

create or replace table tmp_1m.kn_mbm_cosmos_claims_2024 as
select
	clm_aud_nbr as clm_id
	, gal_mbi_hicn_fnl as mbi
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
    , fst_srvc_month
    , fst_srvc_year
    , proc_cd
	, case when proc_cd in ('98940','98941','98942', 'G0281', 'G0282', 'G0283', '97012', '97016', '97018', '97022', '97024', 
								'97026', '97028', '97032', 
	                           '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', '97124', '97139', 
	                           '97140', '97150', '97161', '97162', '97163', '97164', '97530', '97533', '97535', '97537', '97542', 
	                           '97750', '97755', '97760', '97761', '97762', '97799', 'G0281', 'G0282', 'G0283', '97012', '97016', 
	                           '97018', '97022', '97024', '97026', '97028', '97032', '97033', '97034', '97035', '97036', '97039', 
	                           '97110', '97112', '97113', '97116', '97124', '97139', '97140', '97150', '97165', '97166', '97167', 
	                           '97168', '97530', '97532', '97533', '97535', '97537', '97542', '97750', '97755', '97760', '97761', 
	                           '97762', '97799', -- PT/OT
	                           '92507', '92508', '92521', '92522', '92523', '92524', '92526', '92626', '92627', '96105') /* ST */ then 'PT/OT/ST/Chiro'
		else 'Not Therapies'
	end as therapy_flag
from fichsrv.cosmos_pr
where brand_fnl = 'M&R'
	and global_cap = 'NA'
	and product_level_3_fnl not in ('INSTITUTIONAL', 'DUAL')
	and tfm_include_flag = 1
	and plan_level_2_fnl not in ('PFFS')			
	and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'
	and clm_dnl_f = 'N'
	and (substring(coalesce(bil_typ_cd,'0'),0,1) != 3 or substring(coalesce(bil_typ_cd,'0'),0,1) != '3')
	and (ama_pl_of_srvc_cd != 12 or ama_pl_of_srvc_cd != '12')     
	and fst_srvc_year = '2024'
union all
select
	clm_aud_nbr as clm_id
	, gal_mbi_hicn_fnl as mbi
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
    , fst_srvc_month
    , fst_srvc_year
    , proc_cd
	, case when proc_cd in ('98940','98941','98942', 'G0281', 'G0282', 'G0283', '97012', '97016', '97018', '97022', '97024', 
								'97026', '97028', '97032', 
	                           '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', '97124', '97139', 
	                           '97140', '97150', '97161', '97162', '97163', '97164', '97530', '97533', '97535', '97537', '97542', 
	                           '97750', '97755', '97760', '97761', '97762', '97799', 'G0281', 'G0282', 'G0283', '97012', '97016', 
	                           '97018', '97022', '97024', '97026', '97028', '97032', '97033', '97034', '97035', '97036', '97039', 
	                           '97110', '97112', '97113', '97116', '97124', '97139', '97140', '97150', '97165', '97166', '97167', 
	                           '97168', '97530', '97532', '97533', '97535', '97537', '97542', '97750', '97755', '97760', '97761', 
	                           '97762', '97799', -- PT/OT
	                           '92507', '92508', '92521', '92522', '92523', '92524', '92526', '92626', '92627', '96105') /* ST */ then 'PT/OT/ST/Chiro'
	else 'Not Therapies' 
	end as therapy_flag
from fichsrv.cosmos_op
where brand_fnl = 'M&R'
	and global_cap = 'NA'
	and product_level_3_fnl not in ('INSTITUTIONAL', 'DUAL')
	and tfm_include_flag = 1
	and plan_level_2_fnl not in ('PFFS')			
	and special_network not in ('ERICKSON')			
	and prov_prtcp_sts_cd = 'P'
	and clm_dnl_f = 'N'
	and (substring(coalesce(bil_typ_cd,'0'),0,1) != 3 or substring(coalesce(bil_typ_cd,'0'),0,1) != '3') -- Home health
	and (ama_pl_of_srvc_cd != 12 or ama_pl_of_srvc_cd != '12')     -- Home health
	and fst_srvc_year = '2024'
;


with therapy_2023 as (
select
	mbi
	, fst_srvc_year
where therapy_flag = 'PT/OT/ST/Chiro'
from tmp_1m.kn_mbm_cosmos_claims_2023
)
, 
therapy_2024 as (
select
	mbi
	, fst_srvc_year
where therapy_flag = 'PT/OT/ST/Chiro'
from tmp_1m.kn_mbm_cosmos_claims_2024
)



with therapy_2023 as (
select distinct 
	mbi
from tmp_1m.kn_mbm_cosmos_claims_2023
where therapy_flag = 'PT/OT/ST/Chiro'
),
therapy_2024 as (
select distinct 
	mbi
from tmp_1m.kn_mbm_cosmos_claims_2024
where therapy_flag = 'PT/OT/ST/Chiro'
),
therapy_both as (
select 
	mbi 
from therapy_2023
intersect
select 
	mbi 
from therapy_2024
),
only_2023 as (
select a.mbi from therapy_2023 as a
left join therapy_both as b 
	on a.mbi = b.mbi
where b.mbi is null
),
only_2024 as (
select a.mbi from therapy_2024 as a
left join therapy_both as b 
	on a.mbi = b.mbi
where b.mbi is null
)
select
    (select count(*) from only_2023)  as only_2023
    , (select count(*) from only_2024)  as only_2024
    , (select count(*) from therapy_both) as both_2023_2024
;


with mm_2023 as (
select distinct
	fin_inc_year
	, fin_mbi_hicn_fnl as mbi
from fichsrv.tre_membership
where global_cap = 'NA'
	and tfm_include_flag = 1
	and fin_brand = 'M&R'
	and fin_product_level_3 not in ('INSTITUTIONAL', 'DUAL')
	and fin_inc_year = 2023
)
,
mm_2024 as (
select distinct
	fin_inc_year
	, fin_mbi_hicn_fnl as mbi
from fichsrv.tre_membership
where global_cap = 'NA'
	and tfm_include_flag = 1
	and fin_brand = 'M&R'
	and fin_product_level_3 not in ('INSTITUTIONAL', 'DUAL')
	and fin_inc_year = 2024
)
, 
mm_both as (
select 
	mbi
from mm_2023
intersect
select 
	mbi
from mm_2024
)
, 
mm_only_2023 as (
select a.mbi from mm_2023 as a
left join mm_both as b 
	on a.mbi = b.mbi
where b.mbi is null
),
mm_only_2024 as (
select a.mbi from mm_2024 as a
left join mm_both as b 
	on a.mbi = b.mbi
where b.mbi is null
)
select
    (select count(*) from mm_only_2023)  as only_2023
    , (select count(*) from mm_only_2024)  as only_2024
    , (select count(*) from mm_both) as both_2023_2024
;

