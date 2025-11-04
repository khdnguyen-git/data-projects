/*==============================================================================
 * Pull therapy claims from COSMOS OP + PR for 2021+
 *==============================================================================*/
drop table if exists tmp_1m.kn_mbm_cpbp;
create table tmp_1m.kn_mbm_cpbp as
select 
	'OP' as component
	, a.fst_srvc_year
	, a.brand_fnl
	, a.contract_fnl 
	, a.pbp_fnl
	, a.contractpbp_fnl
	, left(a.contractpbp_fnl, 9) as contractpbp_fnl_l9
	, a.product_level_3_fnl
	, a.tfm_product_fnl
	, a.tfm_product_new_fnl
	, a.tadmprodrollup_fnl
from fichsrv.cosmos_op as a
where a.fst_srvc_year >= 2021
	and a.tfm_include_flag = 1 			
	and a.global_cap in ('NA')			
	and a.product_level_3_fnl not in ('INSTITUTIONAL', 'DUAL')			
	and a.plan_level_2_fnl not in ('PFFS')			
	and a.special_network not in ('ERICKSON')			
	and a.st_abbr_cd = a.market_fnl			
	and a.prov_prtcp_sts_cd = 'P'			
	and substring(coalesce(a.bil_typ_cd,'0'),0,1) <> 3			
	and a.ama_pl_of_srvc_cd <> 12 			
	and (a.proc_cd in 			
		('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 '98940', '98941', '98942') 
		or a.rvnu_cd in ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
	and a.proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
union all
select 
	'PR' as component
	, a.fst_srvc_year
	, a.brand_fnl
	, a.contract_fnl 
	, a.pbp_fnl
	, a.contractpbp_fnl
	, left(a.contractpbp_fnl, 9) as contractpbp_fnl_l9
	, a.product_level_3_fnl
	, a.tfm_product_fnl
	, a.tfm_product_new_fnl
	, a.tadmprodrollup_fnl
from fichsrv.cosmos_pr as a
where a.fst_srvc_year >= 2021
	and a.tfm_include_flag = 1 			
	and a.global_cap in ('NA')			
	and a.product_level_3_fnl not in ('INSTITUTIONAL', 'DUAL')			
	and a.plan_level_2_fnl not in ('PFFS')			
	and a.special_network not in ('ERICKSON')			
	and a.st_abbr_cd = a.market_fnl			
	and a.prov_prtcp_sts_cd = 'P'			
	and substring(coalesce(a.bil_typ_cd,'0'),0,1) <> 3			
	and a.ama_pl_of_srvc_cd <> 12 			
	and (a.proc_cd in 			
		('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
		 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
		 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
		 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
		 '98940', '98941', '98942') 
		or a.rvnu_cd in ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
	and a.proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131')
;


/*==============================================================================
 * Import contractpbp_fnl list from Excel file
 *==============================================================================*/
drop table if exists tmp_7d.kn_mbm_contractpbp_checklist;
create table tmp_7d.kn_mbm_contractpbp_checklist as
select "contractpbp_fnl" as cpbp 
from tmp_7d.MBM_CONTRACTPBP_FNL_CHECK
;
/*==============================================================================
 * Join contractpbp_fnl list with mbm claims, and make flags for XLOOKUP
 *==============================================================================*/

drop table if exists tmp_1m.kn_mbm_cpbp_join;
create table tmp_1m.kn_mbm_cpbp_join as
with cte_join as (
select
    a.*
    , b.*
    , case when b.contractpbp_fnl_l9 is null then 'Not Found'
           else 'Found'
      end as cpbp_flag
from tmp_7d.kn_mbm_contractpbp_checklist as a
left join tmp_1m.kn_mbm_cpbp as b
	on a.cpbp = b.contractpbp_fnl_l9
    or (a.cpbp = 'H2001-8XX'
    and b.contractpbp_fnl_l9 like 'H2001-8__'
    )
)
select
	fst_srvc_year
	, brand_fnl
	, cpbp
	, contractpbp_fnl_l9
	, cpbp_flag
	, contractpbp_fnl
	, component
	, product_level_3_fnl
	, tfm_product_fnl
	, tfm_product_new_fnl
	, tadmprodrollup_fnl
	, count(*) as n_row
from cte_join
group by 
	fst_srvc_year
	, brand_fnl
	, cpbp
	, contractpbp_fnl_l9
	, cpbp_flag
	, contractpbp_fnl
	, component
	, product_level_3_fnl
	, tfm_product_fnl
	, tfm_product_new_fnl
	, tadmprodrollup_fnl
;

select count(*) from tmp_1m.kn_mbm_cpbp_join; 
-- 661


/*==============================================================================
 * Get info for Not Found cpbp
 *==============================================================================*/

drop table if exists tmp_1m.kn_mbm_cpbp_notfound;
create table tmp_1m.kn_mbm_cpbp_notfound as
with cte_claims as (
select distinct
	fst_srvc_year
	, brand_fnl
	, 'OP' as component
	, contractpbp_fnl
	, product_level_3_fnl
	, product_level_2_fnl
	, tfm_product_fnl
	, tfm_product_new_fnl
	, tadmprodrollup_fnl
	, tfm_include_flag
	, migration_source
	, global_cap
	, special_network
	, st_abbr_cd
	, prov_prtcp_sts_cd
	, bil_typ_cd
	, ama_pl_of_srvc_cd 
	, proc_cd
	, rvnu_cd
from fichsrv.cosmos_op
where left(contractpbp_fnl, 9) in 
('H1045-012'
, 'H1045-041'
, 'R0759-001'
, 'H1045-063'
, 'H5420-015'
)
and fst_srvc_year >= '2023'
union all 
select distinct
	fst_srvc_year
	, brand_fnl
	, 'PR' as component
	, contractpbp_fnl
	, product_level_3_fnl
	, product_level_2_fnl
	, tfm_product_fnl
	, tfm_product_new_fnl
	, tadmprodrollup_fnl
	, tfm_include_flag
	, migration_source
	, global_cap
	, special_network
	, st_abbr_cd
	, prov_prtcp_sts_cd
	, bil_typ_cd
	, ama_pl_of_srvc_cd 
	, proc_cd
	, rvnu_cd
from fichsrv.cosmos_pr
where left(contractpbp_fnl, 9) in 
('H1045-012'
, 'H1045-041'
, 'R0759-001'
, 'H1045-063'
, 'H5420-015'
)
and fst_srvc_year >= '2023'
)
select 
	fst_srvc_year
	, brand_fnl
	, component
	, contractpbp_fnl
	, product_level_3_fnl
	, product_level_2_fnl
	, tfm_product_fnl
	, tfm_product_new_fnl
	, tadmprodrollup_fnl
	, tfm_include_flag
	, migration_source
	, global_cap
	, special_network
	, st_abbr_cd
	, prov_prtcp_sts_cd
	, bil_typ_cd
	, ama_pl_of_srvc_cd 
	, proc_cd
	, rvnu_cd
	, count(*) as n_row
from cte_claims
group by 
	fst_srvc_year
	, brand_fnl
	, component
	, contractpbp_fnl
	, product_level_3_fnl
	, product_level_2_fnl
	, tfm_product_fnl
	, tfm_product_new_fnl
	, tadmprodrollup_fnl
	, tfm_include_flag
	, migration_source
	, global_cap
	, special_network
	, st_abbr_cd
	, prov_prtcp_sts_cd
	, bil_typ_cd
	, ama_pl_of_srvc_cd 
	, proc_cd
	, rvnu_cd
;

select count(*) from tmp_1m.kn_mbm_cpbp_notfound;
-- 435034
