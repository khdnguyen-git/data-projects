select
    *
from hce_proj_bd.hce_adr_avtar_like_24_25_f
where business_segment not in ('EnI', 'ERR', 'null')
    and medicare_id is not null
    and proc_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
    and notif_yrmonth between '202401' and '202512' limit 200
from tmp_7d.esb_nonemergent_codes_2023 limit 50select count(*)
from tmp_7d.esb_nonemergent_codes_2023 describe hce_proj_bd.hce_adr_avtar_like_24_25_f
;

drop table tmp_1m.kn_nemt_2023;
create table tmp_1m.kn_nemt_2023 stored as orc as
select
    case_id
    , medicare_id
    , create_dt
    , substring(notif_recd_dttm, 1, 10) as notif_date
    , substring(notif_yrmonth, 1, 4) as notif_year
    , notif_yrmonth
    , prim_srvc_sub_cat
    , prim_diag_cd
    , entity
    , case_category_cd
    , channel_cd
    , proc_cd
    , prim_srvc_cat
    , prim_proc_ind
    , pa_program
    , case_decn_stat_cd
    , business_segment
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
	, case when case_decn_stat_cd = 'PD-Partially Adverse Determination' then 1 
		else 0 
	end as partially_adverse
	, case when case_decn_stat_cd = 'AD-Fully Adverse Determination' or case_decn_stat_cd = 'AD - Fully Adverse Determination' then 1 
		else 0 
	end as fully_adverse	
from hce_proj_bd.hce_adr_avtar_like_2023_f as a
where business_segment not in ('EnI','ERR','null')
	and medicare_id is not null
	and proc_cd in ('A0425','A0428')
	and notif_yrmonth between '202301' and '202312';


drop table tmp_1m.kn_nemt_2024_2025;
create table tmp_1m.kn_nemt_2024_2025 stored as orc as
select
    case_id
    , medicare_id
    , create_dt
    , substring(notif_recd_dttm, 1, 10) as notif_date
    , substring(notif_yrmonth, 1, 4) as notif_year
    , notif_yrmonth
    , prim_srvc_sub_cat
    , prim_diag_cd
    , entity
    , case_category_cd
    , channel_cd
    , proc_cd
    , prim_srvc_cat
    , prim_proc_ind
    , pa_program
    , case_decn_stat_cd
    , business_segment
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
	, case when case_decn_stat_cd = 'PD-Partially Adverse Determination' then 1 
		else 0 
	end as partially_adverse
	, case when case_decn_stat_cd = 'AD-Fully Adverse Determination' or case_decn_stat_cd = 'AD - Fully Adverse Determination' then 1 
		else 0 
	end as fully_adverse
	, case when business_segment = 'MnR' and fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and tfm_include_flag = 1 and fin_product_level_3 <> 'INSTITUTIONAL' then 1 
		else 0 
	end as mnr_cosmos_ffs_flag
	, case when business_segment = 'MnR' and fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type = 'FFS' then 1 
		else 0 
	end as mnr_nice_ffs_flag
	, case when (business_segment = 'MnR' and fin_brand = 'M&R' and global_cap = 'NA' and sgr_source_name = 'COSMOS' and tfm_include_flag = 1 and fin_product_level_3 <> 'INSTITUTIONAL') 
       		 or (business_segment = 'MnR' and fin_brand = 'M&R' and sgr_source_name = 'NICE' and nce_tadm_dec_risk_type in ('FFS')) then 1 
		else 0 
	end as mnr_total_ffs_flag
	, case when (substring(notif_yrmonth, 1, 4) = '2024' and business_segment = 'CnS' and fin_brand in ('M&R','C&S') and global_cap = 'NA' and sgr_source_name in ('COSMOS','CSP') and migration_source = 'OAH' and fin_state = 'MD') then 0 
		when (business_segment = 'MnR' and fin_brand = 'M&R' and migration_source = 'OAH')
		  or (business_segment = 'CnS' and fin_brand = 'C&S' and migration_source = 'OAH') then 1 
		else 0 
	end as oah_flag
	, case when 
		(
			(business_segment = 'CnS' and fin_brand in ('M&R','C&S') and migration_source <> 'OAH' and global_cap = 'NA' and fin_product_level_3 = 'DUAL' 
			 and sgr_source_name in ('COSMOS','CSP') and fin_state not in ('OK','NC','NM','NV','OH','TX')) 
			or 
			(substring(notif_yrmonth, 1, 4) = '2024' and business_segment = 'CnS' and fin_brand in ('M&R','C&S') and global_cap = 'NA' 
			 and sgr_source_name in ('COSMOS','CSP') and migration_source = 'OAH' and fin_state = 'MD')
		) then 1 
		else 0 
	end as cns_dual_flag
	, case when business_segment = 'MnR' and fin_brand = 'M&R' and fin_product_level_3 = 'DUAL' then 1 
		else 0 
	end as mnr_dual_flag
	, case when business_segment = 'MnR' and fin_brand = 'M&R' and fin_product_level_3 = 'INSTITUTIONAL' then 1 
		else 0 
	end as isnp_flag
from hce_proj_bd.hce_adr_avtar_like_24_25_f as a
where business_segment not in ('EnI','ERR','null')
	and medicare_id is not null
	and proc_cd in ('A0425','A0428')
	and notif_yrmonth between '202401' and '202512'
	and fin_plan_level_2 != 'PFFS'
;


describe tmp_1m.kn_nemt_2024_2025;

drop table tmp_1m.kn_nemt_2024_2025_sum;
create table tmp_1m.kn_nemt_2024_2025_sum as
select
    case_id
    , medicare_id
    , create_dt
    , notif_date
    , notif_year
    , notif_yrmonth
    , prim_srvc_sub_cat
    , prim_diag_cd
    , entity
    , case_category_cd
    , channel_cd
    , proc_cd
    , prim_srvc_cat
    , prim_proc_ind
    , pa_program
    , case_decn_stat_cd
    , business_segment
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
    , partially_adverse
    , fully_adverse
    , mnr_cosmos_ffs_flag
    , mnr_nice_ffs_flag
    , mnr_total_ffs_flag
    , oah_flag
    , cns_dual_flag
    , mnr_dual_flag
    , isnp_flag
    , case 
    	when mnr_total_ffs_flag = 1 then 'M&R FFS'
    	when oah_flag = 1 then 'OAH'
    	when cns_dual_flag = 1 then 'C&S Dual'
    	when mnr_dual_flag = 1 then 'M&R Dual'
    	when isnp_flag = 1 then 'ISNP'
    	else 'Others'
    end as Population
	, count(case_id) as n_auth
	, count(concat_ws("-", case_id, notif_date, proc_cd)) as n_distinct_auth
from tmp_1m.kn_nemt_2024_2025
group by
    case_id
    , medicare_id
    , create_dt
    , notif_date
    , notif_year
    , notif_yrmonth
    , prim_srvc_sub_cat
    , prim_diag_cd
    , entity
    , case_category_cd
    , channel_cd
    , proc_cd
    , prim_srvc_cat
    , prim_proc_ind
    , pa_program
    , case_decn_stat_cd
    , business_segment
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
    , partially_adverse
    , fully_adverse
    , mnr_cosmos_ffs_flag
    , mnr_nice_ffs_flag
    , mnr_total_ffs_flag
    , oah_flag
    , cns_dual_flag
    , mnr_dual_flag
    , isnp_flag
	, case 
    	when mnr_total_ffs_flag = 1 then 'M&R FFS'
    	when oah_flag = 1 then 'OAH'
    	when cns_dual_flag = 1 then 'C&S Dual'
    	when mnr_dual_flag = 1 then 'M&R Dual'
    	when isnp_flag = 1 then 'ISNP'
		else 'Others'
	end
	;

-- COSMOS
 drop table if exists tmp_1m.kn_nemt_cosmos_claims;
 create table tmp_1m.kn_nemt_cosmos_claims as
 select
 	'COSMOS' as entity
 	, clm_aud_nbr as clm_id
 	, gal_mbi_hicn_fnl as mbi
     , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
 	, component
 	, service_code
	, case when prov_prtcp_sts_cd in ('P', 'C') then 'PAR' 
			when prov_prtcp_sts_cd in ('N', 'D') then 'Non-PAR'
	end as prov_parstatus
     , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
     , primary_diag_cd, icd_2, icd_3, icd_4
     , fst_srvc_month
     , fst_srvc_year
 	, global_cap
 	, clm_dnl_f
     , market_fnl
     , brand_fnl
     , group_ind_fnl
     , tfm_include_flag
     , migration_source
     , tfm_product_new_fnl
     , product_level_3_fnl
     , case when (brand_fnl = 'M&R' and migration_source = 'OAH') then 'M&R OAH'
 	       when (brand_fnl = 'C&S' and migration_source = 'OAH') then 'C&S OAH'
 	       when (brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL') then 'M&R ISNP'
 	       else brand_fnl 
 	end as entity1
 	, allw_amt_fnl as allw
 	, net_pd_amt_fnl as pd
 from fichsrv.cosmos_pr
 where (proc_1_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215') 
 	or proc_2_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
 	or proc_3_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215') 
 	or proc_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
 	)
 	and fst_srvc_month >= '202401'
 	and brand_fnl in ('M&R', 'C&S')
 	and global_cap = 'NA'
 union all
 select
 	'COSMOS' as entity
 	, clm_aud_nbr as clm_id
 	, gal_mbi_hicn_fnl as mbi
     , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
 	, component
 	, hce_service_code as service_code
	, case when prov_prtcp_sts_cd in ('P', 'C') then 'PAR' 
		when prov_prtcp_sts_cd in ('N', 'D') then 'Non-PAR'
	end as prov_parstatus
     , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
     , primary_diag_cd, icd_2, icd_3, icd_4
     , hce_month as fst_srvc_month
     , fst_srvc_year
 	, global_cap
 	, clm_dnl_f
     , market_fnl, brand_fnl
     , group_ind_fnl
     , tfm_include_flag
     , migration_source
     , tfm_product_new_fnl
     , product_level_3_fnl
 	, case when (brand_fnl = 'M&R' and migration_source = 'OAH') then 'M&R OAH'
 	       when (brand_fnl = 'C&S' and migration_source = 'OAH') then 'C&S OAH'
 	       when (brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL') then 'M&R ISNP'
 	       else brand_fnl 
 	end as entity1
 	, allw_amt_fnl as allw
 	, net_pd_amt_fnl as pd
 from fichsrv.cosmos_op
 where (proc_1_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215') 
 	or proc_2_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
 	or proc_3_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215') 
 	or proc_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
 	)
 	and hce_month >= '202401'
 	and brand_fnl in ('M&R', 'C&S')
 	and global_cap = 'NA'
 ;
 
 
 -- CSP
 drop table if exists tmp_1m.kn_nemt_csp_claims;
 create table tmp_1m.kn_nemt_csp_claims as
 select
 	'CSP' as entity
 	, clm_aud_nbr as clm_id
 	, gal_mbi_hicn_fnl as mbi
     , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
 	, component
 	, service_code
 	, case when prov_prtcp_sts_cd in ('P', 'C') then 'PAR' 
			when prov_prtcp_sts_cd in ('N', 'D') then 'Non-PAR'
	end as prov_parstatus
     , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
     , primary_diag_cd, icd_2, icd_3, icd_4
     , fst_srvc_month
     , fst_srvc_year
 	, global_cap
 	, clm_dnl_f
     , market_fnl, brand_fnl
     , group_ind_fnl
     , tfm_include_flag
     , migration_source
     , tfm_product_fnl as tfm_product_new_fnl
     , product_level_3_fnl
    	, case when migration_source = 'OAH' then 'C&S OAH'
 	       else 'C&S DSNP' 
 	end as entity1
 	, allw_amt_fnl as allw
 	, net_pd_amt_fnl as pd
 from tadm_tre_cpy.dcsp_pr_f_202506
 where (proc_1_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215') 
 	or proc_2_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
 	or proc_3_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215') 
 	or proc_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
 	)
 	and fst_srvc_month >= '202401'
 	and brand_fnl = 'C&S' 
 	and global_cap = 'NA'
 union all
 select
 	'CSP' as entity
 	, clm_aud_nbr as clm_id
 	, gal_mbi_hicn_fnl as mbi
     , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
 	, component
 	, hce_service_code as service_code
 	, case when prov_prtcp_sts_cd in ('P', 'C') then 'PAR' 
			when prov_prtcp_sts_cd in ('N', 'D') then 'Non-PAR'
	end as prov_parstatus
     , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
     , primary_diag_cd, icd_2, icd_3, icd_4
     , fst_srvc_month
     , fst_srvc_year
 	, global_cap
 	, clm_dnl_f
     , market_fnl, brand_fnl
     , group_ind_fnl
     , tfm_include_flag
     , migration_source
     , tfm_product_fnl as tfm_product_new_fnl
     , product_level_3_fnl
    	, case when migration_source = 'OAH' then 'C&S OAH'
 	       else 'C&S DSNP' 
 	end as entity1
 	, allw_amt_fnl as allw
 	, net_pd_amt_fnl as pd
 from tadm_tre_cpy.dcsp_op_f_202506
 where (proc_1_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215') 
 	or proc_2_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
 	or proc_3_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215') 
 	or proc_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
 	)
 	and fst_srvc_month >= '202401'
 	and brand_fnl = 'C&S' 
 	and global_cap = 'NA'
 ;
 
 
 -- NICE
 drop table if exists tmp_1m.kn_nemt_nice_claims;
 create table tmp_1m.kn_nemt_nice_claims as
 select
 	'NICE' as entity
 	, clm_aud_nbr as clm_id
 	, mbi_hicn_fnl as mbi
     , concat(mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
 	, component
 	, service_code
 	, case when prov_prtcp_sts_cd in ('P', 'C') then 'PAR' 
			when prov_prtcp_sts_cd in ('N', 'D') then 'Non-PAR'
	end as prov_parstatus
     , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
     , primary_diag_cd, icd_2, icd_3, icd_4
     , fst_srvc_month
     , fst_srvc_year
 	, clm_cap_flag as global_cap
 	, clm_dnl_f
     , market_fnl, brand_fnl
     , group_ind_fnl
     , tfm_include_flag
     , 'NA' as migration_source
     , tfm_product_fnl as tfm_product_new_fnl
     , product_level_3_fnl
 	, brand_fnl as entity1
 	, allw_amt_fnl as allw
 	, net_pd_amt_fnl as pd
 from fichsrv.nice_pr
 where (proc_1_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215') 
 	or proc_2_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
 	or proc_3_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215') 
 	or proc_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
 	)
 	and fst_srvc_month >= '202401'
 	and brand_fnl = 'M&R'
 	and clm_cap_flag = 'FFS'
 union all
 select
 	'NICE' as entity
 	, clm_aud_nbr as clm_id
 	, mbi_hicn_fnl as mbi
     , concat(mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
 	, component
 	, hce_service_code as service_code
 	, case when prov_prtcp_sts_cd in ('P', 'C') then 'PAR' 
			when prov_prtcp_sts_cd in ('N', 'D') then 'Non-PAR'
	end as prov_parstatus
     , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
     , primary_diag_cd, icd_2, icd_3, icd_4
     , fst_srvc_month
     , fst_srvc_year
 	, clm_cap_flag as global_cap
 	, clm_dnl_f
     , market_fnl, brand_fnl
     , group_ind_fnl
     , tfm_include_flag
     , 'NA' as migration_source
     , tfm_product_fnl as tfm_product_new_fnl
     , product_level_3_fnl
     , brand_fnl as entity1
 	, allw_amt_fnl as allw
 	, net_pd_amt_fnl as pd
 from fichsrv.nice_op
 where (proc_1_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215') 
 	or proc_2_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
 	or proc_3_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215') 
 	or proc_cd in ('A0080', 'A0090', 'A0100', 'A0110', 'A0120', 'A0130', 'A0140', 'A0160', 'A0170', 'A0180', 'A0190', 'A0200', 'A0210', 'A0425', 'A0426', 'A0428', 'S0215')
 	)
 	and fst_srvc_month >= '202401'
 	and brand_fnl = 'M&R'
 	and clm_cap_flag = 'FFS'
 ;
 
 drop table if exists tmp_1m.kn_nemt_cosmos_csp_nice_claims;
 create table tmp_1m.kn_nemt_cosmos_csp_nice_claims as
 select
 	entity
 	, entity1
 	, case 
 		when entity1 = 'M&R' then 'M&R FFS'
 		when entity1 in ('C&S', 'C&S DSNP') then 'C&S DSNP'
 		when entity1 in ('C&S OAH', 'M&R OAH') then 'OAH'
 		when entity1 = 'M&R ISNP' then 'ISNP'
 	end as Population
 	, clm_id
 	, mbi
 	, unique_id
 	, component
 	, service_code
 	, prov_parstatus
     , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
     , primary_diag_cd, icd_2, icd_3, icd_4
 	, fst_srvc_month
     , fst_srvc_year
 	, global_cap
 	, clm_dnl_f
     , market_fnl
     , brand_fnl
     , group_ind_fnl
     , tfm_include_flag
     , migration_source
     , tfm_product_new_fnl
     , product_level_3_fnl
     , allw
     , pd
 from tmp_1m.kn_nemt_cosmos_claims
 union all
 select 
 	entity
 	, entity1
 	, case 
 		when entity1 = 'M&R' then 'M&R FFS'
 		when entity1 in ('C&S', 'C&S DSNP') then 'C&S DSNP'
 		when entity1 in ('C&S OAH', 'M&R OAH') then 'OAH'
 		when entity1 = 'M&R ISNP' then 'ISNP'
 	end as Population
 	, clm_id
 	, mbi
 	, unique_id
 	, component
 	, service_code
 	, prov_parstatus
     , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
     , primary_diag_cd, icd_2, icd_3, icd_4
 	, fst_srvc_month
     , fst_srvc_year
 	, global_cap
 	, clm_dnl_f
     , market_fnl
     , brand_fnl
     , group_ind_fnl
     , tfm_include_flag
     , migration_source
     , tfm_product_new_fnl
     , product_level_3_fnl
     , allw
     , pd
 from tmp_1m.kn_nemt_csp_claims
 union all
 select 
 	entity
 	, entity1
 	, case 
 		when entity1 = 'M&R' then 'M&R FFS'
 		when entity1 in ('C&S', 'C&S DSNP') then 'C&S DSNP'
 		when entity1 in ('C&S OAH', 'M&R OAH') then 'OAH'
 		when entity1 = 'M&R ISNP' then 'ISNP'
 	end as Population
 	, clm_id
 	, mbi
 	, unique_id
 	, component
 	, service_code
 	, prov_parstatus
     , proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
     , primary_diag_cd, icd_2, icd_3, icd_4
 	, fst_srvc_month
     , fst_srvc_year
 	, global_cap
 	, clm_dnl_f
     , market_fnl
     , brand_fnl
     , group_ind_fnl
     , tfm_include_flag
     , migration_source
     , tfm_product_new_fnl
     , product_level_3_fnl
     , allw
     , pd
 from tmp_1m.kn_nemt_nice_claims
 ;
 
select count(*) from tmp_1m.kn_nemt_cosmos_csp_nice_claims_v2;

-- No global_cap, clm_dnl_f = 'N';

drop table tmp_1m.kn_nemt_cosmos_csp_nice_claims_v2;
create table tmp_1m.kn_nemt_cosmos_csp_nice_claims_v2 as
select distinct
	entity
	, entity1
	, Population
	, clm_id
	, mbi
	, unique_id
	, component
	, service_code
	, prov_parstatus
	, proc_cd, proc_1_cd, proc_2_cd, proc_3_cd
	, primary_diag_cd, icd_2, icd_3, icd_4
	, fst_srvc_month
	, fst_srvc_year
	, global_cap
	, clm_dnl_f
	, market_fnl
	, brand_fnl
	, group_ind_fnl
	, tfm_include_flag
	, migration_source
	, tfm_product_new_fnl
	, product_level_3_fnl
	, allw
	, pd
from tmp_1m.kn_nemt_cosmos_csp_nice_claims
where global_cap = 'NA'
	and clm_dnl_f = 'N'
;

 
drop table tmp_1m.kn_nemt_cosmos_csp_nice_claims_summary;
create table tmp_1m.kn_nemt_cosmos_csp_nice_claims_summary as
select 
	entity
	, entity1
	, Population
	, component
	, service_code
	, prov_parstatus
	, proc_cd
	, primary_diag_cd
	, fst_srvc_month
	, fst_srvc_year
	, global_cap
	, clm_dnl_f
	, market_fnl
	, brand_fnl
	, group_ind_fnl
	, tfm_include_flag
	, migration_source
	, tfm_product_new_fnl
	, product_level_3_fnl
	, sum(allw) as allowed
	, sum(pd) as paid
	, count(distinct clm_id) as n_distinct_clm_id
	, count(clm_id) as n_clm_id
	, count(unique_id) as n_id
	, count(distinct unique_id) as n_distinct_id
from tmp_1m.kn_nemt_cosmos_csp_nice_claims
where clm_dnl_f = 'N'
	and global_cap = 'NA'
group by 
	entity
	, entity1
	, Population
	, component
	, service_code
	, prov_parstatus
	, proc_cd
	, primary_diag_cd
	, fst_srvc_month
	, fst_srvc_year
	, global_cap
	, clm_dnl_f
	, market_fnl
	, brand_fnl
	, group_ind_fnl
	, tfm_include_flag
	, migration_source
	, tfm_product_new_fnl
	, product_level_3_fnl
;

select count(*) from tmp_1m.kn_nemt_cosmos_csp_nice_claims_summary;
