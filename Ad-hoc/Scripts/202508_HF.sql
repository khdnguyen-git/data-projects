-- Find mm with HF in PR, for mbi list;
-- HF: ('I501','I5020','I5021','I5022','I5023','I5030','I5031','I5032','I5033','I5040','I5041','I5042','I5043','I50810','I50811','I50812','I50813','I50814','I5082','I5083','I5084','I5089','I509')

use ving_prd_trend_db.tmp_1m; -- Snowflake specific query to initialize DB

-- COSMOS
drop table if exists tmp_1m.kn_hf_COSMOS_claims_pr;
create table tmp_1m.kn_hf_COSMOS_claims_pr as
select
    'COSMOS' as entity
    , clm_aud_nbr as clm_id
    , gal_mbi_hicn_fnl as mbi
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
    , component
    , service_code
    , proc_cd 
    , primary_diag_cd
    , icd_2
    , icd_3
    , icd_4
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
    , case 
        when brand_fnl = 'M&R' and migration_source = 'OAH' then 'M&R OAH'
        when brand_fnl = 'C&S' and migration_source = 'OAH' then 'C&S OAH'
        when brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL' then 'M&R ISNP'
        else brand_fnl 
      end as entity1
    , allw_amt_fnl as allw
    , net_pd_amt_fnl as pd
    , case 
        when primary_diag_cd in ('I501','I5020','I5021','I5022','I5023','I5030','I5031','I5032','I5033','I5040','I5041','I5042','I5043','I50810','I50811','I50812','I50813','I50814','I5082','I5083','I5084','I5089','I509')
          or icd_2 in ('I501','I5020','I5021','I5022','I5023','I5030','I5031','I5032','I5033','I5040','I5041','I5042','I5043','I50810','I50811','I50812','I50813','I50814','I5082','I5083','I5084','I5089','I509')
          or icd_3 in ('I501','I5020','I5021','I5022','I5023','I5030','I5031','I5032','I5033','I5040','I5041','I5042','I5043','I50810','I50811','I50812','I50813','I50814','I5082','I5083','I5084','I5089','I509')
          or icd_4 in ('I501','I5020','I5021','I5022','I5023','I5030','I5031','I5032','I5033','I5040','I5041','I5042','I5043','I50810','I50811','I50812','I50813','I50814','I5082','I5083','I5084','I5089','I509')
        then 1 else 0 
      end as hf
from fichsrv.COSMOS_pr
where fst_srvc_month >= '202301'
  and brand_fnl in ('M&R', 'C&S')
  and global_cap = 'NA'
;

-- SMART
drop table if exists tmp_1m.kn_hf_SMART_claims_pr;
create table tmp_1m.kn_hf_SMART_claims_pr as
select
    'SMART' as entity
    , clm_aud_nbr as clm_id
    , gal_mbi_hicn_fnl as mbi
    , concat(gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
    , component
    , service_code
    , proc_cd 
    , primary_diag_cd
    , icd_2
    , icd_3
    , icd_4
    , fst_srvc_month
    , fst_srvc_year
    , global_cap
    , clm_dnl_f
    , market_fnl
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , migration_source
    , tfm_product_fnl as tfm_product_new_fnl
    , product_level_3_fnl
    , case 
        when brand_fnl = 'M&R' and migration_source = 'OAH' then 'M&R OAH'
        when brand_fnl = 'C&S' and migration_source = 'OAH' then 'C&S OAH'
        when brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL' then 'M&R ISNP'
        else brand_fnl 
      end as entity1
    , allw_amt_fnl as allw
    , net_pd_amt_fnl as pd
    , case 
        when primary_diag_cd in ('I501','I5020','I5021','I5022','I5023','I5030','I5031','I5032','I5033','I5040','I5041','I5042','I5043','I50810','I50811','I50812','I50813','I50814','I5082','I5083','I5084','I5089','I509')
          or icd_2 in ('I501','I5020','I5021','I5022','I5023','I5030','I5031','I5032','I5033','I5040','I5041','I5042','I5043','I50810','I50811','I50812','I50813','I50814','I5082','I5083','I5084','I5089','I509')
          or icd_3 in ('I501','I5020','I5021','I5022','I5023','I5030','I5031','I5032','I5033','I5040','I5041','I5042','I5043','I50810','I50811','I50812','I50813','I50814','I5082','I5083','I5084','I5089','I509')
          or icd_4 in ('I501','I5020','I5021','I5022','I5023','I5030','I5031','I5032','I5033','I5040','I5041','I5042','I5043','I50810','I50811','I50812','I50813','I50814','I5082','I5083','I5084','I5089','I509')
        then 1 else 0 
      end as hf
from tadm_tre_cpy.dcsp_pr_f_202507
where fst_srvc_month >= '202301'
  and brand_fnl in ('M&R', 'C&S')
  and global_cap = 'NA'
;

-- NICE 
drop table if exists tmp_1m.kn_hf_NICE_claims_pr;
create table tmp_1m.kn_hf_NICE_claims_pr as
select
    'NICE' as entity
    , clm_aud_nbr as clm_id
    , mbi_hicn_fnl as mbi
    , concat(mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
    , component
    , service_code
    , proc_cd 
    , primary_diag_cd
    , icd_2
    , icd_3
    , icd_4
    , fst_srvc_month
    , fst_srvc_year
	, clm_cap_flag as global_cap
    , clm_dnl_f
    , market_fnl
    , brand_fnl
    , group_ind_fnl
    , tfm_include_flag
    , 'NA' as migration_source
    , tfm_product_fnl as tfm_product_new_fnl
    , product_level_3_fnl
    , case 
        when brand_fnl = 'M&R' and migration_source = 'OAH' then 'M&R OAH'
        when brand_fnl = 'C&S' and migration_source = 'OAH' then 'C&S OAH'
        when brand_fnl = 'M&R' and product_level_3_fnl = 'INSTITUTIONAL' then 'M&R ISNP'
        else brand_fnl 
      end as entity1
    , allw_amt_fnl as allw
    , net_pd_amt_fnl as pd
    , case 
        when primary_diag_cd in ('I501','I5020','I5021','I5022','I5023','I5030','I5031','I5032','I5033','I5040','I5041','I5042','I5043','I50810','I50811','I50812','I50813','I50814','I5082','I5083','I5084','I5089','I509')
          or icd_2 in ('I501','I5020','I5021','I5022','I5023','I5030','I5031','I5032','I5033','I5040','I5041','I5042','I5043','I50810','I50811','I50812','I50813','I50814','I5082','I5083','I5084','I5089','I509')
          or icd_3 in ('I501','I5020','I5021','I5022','I5023','I5030','I5031','I5032','I5033','I5040','I5041','I5042','I5043','I50810','I50811','I50812','I50813','I50814','I5082','I5083','I5084','I5089','I509')
          or icd_4 in ('I501','I5020','I5021','I5022','I5023','I5030','I5031','I5032','I5033','I5040','I5041','I5042','I5043','I50810','I50811','I50812','I50813','I50814','I5082','I5083','I5084','I5089','I509')
        then 1 else 0 
      end as hf
from fichsrv.NICE_pr
where fst_srvc_month >= '202301'
  and brand_fnl in ('M&R', 'C&S')
  and global_cap = 'NA'
;

drop table if exists tmp_1m.kn_hf_COSMOS_SMART_NICE_claims_pr;
create table tmp_1m.kn_hf_COSMOS_SMART_NICE_claims_pr as
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
    , proc_cd
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
    , hf
    , allw
    , pd
from tmp_1m.kn_hf_COSMOS_claims_pr
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
    , proc_cd
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
    , hf
    , allw
    , pd
from tmp_1m.kn_hf_SMART_claims_pr
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
    , proc_cd
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
    , hf
    , allw
    , pd
from tmp_1m.kn_hf_NICE_claims_pr
;

select count(*) from tmp_1m.kn_hf_COSMOS_SMART_NICE_claims_pr;
-- 1,002,916,189

select count(*) from tmp_1m.kn_hf_COSMOS_SMART_NICE_claims_pr where hf = 1;
-- 28,638,894

select count(distinct mbi) from tmp_1m.kn_hf_COSMOS_SMART_NICE_claims_pr;
-- 10,196,372

select count(distinct mbi) from tmp_1m.kn_hf_COSMOS_SMART_NICE_claims_pr where hf = 1;
-- 1,416,991

drop table if exists tmp_1m.kn_hf_mbi_COSMOS_SMART_NICE_claims;
create table tmp_1m.kn_hf_mbi_COSMOS_SMART_NICE_claims as
select 
	a.mbi
    , b.entity
    , b.entity1
    , b.population
    , b.clm_id
    , b.unique_id
    , b.component
    , b.service_code
    , b.proc_cd
    , b.primary_diag_cd
    , b.icd_2
    , b.icd_3
    , b.icd_4
    , b.fst_srvc_month
    , b.fst_srvc_year
    , b.global_cap
    , b.clm_dnl_f
    , b.market_fnl
    , b.brand_fnl
    , b.group_ind_fnl
    , b.tfm_include_flag
    , b.migration_source
    , b.tfm_product_new_fnl
    , b.product_level_3_fnl
    , b.hf
    , b.allw
    , b.pd
from tmp_1m.kn_hf_mbi as a
left join tmp_1m.kn_hf_COSMOS_SMART_NICE_claims_pr as b
	on a.mbi = b.mbi
;

select count(distinct mbi) from tmp_1m.kn_hf_mbi_COSMOS_SMART_NICE_claims;
-- 76,602

select count(distinct mbi) from tmp_1m.kn_hf_mbi_COSMOS_SMART_NICE_claims where hf = 1;
-- 18,132

select count(distinct mbi) from tmp_1m.kn_hf_mbi_COSMOS_SMART_NICE_claims 
where primary_diag_cd in ('I501','I5020','I5021','I5022','I5023','I5030','I5031','I5032','I5033','I5040','I5041','I5042','I5043','I50810','I50811','I50812','I50813','I50814','I5082','I5083','I5084','I5089','I509')
-- 11,501

select * from tmp_1m.kn_hf_mbi_COSMOS_SMART_NICE_claims;

drop table if exists tmp_1m.kn_hf1_mbi_COSMOS_SMART_NICE_claims;
create table tmp_1m.kn_hf1_mbi_COSMOS_SMART_NICE_claims as
select 
	entity
	, entity1
	, population
	, clm_id
	, mbi
	, unique_id
	, component
	, service_code
    , proc_cd
    , primary_diag_cd, icd_2, icd_3, icd_4
    , hf
    , case when primary_diag_cd in ('I501','I5020','I5021','I5022','I5023','I5030','I5031','I5032','I5033','I5040','I5041','I5042','I5043','I50810','I50811',
    		'I50812','I50813','I50814','I5082','I5083','I5084','I5089','I509') 
    	then 1
    	else 0
    end as hf_primarydx			
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
from tmp_1m.kn_hf_mbi_COSMOS_SMART_NICE_claims 
where hf = 1;

select count(distinct mbi) from tmp_1m.kn_hf1_mbi_COSMOS_SMART_NICE_claims; -- 18,132
select count(distinct mbi) from tmp_1m.kn_hf1_mbi_COSMOS_SMART_NICE_claims where hf_primarydx = 1; -- 11,501

-- All Membership;
create table tmp_1m.kn_hf_membership as
with mnr_membership as (
select
	'COSMOS' as entity_source
	, fin_mbi_hicn_fnl
	, fin_inc_month
	, fin_brand
	, fin_market
	, fin_state
	, fin_g_i 
	, fin_product_level_3
	, fin_tfm_product_new
	, migration_source
	, global_cap
	, nce_tadm_dec_risk_type
	, case when migration_source = 'OAH' then 'M&R OAH'
	       when fin_product_level_3 = 'INSTITUTIONAL' then 'M&R ISNP'
	       else 'M&R FFS' 
	end as entity
	, case when fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD' then 2
	       when  migration_source = 'OAH' then 1 else 0 
	end as OAH_flag
	, 0 as CnS_Dual_flag
	, sum(fin_member_cnt) as mm 
from fichsrv.tre_membership
where 
	sgr_source_name = 'COSMOS'
	and fin_brand = 'M&R'
	and global_cap = 'NA'
	and fin_inc_month >= '202301'
group by
	fin_mbi_hicn_fnl
	, fin_inc_month
	, fin_brand
	, fin_market
	, fin_state
	, fin_g_i 
	, fin_product_level_3
	, fin_tfm_product_new
	, migration_source
	, global_cap
	, nce_tadm_dec_risk_type
	, case when migration_source = 'OAH' then 'M&R OAH'
	       when fin_product_level_3 = 'INSTITUTIONAL' then 'M&R ISNP'
	       else 'M&R FFS' 
	end 
	, case when fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD' then 2
	       when  migration_source = 'OAH' then 1 
	       else 0 
	end
),
cns_membership as (
select
	'COSMOS' as entity_source
	, fin_mbi_hicn_fnl
	, fin_inc_month
	, fin_brand
	, fin_market
	, fin_state
	, fin_g_i 
	, fin_product_level_3
	, fin_tfm_product_new
	, migration_source
	, global_cap
	, nce_tadm_dec_risk_type
	, case when migration_source = 'OAH' then 'OAH'
	      else 'C&S DSNP' 
	end as entity
	, case when fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD' then 2
	       when migration_source = 'OAH' then 1 else 0 
	end as OAH_flag
	, case when 
		   ((migration_source <> 'OAH' and fin_product_level_3 = 'DUAL' and fin_state not in ('OK','NC','NM','NV','OH','TX')) 
		or (fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD')) then 1 else 0 
	end as CnS_Dual_flag
	, sum(fin_member_cnt) as mm 
from fichsrv.tre_membership
where 
	sgr_source_name = 'COSMOS'
	and fin_brand = 'C&S'
	and global_cap = 'NA'
	and fin_inc_month >= '202301'
group by
	fin_mbi_hicn_fnl
	, fin_inc_month
	, fin_brand
	, fin_market
	, fin_state
	, fin_g_i 
	, fin_product_level_3
	, fin_tfm_product_new
	, migration_source
	, global_cap
	, nce_tadm_dec_risk_type
	, case when migration_source = 'OAH' then 'OAH'
	      else 'C&S DSNP' 
	end
	, case when fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD' then 2
	       when migration_source = 'OAH' then 1 
	       else 0 
	end
	, case when 
		   ((migration_source <> 'OAH' and fin_product_level_3 = 'DUAL' and fin_state not in ('OK','NC','NM','NV','OH','TX')) 
		or (fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD')) then 1 
		else 0 
	end
),
SMART_membership as (
select
	'SMART' as entity_source
	, fin_mbi_hicn_fnl
	, fin_inc_month
	, fin_brand
	, fin_market
	, fin_state
	, fin_g_i 
	, fin_product_level_3
	, fin_tfm_product_new
	, migration_source
	, global_cap 
	, nce_tadm_dec_risk_type
	, case when migration_source = 'OAH' then 'OAH'
	      else 'C&S DSNP' 
	end as entity
	, case when fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD' then 2
	       when migration_source = 'OAH' then 1 
	       else 0 
	end as OAH_flag
	, case when 
		   ((migration_source <> 'OAH' and fin_product_level_3 = 'DUAL' and fin_state not in ('OK','NC','NM','NV','OH','TX')) 
		or (fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD')) then 1 
			else 0 
	end as CnS_Dual_flag
	, sum(fin_member_cnt) as mm
from fichsrv.tre_membership 
where		
	fin_brand = 'C&S'	
	and sgr_source_name = 'SMART'
	and global_cap = 'NA'
	and fin_inc_month >= '202301'
group by
	fin_mbi_hicn_fnl
	, fin_inc_month
	, fin_brand
	, fin_market
	, fin_state
	, fin_g_i 
	, fin_product_level_3
	, fin_tfm_product_new
	, migration_source
	, global_cap
	, nce_tadm_dec_risk_type
	, case when migration_source = 'OAH' then 'OAH'
	      else 'C&S DSNP' 
	end
	, case when fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD' then 2
	       when migration_source = 'OAH' then 1 else 0 
	end
	, case when 
		   ((migration_source <> 'OAH' and fin_product_level_3 = 'DUAL' and fin_state not in ('OK','NC','NM','NV','OH','TX')) 
		or (fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD')) then 1 else 0 
	end
),
NICE_membership as (
select
	'NICE' as entity_source
	, fin_mbi_hicn_fnl
	, fin_inc_month
	, fin_brand
	, fin_market
	, fin_state
	, fin_g_i 
	, fin_product_level_3
	, fin_tfm_product_new
	, migration_source
	, global_cap 
	, nce_tadm_dec_risk_type
	, 'M&R FFS' as entity
	, case when fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD' then 2
	       when  migration_source = 'OAH' then 1 
	       else 0 
	end as OAH_flag
	, 0 as CnS_Dual_flag
	, sum(fin_member_cnt) as mm 
from fichsrv.tre_membership
where 
	fin_brand = 'M&R'	
	and sgr_source_name = 'NICE'
	and nce_tadm_dec_risk_type = 'FFS'
	and fin_inc_month >= '202301'
group by
	fin_mbi_hicn_fnl
	, fin_inc_month
	, fin_brand
	, fin_market
	, fin_state
	, fin_g_i 
	, fin_product_level_3
	, fin_tfm_product_new
	, migration_source
	, global_cap 
	, nce_tadm_dec_risk_type
	, case when fin_inc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD' then 2
	       when  migration_source = 'OAH' then 1 else 0 
	end
)
select * from mnr_membership
union all
select * from cns_membership
union all
select * from SMART_membership
union all
select * from NICE_membership
;


select count(*) from tmp_1m.kn_hf_membership; -- 219533511

select * from tmp_1m.kn_hf_membership
limit 2;

select * from tmp_1m.kn_hf_COSMOS_SMART_NICE_claims_pr
limit 2;

drop table tmp_1m.kn_hf_claim_mm;
create table tmp_1m.kn_hf_claim_mm as
select
	a.entity
	, a.entity1
	, a.population
	, a.clm_id
	, a.mbi
	, a.unique_id
	, a.component
	, a.service_code
	, a.proc_cd
	, a.primary_diag_cd
	, a.fst_srvc_month
	, a.fst_srvc_year
	, a.global_cap
	, a.clm_dnl_f
	, a.market_fnl
	, a.brand_fnl
	, a.group_ind_fnl
	, a.tfm_include_flag
	, a.migration_source
	, a.product_level_3_fnl
	, a.allw
	, a.pd
	, b.mm
from tmp_1m.kn_hf_COSMOS_SMART_NICE_claims_pr as a 
left join tmp_1m.kn_hf_membership as b
on a.mbi = b.fin_mbi_hicn_fnl 
	and a.fst_srvc_month = b.fin_inc_month
	and a.market_fnl = b.fin_market
	and a.global_cap = b.global_cap
;

select count(*) from tmp_1m.kn_hf_claim_mm;

drop table tmp_1m.kn_hf_claim_mm_sum;
create table tmp_1m.kn_hf_claim_mm_sum as
select 
	a.entity
	, a.population
	, a.unique_id
	, a.component
	, a.service_code
	, a.proc_cd
	, a.primary_diag_cd
	, a.fst_srvc_month
	, a.fst_srvc_year
	, a.global_cap
	, a.clm_dnl_f
	, a.market_fnl
	, a.brand_fnl
	, a.group_ind_fnl
	, a.tfm_include_flag
	, a.migration_source
	, a.product_level_3_fnl
	, sum(a.allw) as allowed
	, sum(a.pd) as paid
	, sum(a.mm) as mms
from tmp_1m.kn_hf_claim_mm as a
group by 
	a.entity
	, a.population
	, a.unique_id
	, a.component
	, a.service_code
	, a.proc_cd
	, a.primary_diag_cd
	, a.fst_srvc_month
	, a.fst_srvc_year
	, a.global_cap
	, a.clm_dnl_f
	, a.market_fnl
	, a.brand_fnl
	, a.group_ind_fnl
	, a.tfm_include_flag
	, a.migration_source
	, a.product_level_3_fnl
;
drop table tmp_1m.kn_hf_claim_mm_sum_filtered;
create table tmp_1m.kn_hf_claim_mm_sum_filtered as
select 
	* 
from tmp_1m.kn_hf_claim_mm_sum
where global_cap = 'NA'
and clm_dnl_f = 'N'
and fst_srvc_month >= '202406'
;


select count(*) from tmp_1m.kn_hf_claim_mm_sum_filtered;

tadm_tre_cpy.dcsp_pr_f_202507
