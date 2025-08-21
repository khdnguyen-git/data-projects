drop table tmp_7d.kn_mbm_dtl;

-- Membership
create table tmp_7d.kn_mbm_dtl stored as ORC as
select
	fin_mbi_hicn_fnl
	, fin_inc_month
	, fin_inc_qtr
	, fin_market as market_fnl
	, case 
	    when (fin_market in ('AR', 'GA', 'NJ', 'SC') and fin_g_i = 'I') then 'Pilot'
	    else 'National'
	end as mbm_deploy_dt
	, fin_g_i as group_ind_fnl
	, case 
	    when migration_source = 'CIP' then 'CIP'
	    when migration_source in ('PC', 'MEDICA') then 'SouthFlorida'
	    when fin_product_level_3 = 'DUAL' and tfm_include_flag = 1 then 'M&R DUALS'
	    when fin_product_level_3 = 'DUAL' and tfm_include_flag = 0 then 'C&S DUALS'
	    when migration_source = 'NA' and fin_g_i = 'I' then 'Legacy Individual'
	    when fin_g_i = 'G' then 'Group'
	    else 'OTHERS'
	end as population
	, if(global_cap = 'NA', 1, 0) as global_cap
	, if(tfm_include_flag = '1', 1, 0) as tfm_include
	, if(fin_product_level_3 in ('INSTITUTIONAL'), 1, 0) as inst
	, if(fin_product_level_2 in ('PFFS'), 1, 0) as pffs
	, if(special_network in ('ERICKSON'), 1, 0) as erk
	, sgr_source_name
    , 1 as mm
	from fichsrv.tre_membership
	where year(fin_incurred_dt) > 2018
	    and fin_brand = 'M&R'
	and fin_product_level_3 not in ('INSTITUTIONAL', 'DUAL')
	group by
	    fin_mbi_hicn_fnl
	    , fin_inc_month
	    , fin_inc_qtr
	    , fin_market
	    , case 
	        when (fin_market in ('AR', 'GA', 'NJ', 'SC') and fin_g_i = 'I') then 'Pilot'
	    else 'National'
	end
	, fin_g_i
	, case 
	    when migration_source = 'CIP' then 'CIP'
	    when migration_source in ('PC', 'MEDICA') then 'SouthFlorida'
	    when fin_product_level_3 = 'DUAL' and tfm_include_flag = 1 then 'M&R DUALS'
	    when fin_product_level_3 = 'DUAL' and tfm_include_flag = 0 then 'C&S DUALS'
	    when migration_source = 'NA' and fin_g_i = 'I' then 'Legacy Individual'
	    when fin_g_i = 'G' then 'Group'
	    else 'OTHERS'
	end
	, if(global_cap = 'NA', 1, 0)
	, if(tfm_include_flag = '1', 1, 0)
	, if(fin_product_level_3 in ('INSTITUTIONAL'), 1, 0)
	, if(fin_product_level_2 in ('PFFS'), 1, 0)
	, if(special_network in ('ERICKSON'), 1, 0)
	, sgr_source_name
	;

select count(*) from tmp_7d.kn_mbm_dtl; 
-- 20250405: 407,463,331
-- 20250513: 415,060,257 


-- Membership Summary
drop table tmp_1y.kn_mbm_mshp;
create table tmp_1y.kn_mbm_mshp stored as orc as
select
    fin_inc_month as ep_start_mo
    , substring(market_fnl, 0, 2) as market_fnl
    , mbm_deploy_dt
    , group_ind_fnl
    , population
    , global_cap
    , tfm_include
    , inst
    , pffs
    , erk
    , sgr_source_name
    , sum(mm) as mm
    , substring(fin_inc_month, 0, 4) as ep_yr
    , substring(fin_inc_month, 5, 2) as ep_mnth
from tmp_7d.kn_mbm_dtl a
group by
    fin_inc_month
    , substring(market_fnl, 0, 2)
    , mbm_deploy_dt
    , group_ind_fnl
    , population
    , global_cap
    , tfm_include
    , inst
    , pffs
    , erk
    , sgr_source_name;

-- Membership Summary for exporting to Excel
drop table tmp_1y.kn_mbm_mshp_sum1;
create table tmp_1y.kn_mbm_mshp_sum1 stored as orc as
select
    'MM' as data_type
    , ep_start_mo
    , '' as visit_mo
    , mbm_deploy_dt as pilot_nat
    , '' as category
    , '' as claim_status
    , 0 as visit_ep_lag
    , 0 as visit_runout_mo
    , 0 as ep_cnt
    , 0 as visit_cnt
    , 0 as allowed_amt
    , sum(mm) as mms
from tmp_1y.kn_mbm_mshp
where population not in ('M&R DUALS', 'C&S DUALS')
group by
    ep_start_mo
    , mbm_deploy_dt;

select count(*) from tmp_1y.kn_mbm_mshp_sum1;
-- 20250405: 126
-- 20250513: 128


-- LOPA
describe formatted tmp_1y.PA_TRCKNG_op_EVNT_LOPA_DTL
describe formatted tmp_1y.PA_TRCKNG_pr_EVNT_LOPA_DTL

-- LOPA OP
drop table tmp_1y.kn_LOPA_OP_1;
create table tmp_1y.kn_LOPA_OP_1 as
select
    case 
        when include_non_sug_event = 1 then mbi_dos
    end as total_mbi_dos
    , case 
        when final_LOPA_ind = 1 and mbr_dos_latest_submission = 1 and include_non_sug_event = 1 then mbi_dos
    end as still_LOPA_mbi_dos
    , case 
        when include_non_sug_event = 1 and (final_LOPA_ind <> 1 or mbr_dos_latest_submission <> 1) then mbi_dos
    end as overturn_LOPA_mbi_dos
    , *
from tmp_1y.pa_trckng_OP_evnt_LOPA_dtl;

select * from tmp_kn_LOPA_OP_1

drop table tmp_1y.kn_LOPA_op;
create table tmp_1y.kn_LOPA_op as
select
    case 
        when still_LOPA_mbi_dos is not null or overturn_LOPA_mbi_dos is not null then mbi_dos
        else null
    end as ever_LOPA
    , *
from tmp_1y.kn_LOPA_OP_1;

select count(*) from tmp_1y.kn_LOPA_op;
-- 20250513: 2,417,275

-- LOPA PR
drop table tmp_1y.kn_LOPA_PR_1;
create table tmp_1y.kn_LOPA_PR_1 as
select
    mbi_dos as total_mbi_dos
    , case 
        when final_LOPA_ind = 1 and mbr_dos_latest_submission = 1 then mbi_dos
    end as still_LOPA_mbi_dos
    , case 
        when final_LOPA_ind <> 1 or mbr_dos_latest_submission <> 1 then mbi_dos
    end as overturn_LOPA_mbi_dos
    , *
from tmp_1y.pa_trckng_PR_evnt_LOPA_dtl;


drop table tmp_1y.kn_LOPA_pr;
create table tmp_1y.kn_LOPA_pr as
select
    case 
        when still_LOPA_mbi_dos is not null or overturn_LOPA_mbi_dos is not null then mbi_dos
        else null
    end as ever_LOPA
    , *
from tmp_1y.kn_LOPA_PR_1;

select count(*) from tmp_1y.kn_LOPA_pr;
-- 20250513: 3,714,719

describe fichsrv.cosmos_pr

drop table tmp_1y.kn_mbm_episode_1; 
create table tmp_1y.kn_mbm_episode_1 as 
select
	a.gal_mbi_hicn_fnl as mbi
	, a.component
	, a.eventkey as id
	, a.service_code  
	, a.fst_srvc_dt as start_dt
	, a.fst_srvc_month as serv_month
	, a.fst_srvc_qtr as hce_qtr
	, to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(a.adjd_dt,10),'MM'),'-','1','-',date_format(date_add(a.adjd_dt,10),'yyyy')),'MM-dd-yyyy'))) as hctapaidmonth
	, a.market_fnl
	, a.group_ind_fnl
	, a.proc_cd
	, a.rvnu_cd
	, a.primary_diag_cd
	, a.ahrq_diag_genl_catgy_desc
	, a.ahrq_diag_dtl_catgy_desc
	, a.prov_prtcp_sts_cd
	, a.prov_tin
	, a.full_nm as prov_full_nm
	, case when b.ever_LOPA is not null then 1 else 0 end as LOPA_flg
	, case when b.still_LOPA_mbi_dos is not null then 1 else 0 end as still_LOPA
	, case when b.overturn_LOPA_mbi_dos is not null then 1 else 0 end as overturn_LOPA
	, 0 as apc_pbl_flg
	, if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') ))  CATEGORY		
			, sum(a.allw_amt_fnl) as allowed
	, sum(a.net_pd_amt_fnl) as paid
	, sum(0) as tadm_util
	, count(distinct a.eventkey) as visits                     
	, sum(a.adj_srvc_unit_cnt) as adj_srvc_units
from fichsrv.cosmos_pr as a
left join tmp_1y.kn_LOPA_pr as b 
	on concat(a.gal_mbi_hicn_fnl, "_", a.fst_srvc_dt) = b.total_mbi_dos 
	and a.proc_cd = b.proc_cd
	and a.prov_tin = b.prov_tin
where a.tfm_include_flag = 1 
	and a.global_cap in ('NA')
	and a.product_level_3_fnl not in ('INSTITUTIONAL', 'DUAL' )
	and a.plan_level_2_fnl not in ('PFFS')
	and a.special_network not in ('ERICKSON')
	and a.st_abbr_cd = a.market_fnl
	and a.prov_prtcp_sts_cd = 'P'
	and (substring(coalesce(a.bil_typ_cd,'0'),0,1) <> 3 or substring(coalesce(a.bil_typ_cd,'0'),0,1) <> '3') 
	and (a.ama_pl_of_srvc_cd <> 12 or a.ama_pl_of_srvc_cd <> '12')     
	and (a.proc_cd in
	('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028', 
	 '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', 
	 '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537', 
	 '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', 
	 '98940', '98941', '98942') 
	or a.rvnu_cd in ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429','0440','0441','0442','0443','0444','0449') )
	and a.proc_cd not in ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151','G0152','G9041','G9043','G9044','S9128','S9129','S9131') 
group by
	a.gal_mbi_hicn_fnl
	, a.component
	, a.eventkey
	, a.service_code
	, a.fst_srvc_dt  
	, a.fst_srvc_month  
	, a.fst_srvc_qtr  
	, to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(a.adjd_dt,10),'MM'),'-','1','-',date_format(date_add(a.adjd_dt,10),'yyyy')),'MM-dd-yyyy')))
	, a.market_fnl
	, a.group_ind_fnl
	, a.proc_cd
	, a.rvnu_cd
	, a.primary_diag_cd
	, a.ahrq_diag_genl_catgy_desc
	, a.ahrq_diag_dtl_catgy_desc
	, a.prov_prtcp_sts_cd
	, a.prov_tin
	, a.full_nm 
	, case when b.ever_LOPA is not null then 1 else 0 end
	, case when b.still_LOPA_mbi_dos is not null then 1 else 0 end
	, case when b.overturn_LOPA_mbi_dos is not null then 1 else 0 end
	,if(a.proc_cd in ('98940','98941','98942') and a.Component = 'PR' , 'Chiro' ,if(a.ama_pl_of_srvc_cd in ('11', '49'), 'Office',if(a.ama_pl_of_srvc_cd IN ('22','62', '19', '24') and  a.Component = 'OP','OP_REHAB','Other') )) 		
order by a.gal_mbi_hicn_fnl asc;

select count(*) from tmp_1y.cl_mbm_episode_1;


