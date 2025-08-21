-- Full Membership Table;
drop table if exists tmp_1m.kn_mbm_dtl;
create table tmp_1m.kn_mbm_dtl stored as orc as
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
        when b.migration_source = 'CIP' then 'CIP'
        when b.migration_source in ('PC', 'MEDICA') then 'SouthFlorida'
        when b.fin_product_level_3 = 'DUAL' and b.tfm_include_flag = 1 then 'M&R DUALS'
        when b.fin_product_level_3 = 'DUAL' and b.tfm_include_flag = 0 then 'C&S DUALS'
        when b.migration_source = 'NA' and b.fin_g_i = 'I' then 'Legacy Individual'
        when b.fin_g_i = 'G' then 'Group'
        else 'OTHERS'
    end as population
    , if(global_cap = 'NA', 1, 0) as global_cap
    , if(tfm_include_flag = '1', 1, 0) as tfm_include_flag
    , if(fin_product_level_3 in ('INSTITUTIONAL'), 1, 0) as inst
    , if(fin_product_level_2 in ('PFFS'), 1, 0) as pffs
    , if(special_network in ('ERICKSON'), 1, 0) as erk
    , sgr_source_name
    , 1 as mm
from fichsrv.tre_membership as b
where year(fin_incurred_dt) > 2018
    and b.fin_brand = 'M&R'
    and b.fin_product_level_3 not in ('INSTITUTIONAL', 'DUAL')
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
        when b.migration_source = 'CIP' then 'CIP'
        when b.migration_source in ('PC', 'MEDICA') then 'SouthFlorida'
        when b.fin_product_level_3 = 'DUAL' and b.tfm_include_flag = 1 then 'M&R DUALS'
        when b.fin_product_level_3 = 'DUAL' and b.tfm_include_flag = 0 then 'C&S DUALS'
        when b.migration_source = 'NA' and b.fin_g_i = 'I' then 'Legacy Individual'
        when b.fin_g_i = 'G' then 'Group'
        else 'OTHERS'
    end
    , if(global_cap = 'NA', 1, 0)
    , if(tfm_include_flag = '1', 1, 0)
    , if(fin_product_level_3 in ('INSTITUTIONAL'), 1, 0)
    , if(fin_product_level_2 in ('PFFS'), 1, 0)
    , if(special_network in ('ERICKSON'), 1, 0)
    , sgr_source_name
;

-- QA 1;
select count(*) from tmp_1m.kn_mbm_dtl;
-- 20250405: 407,463,331
-- 20250513: 415,060,257 
-- 20250610: 422,697,536
-- 20250724: 430,370,488


-- Membership Subset;
-- KN_202505: Added global_cap = 1 since 202504 Affordability
drop table if exists tmp_1m.kn_mbm_mshp_sum1;
create table tmp_1m.kn_mbm_mshp_sum1 stored as orc as
with mbm_grouping as (
select
    fin_inc_month as ep_start_mo
    , substring(market_fnl, 0, 2) as market_fnl
    , mbm_deploy_dt
    , group_ind_fnl
    , population
    , global_cap
    , tfm_include_flag
    , inst
    , pffs
    , erk
    , sgr_source_name
    , sum(mm) as mm
    , substring(fin_inc_month, 0, 4) as ep_yr
    , substring(fin_inc_month, 5, 2) as ep_mnth
from tmp_1m.kn_mbm_dtl as a
group by
    fin_inc_month
    , substring(market_fnl, 0, 2)
    , mbm_deploy_dt
    , group_ind_fnl
    , population
    , global_cap
    , tfm_include_flag
    , inst
    , pffs
    , erk
    , sgr_source_name
)
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
from mbm_grouping
where population not in ('M&R DUALS', 'C&S DUALS') 
    and global_cap = 1
group by
    ep_start_mo
    , mbm_deploy_dt 
;

select count(*) from tmp_1m.kn_mbm_mshp_sum1;
-- <Date>: Count
-- 20250405: 126
-- 20250513: 128
-- 20250612: 130
-- 20250724: 132

-- Removed LOPA;

-- PR claims;
create table tmp_1m.kn_mbm_episode_pr as
select
    gal_mbi_hicn_fnl as mbi
    , component
    , eventkey as id
    , service_code
    , fst_srvc_dt as start_dt
    , fst_srvc_month as serv_month
    , fst_srvc_qtr hce_qtr
    , to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(adjd_dt, 10), 'MM'), '-', '1', '-', 
    date_format(date_add(adjd_dt, 10), 'yyyy')), 'MM-dd-yyyy'))) as hctapaidmonth
    , market_fnl
    , group_ind_fnl
    , proc_cd
    , rvnu_cd
    , primary_diag_cd
    , ahrq_diag_genl_catgy_desc
    , ahrq_diag_dtl_catgy_desc
    , prov_prtcp_sts_cd
    , prov_tin
    , full_nm as prov_full_nm
    , 0 as apc_pbl_flg
    , case 
    	when proc_cd in ('98940', '98941', '98942') and component = 'PR' then 'Chiro'
    	when ama_pl_of_srvc_cd in ('11', '49') then 'Office'
    	when ama_pl_of_srvc_cd in ('22', '62', '19', '24') and component = 'OP' then 'OP_REHAB'
    	else 'Other'
    end as category
    , sum(allw_amt_fnl) as allowed
    , sum(net_pd_amt_fnl) as paid
    , sum(0) as tadm_util
    , count(distinct eventkey) as visits
    , sum(adj_srvc_unit_cnt) as adj_srvc_units
from fichsrv.cosmos_pr as a
where tfm_include_flag = 1
    and global_cap in ('NA')
    and product_level_3_fnl not in ('INSTITUTIONAL', 'DUAL' )
    and plan_level_2_fnl not in ('PFFS')
    and special_network not in ('ERICKSON')
    and st_abbr_cd = market_fnl
    and prov_prtcp_sts_cd = 'P'
    and 
    (substring(coalesce(bil_typ_cd, '0'), 0, 1) <> 3 
    or substring(coalesce(bil_typ_cd, '0'), 0, 1) <> '3')
    and 
    (ama_pl_of_srvc_cd <> 12 
    or ama_pl_of_srvc_cd <> '12')
    and 
    (proc_cd 
    in ('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028' 
        , '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116' 
        , '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537' 
        , '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', '98940' 
        , '98941', '98942')
    or rvnu_cd 
    in ('0430', '0431', '0432', '0433', '0434', '0439', '0420', '0421', '0422', '0423' 
        , '0424', '0429', '0440', '0441', '0442', '0443', '0444', '0449') )
    and proc_cd not 
    in ('92630', '92633', '97001', '97002', '97003', '97004', '97545', '97546', '98943', 'G0129' 
        , 'G0151', 'G0152', 'G9041', 'G9043', 'G9044', 'S9128', 'S9129', 'S9131')
group by
    gal_mbi_hicn_fnl
    , component
    , eventkey
    , service_code
    , fst_srvc_dt
    , fst_srvc_month
    , fst_srvc_qtr
    , to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(adjd_dt, 10), 'MM'), '-', '1', '-', 
    date_format(date_add(adjd_dt, 10), 'yyyy')), 'MM-dd-yyyy')))
    , market_fnl
    , group_ind_fnl
    , proc_cd
    , rvnu_cd
    , primary_diag_cd
    , ahrq_diag_genl_catgy_desc
    , ahrq_diag_dtl_catgy_desc
    , prov_prtcp_sts_cd
    , prov_tin
    , full_nm
    , case 
		when proc_cd in ('98940', '98941', '98942') and component = 'PR' then 'Chiro'
		when ama_pl_of_srvc_cd in ('11', '49') then 'Office'
		when ama_pl_of_srvc_cd in ('22', '62', '19', '24') and component = 'OP' then 'OP_REHAB'
		else 'Other'
	end
;

-- QA 3;
select count(*) from tmp_1m.kn_mbm_episode_pr;
-- Previous: 64,354,173  62,711,063  60,990,327
-- 20250612: 58,833,395
-- 20250724: 60,035,461


-- 35607098 34786029 38657890  37706450  36737912  35885437      select count(*) from TMP_1m.kn_MBM_EPISODE_op

/*______________[OP CLAIM PULL APC ]___(APC = Ambulatory Payment Code)________________________________________________________________________________*/;	
drop table 
    tmp_7d.kn_mbm_claims 
;
 
create table 
   tmp_7d.kn_mbm_claims as
select 
    *
    , max(if(instr(clm_rev_rsn_1_10, '00473-') > 0, 1, 0)) over (partition by site_cd , clm_aud_nbr , sbscr_nbr) as clm_apc_flg
    , sum(allw_amt_fnl) over (partition by site_cd , clm_aud_nbr , sbscr_nbr) as clm_allw_amnt
from (select 
        *
        , concat(a.clm_rev_rsn_1_cd, '-', a.clm_rev_rsn_2_cd, '-', a.clm_rev_rsn_3_cd, '-', a.clm_rev_rsn_4_cd, '-', 
        a.clm_rev_rsn_5_cd, '-', a.clm_rev_rsn_6_cd, '-', a.clm_rev_rsn_7_cd, '-', a.clm_rev_rsn_8_cd, '-', 
        a.clm_rev_rsn_9_cd, '-', a.clm_rev_rsn_10_cd, '-') clm_rev_rsn_1_10
    from fichsrv.cosmos_op as a
    where (a.proc_cd 
        in ('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028' 
            , '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116' 
            , '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537' 
            , '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', '98940' 
            , '98941', '98942')
        or rvnu_cd 
        in ('0430', '0431', '0432', '0433', '0434', '0439', '0420', '0421', '0422', '0423' 
            , '0424', '0429', '0440', '0441', '0442', '0443', '0444', '0449') )
        and proc_cd not 
        in ('92630', '92633', '97001', '97002', '97003', '97004', '97545', '97546', '98943', 'G0129' 
            , 'G0151', 'G0152', 'G9041', 'G9043', 'G9044', 'S9128', 'S9129', 'S9131')
	) as subquery
;
 
/*--77536654  75713104  73889849     72274650           select count(*) from tmp_7d.kn_mbm_claims*/
-- 20250724: 71,184,825

select 
    count(*) 
from tmp_1m.kn_mbm_claims 
;

drop table 
    tmp_7d.kn_mbm_episode_op
;
 
create table 
    tmp_1m.kn_mbm_episode_op as
select
    a.gal_mbi_hicn_fnl as mbi
    , a.component
    , a.eventkey as id
    , a.hce_service_code service_code
    , a.fst_srvc_dt as start_dt
    , a.fst_srvc_month as serv_month
    , a.fst_srvc_qtr hce_qtr
    , to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(a.adjd_dt, 10), 'MM'), '-', '1', '-', 
    date_format(date_add(a.adjd_dt, 10), 'yyyy')), 'MM-dd-yyyy'))) as hctapaidmonth
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
    , case 
        when a.clm_apc_flg = 1 and c.rsn_cd in ('208', '176', '943') then 1 
        else 0 
    end as apc_pbl_flg
    , case 
		when proc_cd in ('98940', '98941', '98942') and component = 'PR' then 'Chiro'
		when ama_pl_of_srvc_cd in ('11', '49') then 'Office'
		when ama_pl_of_srvc_cd in ('22', '62', '19', '24') and component = 'OP' then 'OP_REHAB'
		else 'Other'
	end as category
    , sum(a.allw_amt_fnl) as allowed
    , sum(a.net_pd_amt_fnl) as paid
    , sum(0) as tadm_util
    , count(distinct a.eventkey) as visits
    , sum(a.adj_srvc_unit_cnt) as adj_srvc_units
from tmp_7d.kn_mbm_claims as a
left join fichsrv.tadm_glxy_reason_code as c
    on  a.fnl_rsn_cd_sys_id = c.rsn_cd_sys_id
where a.tfm_include_flag = 1
    and a.global_cap in ('NA')
    and a.product_level_3_fnl not in ('INSTITUTIONAL', 'DUAL')
    and a.plan_level_2_fnl not in ('PFFS')
    and a.special_network not in ('ERICKSON')
    /*--AND ST_ABBR_CD IN ('AR', 'GA', 'NJ', 'SC','CT','NC','PA','NY','AL')*/
    and a.st_abbr_cd = a.market_fnl
    and a.prov_prtcp_sts_cd = 'P'
    and substring(coalesce(a.bil_typ_cd, '0'), 0, 1) <> 3
    and a.ama_pl_of_srvc_cd <> 12
    and 
    (a.proc_cd 
    in ('92507', '92508', '92526', '97012', '97016', '97018', '97022', '97024', '97026', '97028' 
        , '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116' 
        , '97124', '97139', '97140', '97150', '97164', '97168', '97530', '97533', '97535', '97537' 
        , '97542', '97545', '97546', '97750', '97755', '97760', '97761', '97799', 'G0283', '98940' 
        , '98941', '98942')
    or a.rvnu_cd 
    in ('0430', '0431', '0432', '0433', '0434', '0439', '0420', '0421', '0422', '0423' 
        , '0424', '0429', '0440', '0441', '0442', '0443', '0444', '0449') )
    and a.proc_cd not 
    in ('92630', '92633', '97001', '97002', '97003', '97004', '97545', '97546', '98943', 'G0129' 
        , 'G0151', 'G0152', 'G9041', 'G9043', 'G9044', 'S9128', 'S9129', 'S9131')
group by
    a.gal_mbi_hicn_fnl
    , a.component
    , a.eventkey
    , a.hce_service_code
    , a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , to_date(from_unixtime(unix_timestamp(concat(date_format(date_add(a.adjd_dt, 10), 'MM'), '-', '1', '-', 
    date_format(date_add(a.adjd_dt, 10), 'yyyy')), 'MM-dd-yyyy')))
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
    , case 
        when a.clm_apc_flg = 1 and c.rsn_cd in ('208', '176', '943') then 1 
        else 0 
    end
    , case 
		when proc_cd in ('98940', '98941', '98942') and component = 'PR' then 'Chiro'
		when ama_pl_of_srvc_cd in ('11', '49') then 'Office'
		when ama_pl_of_srvc_cd in ('22', '62', '19', '24') and component = 'OP' then 'OP_REHAB'
		else 'Other'
	end
; 
select count(*) from TMP_1m.kn_MBM_EPISODE_op;
-- 20250613: 34,786,029
-- 	38657890 38657890  37706450  36737912  35885437      select count(*) from tmp_1m.kn_MBM_EPISODE_op
-- 20250724: 35,607,033


-- Re-adding 2018-2020 PR data, remove LOPA
drop table tmp_1y.kn_mbm_episode_pr_2018_2020;
create table tmp_1y.kn_mbm_episode_pr_2018_2020 as
select
    mbi 
    , component 
    , id 
    , service_code 
    , start_dt 
    , serv_month 
    , hce_qtr 
    , hctapaidmonth 
    , market_fnl 
    , group_ind_fnl 
    , proc_cd 
    , rvnu_cd 
    , primary_diag_cd 
    , ahrq_diag_genl_catgy_desc 
    , ahrq_diag_dtl_catgy_desc 
    , prov_prtcp_sts_cd 
    , prov_tin 
    , prov_full_nm 
    , apc_pbl_flg 
    , category 
    , allowed 
    , paid 
    , tadm_util 
    , visits 
    , adj_srvc_units
from tmp_1y.cl_mbm_episode_1_2018_2020 
;

-- Re-adding 2018-2020 OP data, remove LOPA
drop table tmp_1y.kn_mbm_episode_op_2018_2020;
create table tmp_1y.kn_mbm_episode_op_2018_2020 as
select
    mbi 
    , component 
    , id 
    , service_code 
    , start_dt 
    , serv_month 
    , hce_qtr 
    , hctapaidmonth 
    , market_fnl 
    , group_ind_fnl 
    , proc_cd 
    , rvnu_cd 
    , primary_diag_cd 
    , ahrq_diag_genl_catgy_desc 
    , ahrq_diag_dtl_catgy_desc 
    , prov_prtcp_sts_cd 
    , prov_tin 
    , prov_full_nm 
    , apc_pbl_flg 
    , category 
    , allowed 
    , paid 
    , tadm_util 
    , visits 
    , adj_srvc_units
from tmp_1y.cl_mbm_episode_1b_2018_2020 
;

select count(*) from tmp_1m.kn_mbm_episode_pr;
select count(*) from tmp_1y.kn_mbm_episode_pr_2018_2020;
select count(*) from tmp_1m.kn_mbm_episode_op;
select count(*) from tmp_1y.kn_mbm_episode_op_2018_2020;


drop table if exists tmp_1m.kn_mbm_episode_1c;
create table tmp_1m.kn_mbm_episode_1c as
select * from tmp_1m.kn_mbm_episode_pr
union all
select * from tmp_1y.kn_mbm_episode_pr_2018_2020
union all
select * from tmp_1m.kn_mbm_episode_op
union all
select * from tmp_1y.kn_mbm_episode_op_2018_2020
;




select count(*) from tmp_1m.kn_mbm_episode_1c;
-- 20250724: 133,883,501

drop table if exists tmp_1m.kn_mbm_episode_2 
;
create table tmp_1m.kn_mbm_episode_2 as
with cte_dnl as (
select
    * 
    , sum(allowed) over(partition by id , start_dt , category) as dnl_allowed
from tmp_1m.kn_mbm_episode_1c
),
cte_optum as (
select
	a.* 
	, b.tin_num
from cte_dnl as a
left join tmp_1y.p8001_optum_tin_2 as b
	on a.prov_tin = b.tin_num 
	and b.i = 1
)
select
    * 
    , case
        when dnl_allowed > 0.01 then 'Paid'
        when apc_pbl_flg = 1 then 'APC-Paid'
        else 'Other Denied'
    end as claim_status 
    , case
        when tin_num is null then 0
        else 1
    end as optum_flg 
    , case
        when proc_cd in('98940', '98941', '98942') then 'Chiro'
        when proc_cd 
            in('97001', '97002', '97003', '97004', '97012', '97016', '97018', '97022', '97024', '97026' 
               , '97028', '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113' 
               , '97116', '97124', '97139', '97140', '97150', '97161', '97162', '97163', '97164', '97165' 
               , '97166', '97167', '97168', '97530', '97532', '97533', '97535', '97537', '97542', '97545' 
               , '97546', '97750', '97755', '97760', '97761', '97762', '97799', 'G0129', 'G0151', 'G0152' 
               , 'G0281', 'G0282', 'G0283', 'G9041', 'G9043', 'G9044', 'S9129', 'S9131') then 'PT-OT'
        when proc_cd 
            in('70371', '92506', '92507', '92508', '92521', '92522', '92523', '92524', '92526', '92626' 
               , '92627', '92630', '92633', '96105', 'S9128') then 'ST'
        else 'Other'
    end as mbmserv_dtl 
    , case
        when market_fnl in('AR', 'GA', 'NJ', 'SC') and group_ind_fnl = 'I' then
            case
                when category = 'OP_REHAB' then 'Phase-II'
                when tin_num is null then 'Phase-II'
                else 'Phase-I'
            end
        else 'National'
    end as mbm_deploy_dt
from cte_optum 
;

select count(*) from tmp_1m.kn_mbm_episode_2;
-- 133,883,501

select serv_month, sum(allowed) as allowedamt 
from TMP_1m.kn_MBM_EPISODE_2
where serv_month = '202406'  -- 73,482,783.47 73,432,103.42  73,635,236.05  73,514,079.66 (shorter proc_cd list)  76,561,115.88 (original proc_cd list) 
group by serv_month

drop table tmp_1m.kn_mbm_episode_3;
create table tmp_1m.kn_mbm_episode_3 as
select
    concat(mbi, '-', category) as mbi
    , component
    , id
    , start_dt
    , serv_month
    , hce_qtr
    , min(hctapaidmonth) as hctapaidmonth
    , mbm_deploy_dt
    , market_fnl
    , claim_status
    , cast(mbmserv_dtl as varchar (10)) as mbmserv
    , category
    , sum(allowed) as allowed
    , sum(paid) as paid
    , sum(tadm_util) as tadm_util
    , count(distinct concat(id, start_dt)) as visits
    , count(visits) as vsts
    , sum(adj_srvc_units) as adj_srvc_units
from tmp_1m.kn_mbm_episode_2
where prov_prtcp_sts_cd = 'P'
group by
    concat(mbi, '-', category)
    , component
    , id
    , start_dt
    , serv_month
    , hce_qtr
    , mbm_deploy_dt
    , market_fnl
    , claim_status
    , mbmserv_dtl
    , optum_flg
    , category
;

-- 63314625 66929218  65797555     64650521  select count(*) from TMP_1m.kn_MBM_EPISODE_3  where mbi like '7GT2FY4RA93%' -- 170
-- 20250724: 63,314,625


drop table if exists tmp_1m.kn_mbm_episode_4 
;

create table tmp_1m.kn_mbm_episode_4 as
select
    mbi
    , component
    , id
    , start_dt
    , row_number() over (partition by mbi, mbm_deploy_dt order by start_dt) as i
    , serv_month
    , hce_qtr
    , hctapaidmonth
    , mbm_deploy_dt
    , market_fnl
    , claim_status
    , mbmserv
    , category
    , allowed
    , paid
    , tadm_util
    , visits
    , vsts
    , adj_srvc_units
from tmp_1m.kn_mbm_episode_3
;

select count(*) from tmp_1m.kn_mbm_episode_4;
-- 20250724: 63,314,625

drop table if exists tmp_1m.kn_mbm_episode_lag 
;

create table 
    tmp_1m.kn_mbm_episode_lag as
select
    a.mbi
    , a.component
    , a.id
    , a.start_dt
    , b.start_dt as prev_start_dt
    , datediff(a.start_dt, b.start_dt) as visit_dy_lag
    , if(datediff(a.start_dt, b.start_dt) > 30, 1, 0) as ep_flag
    , a.i
    , b.i as prev_i
    , a.serv_month
    , a.hce_qtr
    , a.hctapaidmonth
    , a.mbm_deploy_dt
    , a.market_fnl
    , a.claim_status
    , a.mbmserv
    , a.category
    , a.allowed
    , a.paid
    , a.tadm_util
    , a.visits
    , a.vsts
    , a.adj_srvc_units
from tmp_1m.kn_mbm_episode_4 as a
left join tmp_1m.kn_mbm_episode_4 as b
    on  a.mbi = b.mbi
        and a.mbm_deploy_dt = b.mbm_deploy_dt
        and a.i = b.i+1
;

select count(*) from tmp_1m.kn_mbm_episode_lag;
-- 20250724: 63,314,625

drop table if exists tmp_1m.kn_mbm_episode_vst_ep_2
;

create table tmp_1m.kn_mbm_episode_vst_ep_2 as
with episode_lag_with_cmltv as (
select
    * 
    ,
    sum(case
            when prev_start_dt is null then 1
            else ep_flag
        end) over(partition by mbi order by start_dt rows between unbounded preceding and current row) as cmltv_episodes
from tmp_1m.kn_mbm_episode_lag
)
select
    a.mbi 
    , a.component 
    , a.id 
    , a.start_dt 
    , a.prev_start_dt 
    , a.visit_dy_lag 
    , a.ep_flag 
    , min(start_dt) over(partition by mbi , cmltv_episodes) as ep_start_dt 
    , cmltv_episodes 
    , a.i 
    , a.prev_i 
    , a.serv_month 
    , a.hce_qtr 
    , a.hctapaidmonth 
    , min(hctapaidmonth) over(partition by mbi, cmltv_episodes) as ep_hctapaidmonth 
    , a.mbm_deploy_dt 
    , a.market_fnl 
    , a.claim_status 
    , a.mbmserv 
    , a.category 
    , a.allowed 
    , a.paid 
    , a.tadm_util 
    , a.visits 
    , a.vsts 
    , a.adj_srvc_units
from episode_lag_with_cmltv as a 
;


select count(*) from tmp_1m.kn_mbm_episode_vst_ep_2;
-- 20250724: 63,314,625

drop table if exists tmp_1m.kn_mbm_episode_smry
;

create table tmp_1m.kn_mbm_episode_smry as
select
    a.serv_month as visit_month
    , date_format(ep_start_dt, 'yyyyMM') as ep_start_mo
    , a.hctapaidmonth
    , a.mbm_deploy_dt
    , a.market_fnl
    , a.claim_status
    , a.mbmserv
    , a.category
    , count(distinct mbi) as mbr_count
    , sum(a.allowed) as allw
    , sum(a.paid) as pd
    , sum(a.visits) as visits
    , sum(ep_flag) as episodes
from tmp_1m.kn_mbm_episode_vst_ep_2 as a
group by
    a.serv_month
    , date_format(ep_start_dt, 'yyyyMM')
    , a.hctapaidmonth
    , a.mbm_deploy_dt
    , a.market_fnl
    , a.claim_status
    , a.mbmserv
    , a.category
;
-- 1310728

drop table if exists tmp_1m.kn_mbm_episode_ro_lag
;

create table tmp_1m.kn_mbm_episode_ro_lag as
select
    a.mbi
    , a.component
    , a.id
    , a.start_dt
    , floor((datediff(hctapaidmonth, start_dt)+20) / 30.5) as visit_runout_mo
    , round((datediff(hctapaidmonth, start_dt)+20) / 1, 0) as visit_runout
    , floor(datediff(start_dt, ep_start_dt) / 30.5) as visit_ep_lag
    , visit_dy_lag
    , if(prev_start_dt is null, 1, ep_flag) as ep_flag
    , ep_start_dt
    , cmltv_episodes
    , a.i
    , a.prev_i
    , a.serv_month
    , a.hce_qtr
    , a.hctapaidmonth
    , ep_hctapaidmonth
    , a.mbm_deploy_dt
    , a.market_fnl
    , a.claim_status
    , a.mbmserv
    , a.category
    , a.allowed
    , a.paid
    , a.tadm_util
    , a.visits
    , a.vsts
    , a.adj_srvc_units
from tmp_1m.kn_mbm_episode_vst_ep_2 as a
;
-- 63314625

select sum(allowed)
from tmp_1m.kn_mbm_episode_ro_lag2
where visit_mo between '202101' and '202108'
;

select sum(allowed)
from tmp_1m.kn_mbm_episode_ro_lag2
where visit_mo between '202101' and '202108'
;

drop table if exists tmp_1m.kn_mbm_episode_ro_lag2
;



create table tmp_1m.kn_mbm_episode_ro_lag2 as
select
    a.mbi
    , a.id
    , ep_start_dt
    , cmltv_episodes
    , start_dt
    , date_format(ep_start_dt, 'yyyyMM') as ep_start_mo
    , date_format(ep_start_dt, 'yyyy') as ep_start_year
    , market_fnl
    , mbm_deploy_dt
    , category
    , claim_status
    , hctapaidmonth
    , mbmserv visit_mbmserv
    , visit_runout_mo
    , 0 ep_runout_mo
    , date_format(start_dt, 'yyyyMM') as visit_mo
    , visit_ep_lag
    , ep_flag episodes
    , visits
    , allowed
    , 0 as mm
from tmp_1m.kn_mbm_episode_ro_lag a
;

-- 63314625

drop table if exists tmp_1m.kn_mbm_episode_agg6_ep
;

create table tmp_1m.kn_mbm_episode_agg6_ep as
with episode_1 as (
select
	*
from tmp_1m.kn_mbm_episode_ro_lag2
where episodes = 1
)
select
    'EPISODES' data_type
    , ep_start_mo
    , concat(ep_start_year, 'Q9') as ep_start_qtr
    , market_fnl
    , mbm_deploy_dt
    , category
    , claim_status
    , ''visit_mbmserv
    , 0 visit_runout_mo
    , 0 ep_runout_mo
    , 0 visit_mo
    , 0 visit_ep_lag
    , sum(episodes) as episodes
    , 0 visits
    , 0 allowed
    , 0 mm
from episode_1
group by
    ep_start_mo
    , concat(ep_start_year, 'Q9')
    , market_fnl
    , mbm_deploy_dt
    , category
    , claim_status
;

drop table if exists tmp_1m.kn_mbm_episode_agg6
;

create table tmp_1m.kn_mbm_episode_agg6 as
select
    'VISITS' data_type
    , ep_start_mo
    , concat(ep_start_year, 'Q9') as ep_start_qtr
    , market_fnl
    , mbm_deploy_dt
    , category
    , claim_status
    , visit_mbmserv
    , visit_runout_mo
    , ep_runout_mo
    , visit_mo
    , visit_ep_lag
    , sum(0) as episodes
    , sum(visits) as visits
    , sum(allowed) as allowed
    , 0 as mm
from tmp_1m.kn_mbm_episode_ro_lag2
group by
    ep_start_mo
    , concat(ep_start_year, 'Q9')
    , market_fnl
    , mbm_deploy_dt
    , category
    , claim_status
    , visit_mbmserv
    , visit_runout_mo
    , ep_runout_mo
    , visit_mo
    , visit_ep_lag
;

create table tmp_1m.kn_mbm_episode_agg6_with_ep as
select
    *
from tmp_1m.kn_mbm_episode_agg6
union all 
select
    *
from tmp_1m.kn_mbm_episode_agg6_ep
;



insert into tmp_1m.kn_mbm_episode_agg6
select
    *
from tmp_1m.kn_mbm_episode_agg6_ep
;

select count(*) from tmp_1m.kn_mbm_episode_agg6_ep; -- 40810
select count(*) from tmp_1m.kn_mbm_episode_agg6; -- 1954051

select count(*) from tmp_1m.kn_MBM_EPISODE_RO_LAG; -- 63314625 66929218  65797555  64650521 

select count(*) from TMP_1y.cl_MBM_EPISODE_AGG6_EP; -- 45630
select count(*) from TMP_1y.cl_MBM_EPISODE_AGG6;


alter table tmp_1m.kn_mbm_episode_agg6 change data_type data_type varchar(20);
alter table tmp_1m.kn_mbm_episode_agg6 change ep_start_mo ep_start_mo varchar(20);
alter table tmp_1m.kn_mbm_episode_agg6 change ep_start_qtr ep_start_qtr varchar(20);
alter table tmp_1m.kn_mbm_episode_agg6 change mbm_deploy_dt mbm_deploy_dt varchar(20);
alter table tmp_1m.kn_mbm_episode_agg6 change claim_status claim_status varchar(20);
alter table tmp_1m.kn_mbm_episode_agg6 change visit_mo visit_mo varchar(20);
alter table tmp_1m.kn_mbm_episode_agg6 change category category varchar(20);


-- Excel
select count(*) from tmp_1m.kn_mbm_episode_agg6_sum1; -- 173,475 179641

drop table tmp_1m.kn_mbm_episode_agg6_sum1;
create table tmp_1m.kn_mbm_episode_agg6_sum1 stored as orc as
select
    data_type
    , ep_start_mo
    , substring(ep_start_mo, 0, 4) as ep_year
    , substring(ep_start_mo, 5, 2) as ep_month
    , visit_mo
    , case
        when mbm_deploy_dt = 'National' then 'National'
        else 'Pilot'
    end as pilot_nat
    , category
    , visit_ep_lag
    , visit_runout_mo
    , sum(episodes) as ep_cnt
    , sum(visits) as visit_cnt
    , sum(allowed) as allowed_amt
    , sum(mm) as mms
from tmp_1m.kn_mbm_episode_agg6
where ep_start_mo > '201812'
group by
    data_type
    , ep_start_mo
    , substring(ep_start_mo, 0, 4)
    , substring(ep_start_mo, 5, 2)
    , visit_mo
    , case
        when mbm_deploy_dt = 'National' then 'National'
        else 'Pilot'
    end
    , category
    , claim_status
    , visit_ep_lag
    , visit_runout_mo
union
select
    data_type
    , ep_start_mo
    , substring(ep_start_mo, 0, 4) as ep_year
    , substring(ep_start_mo, 5, 2) as ep_month
    , visit_mo
    , pilot_nat
    , category
    , visit_ep_lag
    , visit_runout_mo
    , ep_cnt
    , visit_cnt
    , allowed_amt
    , mms
from tmp_1m.kn_mbm_mshp_sum1
;






select 
    count(*) 
from tmp_1m.kn_mbm_episode_agg6_sum1
where visit_mo between '202101' and '202108' 
;
select 
    count(*) 
from tmp_1m.kn_mbm_mshp_sum1
select 
    sum(allowed)
from tmp_1y.kn_mbm_episode_agg6
where visit_mo = '202406'
select 
    sum(mms)
from tmp_1y.kn_mbm_episode_agg6_sum1
select 
    sum(mms)
from tmp_1m.kn_mbm_episode_agg6_sum1
select 
    sum(mms)
from tmp_1y.cl_mbm_episode_agg6_sum1
select 
    sum(episodes)
from tmp_1y.cl_mbm_episode_agg6_ep
union
select 
    sum(episodes)
from tmp_1m.kn_mbm_episode_agg6_ep 
;

select
    ep_start_mo
    , sum(mms)
from tmp_1y.cl_mbm_episode_agg6_sum1
group by
    ep_start_mo