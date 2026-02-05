drop table if exists tmp_7d.knd_mbm_episode_pr_202512;
create table tmp_7d.knd_mbm_episode_pr_202512 as
select
    gal_mbi_hicn_fnl as mbi
    , component
    , eventkey as id
    , service_code
    , fst_srvc_dt as start_dt
    , fst_srvc_month as serv_month
    , fst_srvc_qtr as hce_qtr
	, date_trunc('month', dateadd(day, 10, adjd_dt)) as hctapaidmonth
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
    , case
        when proc_cd in ('98940','98941','98942') and component = 'PR' then 'Chiro'
        when ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when ama_pl_of_srvc_cd in ('22','62','19','24') and component = 'OP' then 'OP_REHAB'
        else 'Other'
	end as category
    , sum(allw_amt_fnl) as allowed
    , sum(net_pd_amt_fnl) as paid
    , sum(0) as tadm_util
    , count(distinct eventkey) as visits
    , sum(adj_srvc_unit_cnt) as adj_srvc_units
from fichsrv.glxy_pr_f as a
where 
	brand_fnl in ('M&R', 'C&S')
	and tfm_include_flag = 1
    and global_cap = 'NA'
    -- and product_level_3_fnl not in ('INSTITUTIONAL','DUAL')
    and plan_level_2_fnl not in ('PFFS')
    and special_network not in ('ERICKSON')
    and st_abbr_cd = market_fnl
    and prov_prtcp_sts_cd = 'P'
    and (substring(coalesce(bil_typ_cd,'0'), 1, 1) <> '3')
    and (ama_pl_of_srvc_cd <> '12')
    and (
        proc_cd in 
        ('92507','92508','92526','97012','97016','97018','97022','97024','97026','97028',
		 '97032','97033','97034','97035','97036','97039','97110','97112','97113','97116',
         '97124','97139','97140','97150','97164','97168','97530','97533','97535','97537',
         '97542','97545','97546','97750','97755','97760','97761','97799','G0283',
         '98940','98941','98942')
        or rvnu_cd in 
        ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429',
         '0440','0441','0442','0443','0444','0449')
    )
    and proc_cd not in 
    ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151',
     'G0152','G9041','G9043','G9044','S9128','S9129','S9131')
group by
    gal_mbi_hicn_fnl
    , component
    , eventkey
    , service_code
    , fst_srvc_dt
    , fst_srvc_month
    , fst_srvc_qtr
	, date_trunc('month', dateadd(day, 10, adjd_dt))
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
        when proc_cd in ('98940','98941','98942') and component = 'PR' then 'Chiro'
        when ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when ama_pl_of_srvc_cd in ('22','62','19','24') and component = 'OP' then 'OP_REHAB'
        else 'Other'
	end
;




/*==============================================================================
 * OUTPATIENT CLAIMS APC PROCESSING
 * Pull in OP claims with APC flags and LOPA flags
 *==============================================================================*/

drop table if exists tmp_7d.knd_mbm_episode_op_202512;
create table tmp_7d.knd_mbm_episode_op_202512 as
select
    a.gal_mbi_hicn_fnl as mbi
    , a.component
    , a.eventkey as id
    , a.hce_service_code as service_code
    , a.fst_srvc_dt as start_dt
    , a.fst_srvc_month as serv_month
    , a.fst_srvc_qtr as hce_qtr
    , date_trunc('month', dateadd(day, 10, a.adjd_dt)) as hctapaidmonth
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
        when a.proc_cd in ('98940','98941','98942') and a.component = 'PR' then 'Chiro'
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
      end as category
    , sum(a.allw_amt_fnl) as allowed
    , sum(a.net_pd_amt_fnl) as paid
    , sum(0) as tadm_util
    , count(distinct a.eventkey) as visits
    , sum(a.adj_srvc_unit_cnt) as adj_srvc_units
from fichsrv.glxy_op_f as a
where a.tfm_include_flag = 1
    and a.global_cap = 'NA'
   -- and a.product_level_3_fnl not in ('INSTITUTIONAL','DUAL')
    and a.plan_level_2_fnl not in ('PFFS')
    and a.special_network not in ('ERICKSON')
    and a.st_abbr_cd = a.market_fnl
    and a.prov_prtcp_sts_cd = 'P'
    and substring(coalesce(a.bil_typ_cd,'0'), 1, 1) != '3'
    and a.ama_pl_of_srvc_cd <> '12'
    and (
        a.proc_cd in
        ('92507','92508','92526','97012','97016','97018','97022','97024','97026','97028',
         '97032','97033','97034','97035','97036','97039','97110','97112','97113','97116',
         '97124','97139','97140','97150','97164','97168','97530','97533','97535','97537',
         '97542','97545','97546','97750','97755','97760','97761','97799','G0283',
         '98940','98941','98942')
        or a.rvnu_cd in
        ('0430','0431','0432','0433','0434','0439','0420','0421','0422','0423','0424','0429',
         '0440','0441','0442','0443','0444','0449')
    )
    and a.proc_cd not in
    ('92630','92633','97001','97002','97003','97004','97545','97546','98943','G0129','G0151',
     'G0152','G9041','G9043','G9044','S9128','S9129','S9131')
group by
    a.gal_mbi_hicn_fnl
    , a.component
    , a.eventkey
    , a.hce_service_code
    , a.fst_srvc_dt
    , a.fst_srvc_month
    , a.fst_srvc_qtr
    , date_trunc('month', dateadd(day, 10, a.adjd_dt))
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
    , case when b.ever_lopa is not null then 1 else 0 end
    , case when b.still_lopa_mbi_dos is not null then 1 else 0 end
    , case when b.overturn_lopa_mbi_dos is not null then 1 else 0 end
    , case when a.clm_apc_flg = 1 and c.rsn_cd in ('208','176','943') then 1 else 0 end
    , case
        when a.proc_cd in ('98940','98941','98942') and a.component = 'PR' then 'Chiro'
        when a.ama_pl_of_srvc_cd in ('11','49') then 'Office'
        when a.ama_pl_of_srvc_cd in ('22','62','19','24') and a.component = 'OP' then 'OP_REHAB'
        else 'Other'
	end
;