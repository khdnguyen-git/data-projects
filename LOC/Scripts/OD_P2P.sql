
@set notifications_date = 04082026


select distinct entity from  tmp_1m.ec_ip_dataset_notif_${notifications_date}_od;

create or replace table tmp_1m.kn_loc_${notifications_date} as
select 
	admit_week
	, hce_admit_month as admit_act_month
	, total_oah_flag
	, institutional_flag
	, fin_tfm_product_new
	, sgr_source_name
	, nce_tadm_dec_risk_type
	, fin_market
	, fin_brand
	, group_name
	, group_number
	, prov_tin
	, par_nonpar
	, hospital_group
	, los_categories
	, respiratory_flag
	, mnr_cosmos_ffs_flag
	, leading_ind_pop
	, mnr_nice_ffs_flag
	, mnr_total_ffs_flag
	, mnr_oah_flag
	, cns_oah_flag
	, mnr_dual_flag
	, cns_dual_flag
	, ocm_migration
	, component
	, entity
	, sum(case_count) as case_count
	, sum(Initial_ADR_cnt) as Initial_ADR_cnt
	, sum(persistent_adr_cnt) as persistent_adr_cnt
	, sum(md_reviewed_cnt) as md_reviewed_cnt
	, sum(appeal_case_cnt) as appeal_case_cnt
	, sum(appeal_ovrtn_case_cnt) as appeal_ovrtn_case_cnt
	, sum(mcr_reconsideration_case_cnt) as mcr_reconsideration_case_cnt
	, sum(mcr_ovrtn_case_cnt) as mcr_ovrtn_case_cnt
	, sum(p2p_case_cnt) as p2p_case_cnt
	, sum(p2p_ovrtn_case_cnt) as p2p_ovrtn_case_cnt
	, sum(other_ovtrns) as other_ovtrns
	, sum(member_appeal_cnt) AS member_appeal_cnt
	, sum(member_appeal_ovtn_cnt) AS member_appeal_ovtn_cnt
	, sum(membership) as membership
from tmp_1m.ec_ip_dataset_notif_${notifications_date}_od
where ipa_pac_flag in ('IPA','MM') 
	and hce_admit_month > '202212'
	and loc_flag = 1
group by 
	admit_week
	, hce_admit_month
	, total_oah_flag
	, institutional_flag
	, fin_tfm_product_new
	, sgr_source_name
	, nce_tadm_dec_risk_type
	, fin_market
	, fin_brand
	, group_name
	, group_number
	, prov_tin
	, par_nonpar
	, hospital_group
	, los_categories
	, respiratory_flag
	, mnr_cosmos_ffs_flag
	, leading_ind_pop
	, mnr_nice_ffs_flag
	, mnr_total_ffs_flag
	, mnr_oah_flag
	, cns_oah_flag
	, mnr_dual_flag
	, cns_dual_flag
	, ocm_migration
	, component
	, entity
;



create or replace table tmp_1m.kn_loc_od_p2p_${notifications_date} as 
select
	admit_act_month
	, admit_week
	, group_name
	, hospital_group
	, fin_market
	, prov_tin
	, sum(case_count) as case_count
    , sum(Initial_ADR_cnt) as initial_adr_count
    , sum(persistent_adr_cnt) as persistent_adr_cnt
    , sum(md_reviewed_cnt) as md_reviewed_cnt
    , sum(appeal_case_cnt) as appeal_case_cnt
    , sum(appeal_ovrtn_case_cnt) as appeal_ovrtn_case_cnt
    , sum(mcr_reconsideration_case_cnt) as mcr_reconsideration_case_cnt
    , sum(mcr_ovrtn_case_cnt) as mcr_ovrtn_case_cnt
    , sum(p2p_case_cnt) as p2p_case_cnt
    , sum(p2p_ovrtn_case_cnt) as p2p_ovrtn_case_cnt
    , sum(member_appeal_cnt) as member_appeal_cnt
    , sum(member_appeal_ovtn_cnt) as member_appeal_ovtn_cnt
    , sum(membership) as membership
from tmp_1m.kn_loc_${notifications_date}
where admit_act_month >= '202501'
	and total_oah_flag = 'Non-OAH' 
	and INSTITUTIONAL_FLAG = 'Non-Institutional' 
	and MNR_TOTAL_FFS_FLAG = 1
group by 1,2,3,4,5,6


create or replace table tmp_1m.kn_loc_od_p2p_${notifications_date}_ranked as 
with base as (
    select
        admit_act_month
        , admit_week
        , group_name
        , hospital_group
        , fin_market
        , prov_tin
        , sum(case_count) as case_count
        , sum(initial_adr_cnt) as initial_adr_count
        , sum(persistent_adr_cnt) as persistent_adr_cnt
        , sum(md_reviewed_cnt) as md_reviewed_cnt
        , sum(appeal_case_cnt) as appeal_case_cnt
        , sum(appeal_ovrtn_case_cnt) as appeal_ovrtn_case_cnt
        , sum(mcr_reconsideration_case_cnt) as mcr_reconsideration_case_cnt
        , sum(mcr_ovrtn_case_cnt) as mcr_ovrtn_case_cnt
        , sum(p2p_case_cnt) as p2p_case_cnt
        , sum(p2p_ovrtn_case_cnt) as p2p_ovrtn_case_cnt
        , sum(member_appeal_cnt) as member_appeal_cnt
        , sum(member_appeal_ovtn_cnt) as member_appeal_ovtn_cnt
        , sum(membership) as membership
    from tmp_1m.kn_loc_${notifications_date}
    where admit_act_month >= '202501'
        and total_oah_flag = 'Non-OAH'
        and institutional_flag = 'Non-Institutional'
        and mnr_total_ffs_flag = 1
    group by
        admit_act_month
        , admit_week
        , group_name
        , hospital_group
        , fin_market
        , prov_tin
),
hospital_totals as (
    select
        hospital_group
        , sum(member_appeal_cnt) as total_member_appeal_cnt
        , sum(p2p_case_cnt) as total_p2p_case_cnt
    from base
    group by hospital_group
),
ranked_hospitals as (
    select
        hospital_group
        , dense_rank() over (
            order by total_member_appeal_cnt desc
        ) as member_appeal_rank
        , dense_rank() over (
            order by total_p2p_case_cnt desc
        ) as p2p_rank
    from hospital_totals
)
select
    b.*
    , case
        when r.member_appeal_rank <= 12 then 1
        else 0
      end as top12_member_appeal
    , case
        when r.p2p_rank <= 12 then 1
        else 0
      end as top12_p2p
from base as b
left join ranked_hospitals as r
    on b.hospital_group = r.hospital_group;


select
	substring(hce_admit_month, 1, 4) as admit_year
	, prov_tin
	, hospital_group
	, loc_flag
	, total_oah_flag
	, institutional_flag
	, fin_tfm_product_new
	, sgr_source_name
	, nce_tadm_dec_risk_type
	, fin_market
	, fin_brand
	, group_name
	, los_categories
	, respiratory_flag
	, mnr_cosmos_ffs_flag
	, leading_ind_pop
	, mnr_nice_ffs_flag
	, mnr_total_ffs_flag
	, mnr_oah_flag
	, cns_oah_flag
	, mnr_dual_flag
	, cns_dual_flag
	, ocm_migration
	, ipa_pac_flag
	, component
	, sum(case_count) as case_count
	, sum(Initial_ADR_cnt) as Initial_ADR_cnt
	, sum(persistent_adr_cnt) as persistent_adr_cnt
	, sum(md_reviewed_cnt) as md_reviewed_cnt
	, sum(appeal_case_cnt) as appeal_case_cnt
	, sum(appeal_ovrtn_case_cnt) as appeal_ovrtn_case_cnt
	, sum(mcr_reconsideration_case_cnt) as mcr_reconsideration_case_cnt
	, sum(mcr_ovrtn_case_cnt) as mcr_ovrtn_case_cnt
	, sum(p2p_case_cnt) as p2p_case_cnt
	, sum(p2p_ovrtn_case_cnt) as p2p_ovrtn_case_cnt
	, sum(other_ovtrns) as other_ovtrns
	, sum(member_appeal_cnt) AS member_appeal_cnt
	, sum(member_appeal_ovtn_cnt) AS member_appeal_ovtn_cnt
	, sum(membership) as membership
from tmp_1m.ec_ip_dataset_notif_${notifications_date}_od
where hospital_group = 'KELSEY-SEYBOLD CLINIC' or prov_tin = '760386391'
group by
	substring(hce_admit_month, 1, 4)
	, prov_tin
	, hospital_group
	, loc_flag
	, total_oah_flag
	, institutional_flag
	, fin_tfm_product_new
	, sgr_source_name
	, nce_tadm_dec_risk_type
	, fin_market
	, fin_brand
	, group_name
	, los_categories
	, respiratory_flag
	, mnr_cosmos_ffs_flag
	, leading_ind_pop
	, mnr_nice_ffs_flag
	, mnr_total_ffs_flag
	, mnr_oah_flag
	, cns_oah_flag
	, mnr_dual_flag
	, cns_dual_flag
	, ocm_migration
	, ipa_pac_flag
	, component
;

select prov_tin from tmp_1m.ec_ip_dataset_${notifications_date}_4_od
where hospital_group = 'KELSEY-SEYBOLD CLINIC' or prov_tin = '760386391'

select prov_tin from tmp_1m.ec_ip_dataset_${notifications_date}_od
where prov_tin = '760386391'




select tin, collection from tmp_1y.tin_collection
where collection ilike '%KELSEY%'

select * from fichsrv.glxy_pr_f
where prov_tin = '760386391'



select 
	substr(fa_prov_id,2,9) as prov_tin
	, substring(admit_act_month, 1, 4) as admit_year
	, count(distinct case_id) as case_count					
    , count(distinct (case when initialfulladr_cases=1 then case_id end)) as Initial_ADR_cnt				
    , count(distinct (case when persistentfulladr_cases=1 then case_id end)) as Persistent_ADR_cnt					
    , count(distinct (case when icm_md_reviewed_ind=1 then case_id end)) as MD_Reviewed_cnt
    , count(distinct (case when initialfulladr_cases=1 AND Appeal_ind=1 then case_id  end )) as Appeal_case_cnt
    , count(distinct (case when Appeal_ovrtn_Ind=1 then case_id  end )) as Appeal_Ovrtn_case_cnt
    , count(distinct (case when mcr_reconsideration_ind=1 then case_id  end )) as MCR_Reconsideration_case_cnt
    , count(distinct (case when MCR_Ovtrn_ind=1  then case_id  end )) as MCR_Ovrtn_case_cnt
    , count(distinct (case when P2P_full_evertouched_cnt=1 then case_id  end )) as P2P_case_cnt
    , count(distinct (case when P2P_full_ovtn=1  then case_id  end )) as P2P_Ovrtn_case_cnt
from HCE_OPS_FNL.HCE_ADR_AVTAR_Like_25_26_f 
where substr(fa_prov_id,2,9) = '760386391'
group by 1,2,3


select 
	apptype
	, avtar_mtch_ind
	, proc_cd
	, prim_diag_cd
	, case_category_cd
	, auth_typ_cd
	, svc_cat_cd
	, svc_cat_dtl_cd 
	, prim_svc_palist
	, pa_program
	, business_segment
	, admit_cat_cd
	, svc_setting
	, plc_of_svc_cd
	, plc_of_svc_drv_cd
	, case_status_rsn_cd
	, case when a.svc_setting ='Inpatient' and a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('17 - Medical') then 'Medical'
		when a.svc_setting ='Inpatient' and  a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('30 - Surgical') then 'Surgical'
	 	when  a.plc_of_svc_cd<>'12 - Home' and a.case_cur_svc_cat_dtl_cd<>'51 - Custodial' and a.case_cur_svc_cat_dtl_cd in ('17 - Long Term Care','42 - Long Term Acute Care') 
	 		then 'LTAC'
	 	when a.plc_of_svc_cd<>'12 - Home' and a.case_cur_svc_cat_dtl_cd<>'51 - Custodial' and (a.case_cur_svc_cat_dtl_cd in ('31 - Skilled Nursing','46 - PAT Skilled Nursing') 
	 		or substr(a.plc_of_svc_cd,1,2) in ('31','16')) then 'SNF'
	 	when a.plc_of_svc_cd<>'12 - Home' and a.case_cur_svc_cat_dtl_cd<>'51 - Custodial' and a.case_cur_svc_cat_dtl_cd in ('35 - Therapy Services') and 
	 		substr(a.plc_of_svc_cd,1,2) in ('61','6') then 'AIR'
	 	else 'NA' end as IP_type
	 , case 	when a.svc_setting ='Inpatient' and a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('17 - Medical') then 1
		when a.svc_setting ='Inpatient' and  a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('30 - Surgical') then 1
		else 0 end as loc_flag
	, count(distinct case_id) 
from HCE_OPS_FNL.HCE_ADR_AVTAR_Like_25_26_f as a
where substr(fa_prov_id,2,9) = '760386391'
group by 	
	apptype
	, avtar_mtch_ind
	, proc_cd
	, prim_diag_cd
	, case_category_cd
	, auth_typ_cd
	, svc_cat_cd
	, svc_cat_dtl_cd
	, palist
	, pa_program
	, business_segment
	, admit_cat_cd
	, svc_setting
	, plc_of_svc_cd
	, plc_of_svc_drv_cd
	, case_status_rsn_cd
	, case when a.svc_setting ='Inpatient' and a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('17 - Medical') then 'Medical'
		when a.svc_setting ='Inpatient' and  a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('30 - Surgical') then 'Surgical'
	 	when  a.plc_of_svc_cd<>'12 - Home' and a.case_cur_svc_cat_dtl_cd<>'51 - Custodial' and a.case_cur_svc_cat_dtl_cd in ('17 - Long Term Care','42 - Long Term Acute Care') 
	 		then 'LTAC'
	 	when a.plc_of_svc_cd<>'12 - Home' and a.case_cur_svc_cat_dtl_cd<>'51 - Custodial' and (a.case_cur_svc_cat_dtl_cd in ('31 - Skilled Nursing','46 - PAT Skilled Nursing') 
	 		or substr(a.plc_of_svc_cd,1,2) in ('31','16')) then 'SNF'
	 	when a.plc_of_svc_cd<>'12 - Home' and a.case_cur_svc_cat_dtl_cd<>'51 - Custodial' and a.case_cur_svc_cat_dtl_cd in ('35 - Therapy Services') and 
	 		substr(a.plc_of_svc_cd,1,2) in ('61','6') then 'AIR'
	 	else 'NA' end
	 , case when a.svc_setting ='Inpatient' and a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('17 - Medical') then 1
		when a.svc_setting ='Inpatient' and  a.plc_of_svc_cd ='21 - Acute Hospital' and a.admit_cat_cd  in ('30 - Surgical') then 1
		else 0 end
;


tmp_1m.ec_ip_dataset_${notifications_date}_4_od

select 



select count(*) from tmp_1m.kn_loc_od_p2p_${notifications_date}_ranked

select * from tmp_1m.kn_loc_od_p2p_${notifications_date}_ranked    




select tin, collection, count(*) from tmp_1y.tin_collection
group by 1,2
--having count(*) > 1
order by 2,1



select hospital_group, fin_market from tmp_1m.ec_ip_dataset_04082026_3_od
where hospital_group ilike 'North Carolina'
group by 1,2

select 
	substring(admit_act_month, 1, 4) as admit_year
	, hospital_group
	, sum(case_count)
	, sum(persistent_adr_cnt)
from tmp_1m.kn_loc_${notifications_date}
where hospital_group ilike '%Seybold%'
group by 1, 2                                                                                                                                                                                                                                                                                                                                                                                            