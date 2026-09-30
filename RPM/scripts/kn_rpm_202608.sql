/*==============================================================================
 * RPM Spend by Diagnosis
 * Pulls RPM procedure claims from all 6 fichsrv entities, classifies DX
 * positions (HF > DM > OTHER), derives population segments.
 *
 * Output:
 *   tmp_1m.kn_rpm_202608_detail  -- claim-line level with provider fields
 *==============================================================================*/
select distinct fst_srvc_year from fichsrv.glxy_pr_f;

create or replace table tmp_1m.kn_rpm_202608_detail as

with dx_map as (
    select v.icd_code, v.condition
    from (values
        -- Diabetes Mellitus (E08x)
        ('E0800','Diabetes Mellitus'),('E0801','Diabetes Mellitus'),('E0821','Diabetes Mellitus'),('E0822','Diabetes Mellitus'),('E0829','Diabetes Mellitus')
        ,('E08319','Diabetes Mellitus'),('E083211','Diabetes Mellitus'),('E083299','Diabetes Mellitus'),('E0836','Diabetes Mellitus'),('E0837X3','Diabetes Mellitus')
        ,('E0840','Diabetes Mellitus'),('E0841','Diabetes Mellitus'),('E0842','Diabetes Mellitus'),('E0843','Diabetes Mellitus'),('E0844','Diabetes Mellitus')
        ,('E0851','Diabetes Mellitus'),('E0859','Diabetes Mellitus'),('E08610','Diabetes Mellitus'),('E08620','Diabetes Mellitus'),('E08621','Diabetes Mellitus')
        ,('E08622','Diabetes Mellitus'),('E08649','Diabetes Mellitus'),('E0865','Diabetes Mellitus'),('E0869','Diabetes Mellitus'),('E088','Diabetes Mellitus')
        ,('E089','Diabetes Mellitus')
        -- Diabetes Mellitus (E09x)
        ,('E0922','Diabetes Mellitus'),('E0965','Diabetes Mellitus'),('E098','Diabetes Mellitus'),('E099','Diabetes Mellitus')
        -- Diabetes Mellitus (E10x)
        ,('E1010','Diabetes Mellitus'),('E1011','Diabetes Mellitus'),('E1021','Diabetes Mellitus'),('E1022','Diabetes Mellitus'),('E1029','Diabetes Mellitus')
        ,('E10311','Diabetes Mellitus'),('E10319','Diabetes Mellitus'),('E103293','Diabetes Mellitus'),('E103299','Diabetes Mellitus'),('E103313','Diabetes Mellitus')
        ,('E103593','Diabetes Mellitus'),('E1039','Diabetes Mellitus'),('E1040','Diabetes Mellitus'),('E1041','Diabetes Mellitus'),('E1042','Diabetes Mellitus')
        ,('E1043','Diabetes Mellitus'),('E1049','Diabetes Mellitus'),('E1051','Diabetes Mellitus'),('E1052','Diabetes Mellitus'),('E1059','Diabetes Mellitus')
        ,('E10610','Diabetes Mellitus'),('E10618','Diabetes Mellitus'),('E10620','Diabetes Mellitus'),('E10621','Diabetes Mellitus'),('E10628','Diabetes Mellitus')
        ,('E10641','Diabetes Mellitus'),('E10649','Diabetes Mellitus'),('E1065','Diabetes Mellitus'),('E1069','Diabetes Mellitus'),('E108','Diabetes Mellitus')
        ,('E109','Diabetes Mellitus'),('E10A0','Diabetes Mellitus')
        -- Diabetes Mellitus (E11x)
        ,('E1100','Diabetes Mellitus'),('E1101','Diabetes Mellitus'),('E1110','Diabetes Mellitus'),('E1111','Diabetes Mellitus'),('E1121','Diabetes Mellitus')
        ,('E1122','Diabetes Mellitus'),('E1129','Diabetes Mellitus'),('E11311','Diabetes Mellitus'),('E11319','Diabetes Mellitus'),('E113212','Diabetes Mellitus')
        ,('E113213','Diabetes Mellitus'),('E113219','Diabetes Mellitus'),('E113291','Diabetes Mellitus'),('E113292','Diabetes Mellitus'),('E113293','Diabetes Mellitus')
        ,('E113299','Diabetes Mellitus'),('E113311','Diabetes Mellitus'),('E113312','Diabetes Mellitus'),('E113313','Diabetes Mellitus'),('E113391','Diabetes Mellitus')
        ,('E113393','Diabetes Mellitus'),('E113399','Diabetes Mellitus'),('E113411','Diabetes Mellitus'),('E113412','Diabetes Mellitus'),('E113493','Diabetes Mellitus')
        ,('E113499','Diabetes Mellitus'),('E113511','Diabetes Mellitus'),('E113512','Diabetes Mellitus'),('E113513','Diabetes Mellitus'),('E113519','Diabetes Mellitus')
        ,('E113539','Diabetes Mellitus'),('E113541','Diabetes Mellitus'),('E113553','Diabetes Mellitus'),('E113592','Diabetes Mellitus'),('E113593','Diabetes Mellitus')
        ,('E113599','Diabetes Mellitus'),('E1136','Diabetes Mellitus'),('E1137X3','Diabetes Mellitus'),('E1137X9','Diabetes Mellitus'),('E1139','Diabetes Mellitus')
        ,('E1140','Diabetes Mellitus'),('E1141','Diabetes Mellitus'),('E1142','Diabetes Mellitus'),('E1143','Diabetes Mellitus'),('E1144','Diabetes Mellitus')
        ,('E1149','Diabetes Mellitus'),('E1151','Diabetes Mellitus'),('E1152','Diabetes Mellitus'),('E1159','Diabetes Mellitus'),('E11610','Diabetes Mellitus')
        ,('E11618','Diabetes Mellitus'),('E11620','Diabetes Mellitus'),('E11621','Diabetes Mellitus'),('E11622','Diabetes Mellitus'),('E11628','Diabetes Mellitus')
        ,('E11641','Diabetes Mellitus'),('E11649','Diabetes Mellitus'),('E1165','Diabetes Mellitus'),('E1169','Diabetes Mellitus'),('E118','Diabetes Mellitus')
        ,('E119','Diabetes Mellitus')
        -- Diabetes Mellitus (E13x)
        ,('E1300','Diabetes Mellitus'),('E1321','Diabetes Mellitus'),('E1322','Diabetes Mellitus'),('E1329','Diabetes Mellitus'),('E13319','Diabetes Mellitus')
        ,('E133593','Diabetes Mellitus'),('E1337X9','Diabetes Mellitus'),('E1340','Diabetes Mellitus'),('E1342','Diabetes Mellitus'),('E1343','Diabetes Mellitus')
        ,('E1349','Diabetes Mellitus'),('E1351','Diabetes Mellitus'),('E13621','Diabetes Mellitus'),('E1365','Diabetes Mellitus'),('E1369','Diabetes Mellitus')
        ,('E138','Diabetes Mellitus'),('E139','Diabetes Mellitus')
        -- Heart Failure (I50x)
        ,('I501','Heart Failure'),('I5020','Heart Failure'),('I5021','Heart Failure'),('I5022','Heart Failure'),('I5023','Heart Failure')
        ,('I5030','Heart Failure'),('I5031','Heart Failure'),('I5032','Heart Failure'),('I5033','Heart Failure'),('I5040','Heart Failure')
        ,('I5041','Heart Failure'),('I5042','Heart Failure'),('I5043','Heart Failure'),('I50810','Heart Failure'),('I50811','Heart Failure')
        ,('I50812','Heart Failure'),('I50813','Heart Failure'),('I5082','Heart Failure'),('I5084','Heart Failure'),('I5089','Heart Failure')
        ,('I509','Heart Failure')
        -- Hypertension (I10-I1A)
        ,('I10','Hypertension')
        ,('I110','Hypertension'),('I119','Hypertension')
        ,('I120','Hypertension'),('I129','Hypertension')
        ,('I130','Hypertension'),('I1310','Hypertension'),('I1311','Hypertension'),('I132','Hypertension')
        ,('I150','Hypertension'),('I151','Hypertension'),('I152','Hypertension'),('I158','Hypertension'),('I159','Hypertension')
        ,('I160','Hypertension'),('I161','Hypertension'),('I169','Hypertension')
        ,('I1A0','Hypertension')
    ) as v(icd_code, condition)
)

, claims as (
    -- COSMOS OP
    select
        'COSMOS' as entity
        , 'OP' as component
        , eventkey as visit_id
        , clm_aud_nbr
        , sub_aud_nbr
        , dtl_ln_nbr
        , clm_rec_cd
        , service_code
        , fst_srvc_dt
        , fst_srvc_month
        , fst_srvc_qtr
        , fst_srvc_year
        , gal_mbi_hicn_fnl as mbi
        , bth_dt
        , gdr_cd
        , proc_cd
        , proc_mod1_cd
        , proc_mod2_cd
        , rvnu_cd
        , primary_diag_cd
        , icd_2
        , icd_3
        , icd_4
        , icd_5
        , icd_ver_cd
        , ahrq_diag_genl_catgy_desc
        , ahrq_diag_dtl_catgy_desc
        , prov_tin
        , mpin
        , srvc_prov_id
        , srvc_prov_npi_nbr
        , full_nm
        , prov_prtcp_sts_cd
        , srvc_prov_npi_fnl
        , bil_prov_npi_nbr
        , cos_prov_spcl_cd
        , crdntl_desc
        , clm_pl_of_srvc_desc
        , adr_ln_1_txt
        , cty_nm as prov_city
        , zip_cd as prov_zip
        , delegated_entity
        , null as provider_group_name
        , special_network
        , catgy_rol_up_2_desc
        , null as tadm_prov_spec_cat
        , brand_fnl
        , plan_level_1_fnl
        , plan_level_2_fnl
        , product_level_1_fnl
        , product_level_2_fnl
        , product_level_3_fnl
        , region_fnl
        , market_fnl
        , fin_state
        , fin_submarket
        , segment_name_fnl
        , contractpbp_fnl
        , contract_fnl
        , global_cap
        , group_ind_fnl
        , tfm_include_flag
        , migration_source
        , st_abbr_cd
        , adjd_dt
        , adjd_qtr
        , clm_pd_dt
        , bil_recv_dt
        , tadm_units
        , sbmt_chrg_amt
        , allw_amt_fnl
        , net_pd_amt_fnl
        , case when clm_dnl_f in ('D', 'Y') then 'Denied'
               when clm_dnl_f = 'N' then 'Paid'
          end as denial_flag
    from fichsrv.glxy_op_f
    where brand_fnl in ('M&R', 'C&S')
        and global_cap = 'NA'
        and fst_srvc_year >= '2021'
        and proc_cd in ('99453','99445','99454','99470','99457','99458','99091','G0322')

    union all

    -- COSMOS PR
    select
        'COSMOS' as entity
        , 'PR' as component
        , eventkey as visit_id
        , site_clm_aud_nbr as clm_aud_nbr
        , sub_aud_nbr
        , dtl_ln_nbr
        , clm_rec_cd
        , service_code
        , fst_srvc_dt
        , fst_srvc_month
        , fst_srvc_qtr
        , fst_srvc_year
        , gal_mbi_hicn_fnl as mbi
        , bth_dt
        , gdr_cd
        , proc_cd
        , proc_mod1_cd
        , proc_mod2_cd
        , rvnu_cd
        , primary_diag_cd
        , icd_2
        , icd_3
        , icd_4
        , icd_5
        , icd_ver_cd
        , ahrq_diag_genl_catgy_desc
        , ahrq_diag_dtl_catgy_desc
        , prov_tin
        , mpin
        , srvc_prov_id
        , srvc_prov_npi_nbr
        , full_nm
        , prov_prtcp_sts_cd
        , srvc_prov_npi_fnl
        , bil_prov_npi_nbr
        , cos_prov_spcl_cd
        , crdntl_desc
        , clm_pl_of_srvc_desc
        , adr_ln_1_txt
        , cty_nm as prov_city
        , zip_cd as prov_zip
        , delegated_entity
        , null as provider_group_name
        , special_network
        , catgy_rol_up_2_desc
        , tadm_prov_spec_cat
        , brand_fnl
        , plan_level_1_fnl
        , plan_level_2_fnl
        , product_level_1_fnl
        , product_level_2_fnl
        , product_level_3_fnl
        , region_fnl
        , market_fnl
        , fin_state
        , fin_submarket
        , segment_name_fnl
        , contractpbp_fnl
        , contract_fnl
        , global_cap
        , group_ind_fnl
        , tfm_include_flag
        , migration_source
        , st_abbr_cd
        , adjd_dt
        , adjd_qtr
        , clm_pd_dt
        , bil_recv_dt
        , tadm_units
        , sbmt_chrg_amt
        , allw_amt_fnl
        , net_pd_amt_fnl
        , case when clm_dnl_f in ('D', 'Y') then 'Denied'
               when clm_dnl_f = 'N' then 'Paid'
          end as denial_flag
    from fichsrv.glxy_pr_f
    where brand_fnl in ('M&R', 'C&S')
        and global_cap = 'NA'
        and fst_srvc_year >= '2021'
        and proc_cd in ('99453','99445','99454','99470','99457','99458','99091','G0322')

    union all

    -- CSP OP
    select
        'CSP' as entity
        , 'OP' as component
        , eventkey as visit_id
        , clm_aud_nbr
        , sub_aud_nbr
        , dtl_ln_nbr
        , clm_rec_cd
        , service_code
        , fst_srvc_dt
        , fst_srvc_month
        , fst_srvc_qtr
        , fst_srvc_year
        , gal_mbi_hicn_fnl as mbi
        , bth_dt
        , gdr_cd
        , proc_cd
        , proc_mod1_cd
        , proc_mod2_cd
        , rvnu_cd
        , primary_diag_cd
        , icd_2
        , icd_3
        , icd_4
        , icd_5
        , icd_ver_cd
        , ahrq_diag_genl_catgy_desc
        , ahrq_diag_dtl_catgy_desc
        , tin as prov_tin
        , mpin
        , srvc_prov_id
        , srvc_prov_npi_nbr
        , full_nm
        , prov_prtcp_sts_cd
        , null as srvc_prov_npi_fnl
        , null as bil_prov_npi_nbr
        , cos_prov_spcl_cd
        , null as crdntl_desc
        , clm_pl_of_srvc_desc
        , adr_ln_1_txt
        , cty_nm as prov_city
        , zip_cd as prov_zip
        , delegated_entity
        , null as provider_group_name
        , special_network
        , null as catgy_rol_up_2_desc
        , null as tadm_prov_spec_cat
        , brand_fnl
        , plan_level_1_fnl
        , plan_level_2_fnl
        , product_level_1_fnl
        , product_level_2_fnl
        , product_level_3_fnl
        , region_fnl
        , market_fnl
        , fin_state
        , fin_submarket
        , segment_name_fnl
        , contractpbp_fnl
        , contract_fnl
        , global_cap
        , group_ind_fnl
        , tfm_include_flag
        , migration_source
        , st_abbr_cd
        , adjd_dt
        , adjd_qtr
        , clm_pd_dt
        , bill_recv_dt as bil_recv_dt
        , tadm_units
        , sbmt_chrg_amt
        , allw_amt_fnl
        , net_pd_amt_fnl
        , case when clm_dnl_f in ('D', 'Y') then 'Denied'
               when clm_dnl_f = 'N' then 'Paid'
          end as denial_flag
    from fichsrv.dcsp_op_f
    where brand_fnl in ('M&R', 'C&S')
        and global_cap = 'NA'
        and fst_srvc_year >= '2021'
        and proc_cd in ('99453','99445','99454','99470','99457','99458','99091','G0322')

    union all

    -- CSP PR
    select
        'CSP' as entity
        , 'PR' as component
        , eventkey as visit_id
        , clm_aud_nbr
        , sub_aud_nbr
        , dtl_ln_nbr
        , clm_rec_cd
        , service_code
        , fst_srvc_dt
        , fst_srvc_month
        , fst_srvc_qtr
        , fst_srvc_year
        , gal_mbi_hicn_fnl as mbi
        , bth_dt
        , gdr_cd
        , proc_cd
        , proc_mod1_cd
        , proc_mod2_cd
        , rvnu_cd
        , primary_diag_cd
        , icd_2
        , icd_3
        , icd_4
        , icd_5
        , icd_ver_cd
        , ahrq_diag_genl_catgy_desc
        , ahrq_diag_dtl_catgy_desc
        , tin as prov_tin
        , mpin
        , srvc_prov_id
        , srvc_prov_npi_nbr
        , full_nm
        , prov_prtcp_sts_cd
        , null as srvc_prov_npi_fnl
        , null as bil_prov_npi_nbr
        , cos_prov_spcl_cd
        , null as crdntl_desc
        , clm_pl_of_srvc_desc
        , adr_ln_1_txt
        , cty_nm as prov_city
        , zip_cd as prov_zip
        , delegated_entity
        , provider_group_name
        , special_network
        , null as catgy_rol_up_2_desc
        , tadm_prov_spec_cat
        , brand_fnl
        , plan_level_1_fnl
        , plan_level_2_fnl
        , product_level_1_fnl
        , product_level_2_fnl
        , product_level_3_fnl
        , region_fnl
        , market_fnl
        , fin_state
        , fin_submarket
        , segment_name_fnl
        , contractpbp_fnl
        , contract_fnl
        , global_cap
        , group_ind_fnl
        , tfm_include_flag
        , migration_source
        , st_abbr_cd
        , adjd_dt
        , adjd_qtr
        , clm_pd_dt
        , bill_recv_dt as bil_recv_dt
        , tadm_units
        , sbmt_chrg_amt
        , allw_amt_fnl
        , net_pd_amt_fnl
        , case when clm_dnl_f in ('D', 'Y') then 'Denied'
               when clm_dnl_f = 'N' then 'Paid'
          end as denial_flag
    from fichsrv.dcsp_pr_f
    where brand_fnl in ('M&R', 'C&S')
        and global_cap = 'NA'
        and fst_srvc_year >= '2021'
        and proc_cd in ('99453','99445','99454','99470','99457','99458','99091','G0322')

    union all

    -- NICE OP
    select
        'NICE' as entity
        , 'OP' as component
        , eventkey as visit_id
        , clm_aud_nbr
        , sub_aud_nbr
        , dtl_ln_nbr
        , clm_rec_cd
        , service_code
        , fst_srvc_dt
        , fst_srvc_month
        , fst_srvc_qtr
        , fst_srvc_year
        , mbi_hicn_fnl as mbi
        , bth_dt
        , gdr_cd
        , proc_cd
        , proc_mod1_cd
        , proc_mod2_cd
        , rvnu_cd
        , primary_diag_cd
        , icd_2
        , icd_3
        , icd_4
        , icd_5
        , icd_ver_cd
        , ahrq_diag_genl_catgy_desc
        , ahrq_diag_dtl_catgy_desc
        , tin as prov_tin
        , mpin
        , srvc_prov_id
        , srvc_prov_npi_nbr
        , full_nm
        , prov_prtcp_sts_cd
        , null as srvc_prov_npi_fnl
        , null as bil_prov_npi_nbr
        , null as cos_prov_spcl_cd
        , crdntl_desc
        , clm_pl_of_srvc_desc
        , adr_ln_1_txt
        , city_nm as prov_city
        , zip_cd as prov_zip
        , delegated_entity
        , provider_group_name
        , null as special_network
        , catgy_rol_up_2_desc
        , null as tadm_prov_spec_cat
        , brand_fnl
        , plan_level_1_fnl
        , plan_level_2_fnl
        , product_level_1_fnl
        , product_level_2_fnl
        , product_level_3_fnl
        , region_fnl
        , market_fnl
        , state_fnl as fin_state
        , fin_submarket_fnl as fin_submarket
        , segment_name_fnl
        , contractpbp_fnl
        , contract_fnl
        , iff(clm_cap_flag = 'FFS', 'NA', 'ENC') as global_cap
        , group_ind_fnl
        , tfm_include_flag
        , 'NA' as migration_source
        , st_abbr_cd
        , adjd_dt
        , adjd_qtr
        , clm_pd_dt
        , bil_recv_dt
        , null as tadm_units
        , sbmt_chrg_amt
        , allw_amt as allw_amt_fnl
        , net_pd_amt as net_pd_amt_fnl
        , case when dnl_f = 'Y' then 'Denied'
               when dnl_f = 'N' then 'Paid'
          end as denial_flag
    from fichsrv.nce_op_f
    where brand_fnl in ('M&R', 'C&S')
        and clm_cap_flag = 'FFS'
        and fst_srvc_year >= '2021'
        and proc_cd in ('99453','99445','99454','99470','99457','99458','99091','G0322')

    union all

    -- NICE PR
    select
        'NICE' as entity
        , 'PR' as component
        , eventkey as visit_id
        , clm_aud_nbr
        , sub_aud_nbr
        , dtl_ln_nbr
        , clm_rec_cd
        , service_code
        , fst_srvc_dt
        , fst_srvc_month
        , fst_srvc_qtr
        , fst_srvc_year
        , mbi_hicn_fnl as mbi
        , bth_dt
        , gdr_cd
        , proc_cd
        , proc_mod1_cd
        , proc_mod2_cd
        , rvnu_cd
        , primary_diag_cd
        , icd_2
        , icd_3
        , icd_4
        , icd_5
        , icd_ver_cd
        , ahrq_diag_genl_catgy_desc
        , ahrq_diag_dtl_catgy_desc
        , tin as prov_tin
        , mpin
        , srvc_prov_id
        , srvc_prov_npi_nbr
        , full_nm
        , prov_prtcp_sts_cd
        , null as srvc_prov_npi_fnl
        , null as bil_prov_npi_nbr
        , cos_prov_spcl_cd
        , crdntl_desc
        , clm_pl_of_srvc_desc
        , adr_ln_1_txt
        , city_nm as prov_city
        , zip_cd as prov_zip
        , delegated_entity
        , provider_group_name
        , null as special_network
        , catgy_rol_up_2_desc
        , tadm_prov_spec_cat
        , brand_fnl
        , plan_level_1_fnl
        , plan_level_2_fnl
        , product_level_1_fnl
        , product_level_2_fnl
        , product_level_3_fnl
        , region_fnl
        , market_fnl
        , state_fnl as fin_state
        , fin_submarket_fnl as fin_submarket
        , segment_name_fnl
        , contractpbp_fnl
        , contract_fnl
        , iff(clm_cap_flag = 'FFS', 'NA', 'ENC') as global_cap
        , group_ind_fnl
        , tfm_include_flag
        , 'NA' as migration_source
        , st_abbr_cd
        , adjd_dt
        , adjd_qtr
        , clm_pd_dt
        , bil_recv_dt
        , tadm_units
        , sbmt_chrg_amt
        , calc_allw as allw_amt_fnl
        , calc_net_pd as net_pd_amt_fnl
        , case when dnl_f = 'Y' then 'Denied'
               when dnl_f = 'N' then 'Paid'
          end as denial_flag
    from fichsrv.nce_pr_f
    where brand_fnl in ('M&R', 'C&S')
        and clm_cap_flag = 'FFS'
        and fst_srvc_year >= '2021'
        and proc_cd in ('99453','99445','99454','99470','99457','99458','99091','G0322')
)

, dx_classified as (
    select
        c.*
        , coalesce(d1.condition, 'OTHER DIAGNOSIS') as prim_dx_condition
        , coalesce(d2.condition, 'OTHER DIAGNOSIS') as dx_condition_2
        , coalesce(d3.condition, 'OTHER DIAGNOSIS') as dx_condition_3
        , coalesce(d4.condition, 'OTHER DIAGNOSIS') as dx_condition_4
        , coalesce(d5.condition, 'OTHER DIAGNOSIS') as dx_condition_5
    from claims as c
    left join dx_map as d1 on c.primary_diag_cd = d1.icd_code
    left join dx_map as d2 on c.icd_2 = d2.icd_code
    left join dx_map as d3 on c.icd_3 = d3.icd_code
    left join dx_map as d4 on c.icd_4 = d4.icd_code
    left join dx_map as d5 on c.icd_5 = d5.icd_code
)

select
    d.*
    , t.collection as hospital_group
    , case
        when d.migration_source = 'OAH'
            and not (d.brand_fnl = 'C&S' and d.fst_srvc_year = '2024' and d.st_abbr_cd = 'MD')
            and not (d.brand_fnl != 'C&S' and d.fst_srvc_year = '2024' and d.market_fnl = 'MD')
        then 'OAH'
        when d.brand_fnl = 'M&R'
            and d.product_level_3_fnl = 'INSTITUTIONAL'
        then 'M&R ISNP'
        when d.entity in ('COSMOS', 'NICE')
            and d.brand_fnl = 'M&R'
            and d.global_cap = 'NA'
            and d.product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL')
            and d.tfm_include_flag = 1
        then 'M&R FFS'
        when d.entity in ('COSMOS', 'CSP')
            and d.global_cap = 'NA'
            and (
                (d.brand_fnl = 'C&S' and d.migration_source != 'OAH' and d.product_level_3_fnl = 'DUAL')
                or (d.brand_fnl = 'C&S' and d.fst_srvc_year = '2024' and d.migration_source = 'OAH' and d.st_abbr_cd = 'MD')
                or (d.brand_fnl != 'C&S' and d.fst_srvc_year = '2024' and d.migration_source = 'OAH' and d.market_fnl = 'MD')
            )
        then 'C&S DSNP'
        when d.brand_fnl = 'M&R'
            and d.product_level_3_fnl = 'DUAL'
        then 'M&R DSNP'
        else 'N/A'
      end as population
    , case
        when d.proc_cd in ('99453') then 'Education and Setup'
        when d.proc_cd in ('99445', '99454') then 'Device Supply'
        when d.proc_cd in ('99457', '99458', '99470', '99091', 'G0322') then 'Treatment and Management'
      end as rpm_proc_cat
    , case
        when 'Heart Failure' in (d.prim_dx_condition, d.dx_condition_2, d.dx_condition_3, d.dx_condition_4, d.dx_condition_5)
        then 'Heart Failure'
        when 'Diabetes Mellitus' in (d.prim_dx_condition, d.dx_condition_2, d.dx_condition_3, d.dx_condition_4, d.dx_condition_5)
        then 'Diabetes Mellitus'
        when 'Hypertension' in (d.prim_dx_condition, d.dx_condition_2, d.dx_condition_3, d.dx_condition_4, d.dx_condition_5)
        then 'Hypertension'
        else 'NOT A VALID DX'
      end as dx_flag_final
from dx_classified as d
left join tmp_1y.tin_collection as t
    on d.prov_tin = t.tin
where d.denial_flag = 'Paid'
;


/*------------------------------------------------------------------------------
 * QA
 *------------------------------------------------------------------------------*/

select count(*) as row_count, count(distinct mbi) as distinct_mbrs
from tmp_1m.kn_rpm_202608_detail;

select entity, component, count(*) as row_count, sum(allw_amt_fnl) as allowed
from tmp_1m.kn_rpm_202608_detail
group by 1, 2
order by 1, 2;

select population, count(*) as row_count, count(distinct mbi) as distinct_mbrs, sum(allw_amt_fnl) as allowed
from tmp_1m.kn_rpm_202608_detail
group by 1
order by 4 desc;

select dx_flag_final, count(*) as row_count, count(distinct mbi) as distinct_mbrs, sum(allw_amt_fnl) as allowed
from tmp_1m.kn_rpm_202608_detail
group by 1
order by 4 desc;
