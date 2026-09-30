/*------------------------------------------------------------------------------
 * KN_PRMM_SEMANTIC_BASE
 * Professional claims rollup base table for PR/MM semantic view.
 * Source: HCE_OPS_STAGE.HCEOPS_PA_TRCKNG_PR_CLM_RLUP_ADD
 * Filter: FST_SRVC_DT >= 2024-01-01
 * Grain: claim-line (EK_PROC near-unique)
 *------------------------------------------------------------------------------*/

create or replace table tmp_1m.kn_prmm_semantic_base as
select
    -- identifiers
    gal_mbi_hicn_fnl as mbi
    , site_clm_aud_nbr
    -- , eventkey
    -- , ek_proc

    -- time
    , fst_srvc_dt as srvc_dt
    , fst_srvc_month as srvc_month
    , fst_srvc_qtr as srvc_qtr
    , fst_srvc_year as srvc_year
    -- , adjd_dt
    -- , adjd_qtr
    , revised_adjd_dt
    , clm_pd_dt
    -- , bil_recv_dt

    -- amounts
    , clm_rec_rlup_allw_amt
    , clm_rec_rlp_net_pd_amt
    -- , clm_rec_rlp_util
    -- , sbmt_chrg_amt
    -- , tadm_units

    -- service
    -- , service_code
    , proc_cd
    -- , proc_mod1_cd
    -- , proc_mod1_desc
    -- , proc_mod2_cd
    -- , proc_mod2_desc
    -- , clm_pl_of_srvc_desc

    -- adjudication: original
    -- , fnl_rsn_cd
    -- , fnl_rsn_cd_desc
    -- , fnl_rsn_cd_sys_id
    -- , clm_lvl_rsn_cd
    -- , clm_lvl_rsn_cd_desc
    -- , clm_lvl_rsn_cd_sys_id
    -- , srvc_lvl_rsn_cd
    -- , srvc_lvl_rsn_cd_desc
    -- , srvc_lvl_rsn_cd_sys_id

    -- adjudication: revised
    -- , adj_fnl_rsn_cd
    -- , adj_fnl_rsn_desc
    -- , adj_fnl_rsn_cd_sys_id
    , revised_fnl_rsn_cd
    , revised_fnl_rsn_desc
    -- , revised_fnl_rsn_cd_sys_id

    -- provider
    , prov_tin
    -- , mpin
    , srvc_prov_id
    -- , full_nm
    , prov_prtcp_sts_cd
    -- , cos_prov_spcl_cd
    -- , tadm_prov_spec_cat

    -- member
    -- , bth_dt
    -- , gdr_cd

    -- geography
    -- , region_fnl
    , market_fnl
    -- , fin_submarket
    -- , fin_state

    -- plan
    -- , plan_level_1_fnl
    , plan_level_2_fnl
    -- , product_level_1_fnl
    , product_level_2_fnl
    , product_level_3_fnl
    -- , segment_name_fnl
    -- , contract_fnl
    , contractpbp_fnl
    -- , groupnumber

    -- diagnosis
    , primary_diag_cd
    , ahrq_diag_genl_catgy_desc
    , ahrq_diag_dtl_catgy_desc
    -- , icd_2
    -- , icd_3
    -- , icd_4
    -- , icd_5
    -- , icd_ver_cd

    -- derived: lopa (latest submission + revised rsn 26)
    , case when mbr_dos_latest_submission = 1 and revised_fnl_rsn_cd = '026' then 1 else 0 end as lopa_ind_new

    -- pre-built indicators
    -- , lopa_26_rsn_ind
    -- , lopa_292_rsn_ind
    -- , lopa_rsn_ind
    -- , clmrev_7000_2316_codes_ind
    -- , clmrev_71_codes_ind
    -- , unitsmax_373_ind
    -- , dnlmednec_558_ind
    -- , dnlaprvdsite_1650_ind
    , mbr_dos_latest_submission
    -- , rec_cd_02_f

    -- pop derivation intermediates
    , brand_fnl
    , global_cap
    , group_ind_fnl
    , tfm_include_flag
    , migration_source

    -- derived: denial flag
    , case
        when clm_dnl_f in ('D', 'Y') then 'Denied'
        when clm_dnl_f = 'N' then 'Paid'
      end as denial_flag

    -- derived: skin sub flag (hardcoded from kn_proc_category)
    , case when proc_cd in (
        -- Application (16)
        '15271','15272','15273','15274','15275','15276','15277','15278'
        ,'C5271','C5272','C5273','C5274','C5275','C5276','C5277','C5278'
        -- Covered (4)
        ,'Q4122','Q4133','Q4182','Q4186'
        -- Unproven: A-codes (34)
        ,'A2001','A2002','A2004','A2005','A2006','A2007','A2008','A2009','A2010','A2011'
        ,'A2012','A2013','A2014','A2015','A2016','A2017','A2018','A2019','A2021','A2026'
        ,'A2027','A2028','A2029','A2030','A2031','A2032','A2033','A2034','A2035'
        ,'A4100','A6021','A6022','A6023','A6024'
        -- Unproven: Q4 codes (245)
        ,'Q4100','Q4101','Q4102','Q4104','Q4105','Q4110','Q4111','Q4112','Q4114','Q4115'
        ,'Q4116','Q4117','Q4118','Q4121','Q4123','Q4124','Q4125','Q4126','Q4127','Q4128'
        ,'Q4130','Q4132','Q4134','Q4135','Q4136','Q4137','Q4138','Q4139','Q4140','Q4141'
        ,'Q4142','Q4143','Q4145','Q4146','Q4147','Q4148','Q4149','Q4150','Q4151','Q4152'
        ,'Q4153','Q4154','Q4155','Q4156','Q4157','Q4158','Q4159','Q4160','Q4161','Q4162'
        ,'Q4163','Q4164','Q4165','Q4166','Q4167','Q4168','Q4169','Q4170','Q4171','Q4173'
        ,'Q4174','Q4175','Q4176','Q4177','Q4178','Q4179','Q4180','Q4181','Q4183','Q4184'
        ,'Q4185','Q4187','Q4188','Q4189','Q4190','Q4191','Q4192','Q4193','Q4194','Q4195'
        ,'Q4196','Q4197','Q4198','Q4199','Q4200','Q4201','Q4202','Q4203','Q4204','Q4205'
        ,'Q4206','Q4208','Q4209','Q4210','Q4211','Q4212','Q4213','Q4214','Q4215','Q4216'
        ,'Q4217','Q4218','Q4219','Q4220','Q4221','Q4222','Q4224','Q4225','Q4226','Q4227'
        ,'Q4229','Q4230','Q4231','Q4232','Q4233','Q4234','Q4235','Q4236','Q4237','Q4238'
        ,'Q4239','Q4240','Q4241','Q4242','Q4245','Q4246','Q4247','Q4248','Q4249','Q4250'
        ,'Q4251','Q4252','Q4253','Q4254','Q4255','Q4256','Q4257','Q4258','Q4259','Q4260'
        ,'Q4261','Q4262','Q4263','Q4264','Q4265','Q4266','Q4267','Q4268','Q4269','Q4270'
        ,'Q4271','Q4272','Q4273','Q4274','Q4275','Q4276','Q4277','Q4278','Q4279','Q4280'
        ,'Q4281','Q4282','Q4283','Q4284','Q4287','Q4288','Q4289','Q4290','Q4291','Q4292'
        ,'Q4293','Q4294','Q4295','Q4296','Q4297','Q4298','Q4299','Q4300','Q4301','Q4302'
        ,'Q4303','Q4304','Q4305','Q4306','Q4307','Q4308','Q4309','Q4310','Q4311','Q4312'
        ,'Q4313','Q4314','Q4315','Q4316','Q4317','Q4318','Q4319','Q4320','Q4321','Q4322'
        ,'Q4323','Q4324','Q4325','Q4326','Q4327','Q4328','Q4329','Q4330','Q4331','Q4332'
        ,'Q4333','Q4334','Q4335','Q4336','Q4337','Q4338','Q4339','Q4340','Q4341','Q4342'
        ,'Q4343','Q4344','Q4345','Q4346','Q4347','Q4348','Q4349','Q4350','Q4351','Q4352'
        ,'Q4353','Q4354','Q4355','Q4356','Q4357','Q4358','Q4359','Q4360','Q4361','Q4362'
        ,'Q4363','Q4364','Q4365','Q4366','Q4367'
      ) then 'Y' else 'N'
      end as skin_sub_flag

    -- derived: skin sub subcategory
    , case
        when proc_cd in ('15271','15272','15273','15274','15275','15276','15277','15278'
                        ,'C5271','C5272','C5273','C5274','C5275','C5276','C5277','C5278')
          then 'Application'
        when proc_cd in ('Q4122','Q4133','Q4182','Q4186')
          then 'Covered'
        when proc_cd in (
            'A2001','A2002','A2004','A2005','A2006','A2007','A2008','A2009','A2010','A2011'
            ,'A2012','A2013','A2014','A2015','A2016','A2017','A2018','A2019','A2021','A2026'
            ,'A2027','A2028','A2029','A2030','A2031','A2032','A2033','A2034','A2035'
            ,'A4100','A6021','A6022','A6023','A6024'
            ,'Q4100','Q4101','Q4102','Q4104','Q4105','Q4110','Q4111','Q4112','Q4114','Q4115'
            ,'Q4116','Q4117','Q4118','Q4121','Q4123','Q4124','Q4125','Q4126','Q4127','Q4128'
            ,'Q4130','Q4132','Q4134','Q4135','Q4136','Q4137','Q4138','Q4139','Q4140','Q4141'
            ,'Q4142','Q4143','Q4145','Q4146','Q4147','Q4148','Q4149','Q4150','Q4151','Q4152'
            ,'Q4153','Q4154','Q4155','Q4156','Q4157','Q4158','Q4159','Q4160','Q4161','Q4162'
            ,'Q4163','Q4164','Q4165','Q4166','Q4167','Q4168','Q4169','Q4170','Q4171','Q4173'
            ,'Q4174','Q4175','Q4176','Q4177','Q4178','Q4179','Q4180','Q4181','Q4183','Q4184'
            ,'Q4185','Q4187','Q4188','Q4189','Q4190','Q4191','Q4192','Q4193','Q4194','Q4195'
            ,'Q4196','Q4197','Q4198','Q4199','Q4200','Q4201','Q4202','Q4203','Q4204','Q4205'
            ,'Q4206','Q4208','Q4209','Q4210','Q4211','Q4212','Q4213','Q4214','Q4215','Q4216'
            ,'Q4217','Q4218','Q4219','Q4220','Q4221','Q4222','Q4224','Q4225','Q4226','Q4227'
            ,'Q4229','Q4230','Q4231','Q4232','Q4233','Q4234','Q4235','Q4236','Q4237','Q4238'
            ,'Q4239','Q4240','Q4241','Q4242','Q4245','Q4246','Q4247','Q4248','Q4249','Q4250'
            ,'Q4251','Q4252','Q4253','Q4254','Q4255','Q4256','Q4257','Q4258','Q4259','Q4260'
            ,'Q4261','Q4262','Q4263','Q4264','Q4265','Q4266','Q4267','Q4268','Q4269','Q4270'
            ,'Q4271','Q4272','Q4273','Q4274','Q4275','Q4276','Q4277','Q4278','Q4279','Q4280'
            ,'Q4281','Q4282','Q4283','Q4284','Q4287','Q4288','Q4289','Q4290','Q4291','Q4292'
            ,'Q4293','Q4294','Q4295','Q4296','Q4297','Q4298','Q4299','Q4300','Q4301','Q4302'
            ,'Q4303','Q4304','Q4305','Q4306','Q4307','Q4308','Q4309','Q4310','Q4311','Q4312'
            ,'Q4313','Q4314','Q4315','Q4316','Q4317','Q4318','Q4319','Q4320','Q4321','Q4322'
            ,'Q4323','Q4324','Q4325','Q4326','Q4327','Q4328','Q4329','Q4330','Q4331','Q4332'
            ,'Q4333','Q4334','Q4335','Q4336','Q4337','Q4338','Q4339','Q4340','Q4341','Q4342'
            ,'Q4343','Q4344','Q4345','Q4346','Q4347','Q4348','Q4349','Q4350','Q4351','Q4352'
            ,'Q4353','Q4354','Q4355','Q4356','Q4357','Q4358','Q4359','Q4360','Q4361','Q4362'
            ,'Q4363','Q4364','Q4365','Q4366','Q4367'
        ) then 'Unproven'
      end as subcategory

    -- derived: population
    , case
        when migration_source = 'OAH'
            and not (brand_fnl = 'C&S' and fst_srvc_year = '2024' and fin_state = 'MD')
            and not (brand_fnl != 'C&S' and fst_srvc_year = '2024' and market_fnl = 'MD')
        then 'OAH'
        when product_level_3_fnl = 'INSTITUTIONAL'
        then 'INSTITUTIONAL'
        when brand_fnl = 'M&R'
            and global_cap = 'NA'
            and product_level_3_fnl not in ('DUAL', 'INSTITUTIONAL')
            and tfm_include_flag = 1
        then 'M&R FFS'
        when global_cap = 'NA'
            and (
                (brand_fnl = 'C&S' and migration_source != 'OAH' and product_level_3_fnl = 'DUAL')
                or (brand_fnl = 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and fin_state = 'MD')
                or (brand_fnl != 'C&S' and fst_srvc_year = '2024' and migration_source = 'OAH' and market_fnl = 'MD')
            )
        then 'C&S DSNP'
        else 'Other'
      end as population
from ving_prd_trend_db.hce_ops_stage.hceops_pa_trckng_pr_clm_rlup_add
where fst_srvc_dt >= '2024-01-01'
;

alter table tmp_1m.kn_prmm_semantic_base
cluster by 
    (population
    , denial_flag
    , srvc_month
    -- , service_code
);


select * from tmp_1m.kn_prmm_semantic_base
where revised_fnl_rsn_cd = '026'
limit 10;
