-- Project: Waterjet ablation for BPH dx
-- Author: Khand Nguyen

-- Summary
-- PR has approval checks -> Confirm it's working
-- OP doesn't -> Explore savings
-- CPT: 'C2596', '0412T'
-- Dx: 'N401'
-- Population: M&R FFS, C&S DSNP, and OAH
-- Output: Allowed, total claims (count distinct MBI&DOS*PROC)

-- PR claims
create table tmp_1m.kn_waterjet_pr as
select
    concat_ws('-', gal_mbi_hicn_fnl, srvc_prov_id, fst_srvc_dt, proc_cd) as unique_id
    , brand_fnl
    , group_ind_fnl
    , tfm_product_new_fnl
    , product_level_3_fnl
    
    , 
    , gal_mbi_hicn_fnl
    , fst_srvc_month
    , fst_srvc_year
    , proc_cd 
    , srvc_prov_id 
    , allowed_amt 
    , primary_diag_cd 
	, concat_ws('-', icd_2, icd_3, icd_4, icd_5, icd_6, icd_7, icd_8, icd_9, icd_10, icd_11, icd_12, icd_13, icd_14, icd_15, 
		icd_16, icd_17, icd_18, icd_19, icd_20, icd_21, icd_22, icd_23, icd_24, icd_25) as other_diag_cd
    , case
        when 'N401' 
            in (primary_diag_cd, icd_2, icd_3, icd_4, icd_5, icd_6, icd_7, icd_8, icd_9, icd_10 
                , icd_11, icd_12, icd_13, icd_14, icd_15, icd_16, icd_17, icd_18, icd_19, icd_20 
                , icd_21, icd_22, icd_23, icd_24, icd_25) then 'Y'
        else 'N'
    end as bph
from fichsrv.cosmos_pr
where proc_cd in ('C2596', '0412T')
    and fst_srvc_month >= '202401' 
	and sgr_source_name = 'COSMOS'
	and fin_brand = 'M&R'
	and migration_source not in ('OAH', 'CSP')
	and fin_product_level_3 not in ('INSTITUTIONAL')
	and global_cap = 'NA' 
;


where 1=1

;