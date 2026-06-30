-- LOC ec_avtar vs tmp_7d validation
-- Purpose: confirm tmp_7d.hce_adr_avtar_like_25_26_f produces equivalent
-- case counts to tmp_1m.ec_avtar_25_26_3_od for the M&R FFS LOC population.
-- If counts align, tmp_7d can replace ec_avtar (gaining icm_ever_owned field).

with ec_counts as (
    select
        count(distinct a.case_id) as total_cases
        , count(distinct case when a.initialfulladr_cases = 1 then a.case_id end) as initial_adr_cases
    from tmp_1m.ec_avtar_25_26_3_od as a
    where a.fin_brand = 'M&R'
        and (
            (a.ip_type in ('Medical', 'Surgical', 'Transplant')
             and to_varchar(a.admit_dt_act, 'MM/dd/yyyy') is not null)
            or a.ip_type in ('LTAC', 'SNF', 'AIR')
        )
        and a.loc_flag = 1
        and (
            (a.global_cap = 'NA' and a.sgr_source_name = 'COSMOS'
             and a.fin_product_level_3 != 'INSTITUTIONAL' and a.tfm_include_flag = 1)
            or (a.sgr_source_name = 'NICE' and a.nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN'))
        )
        and a.hce_admit_month = '202602'
)

, tmp7d_counts as (
    select
        count(distinct a.case_id) as total_cases
        , count(distinct case when a.initialfulladr_cases = 1 then a.case_id end) as initial_adr_cases
    from tmp_7d.hce_adr_avtar_like_25_26_f as a
    where a.fin_brand = 'M&R'
        and a.svc_setting = 'Inpatient'
        and a.admit_cat_cd in ('17 - Medical', '30 - Surgical', '33 - Transplant')
        and to_varchar(a.admit_dt_act, 'MM/dd/yyyy') is not null
        and (
            (a.global_cap = 'NA' and a.sgr_source_name = 'COSMOS'
             and a.fin_product_level_3 != 'INSTITUTIONAL' and a.tfm_include_flag = 1)
            or (a.sgr_source_name = 'NICE' and a.nce_tadm_dec_risk_type in ('FFS', 'PHYSICIAN'))
        )
        and a.admit_act_month = '202602'
)

select
    'ec_avtar' as source
    , ec.total_cases
    , ec.initial_adr_cases
from ec_counts as ec

union all

select
    'tmp_7d' as source
    , t.total_cases
    , t.initial_adr_cases
from tmp7d_counts as t
;

