select 
prim_diag_ahrq_genl_catgy_desc
, prim_diag_ahrq_diag_dtl_catgy_desc
, count(*) 
from hce_ops_fnl.hce_adr_avtar_like_25_26_f 
group by 1, 2 order by 3 desc limit 500

;

select distinct
    prim_diag_ahrq_genl_catgy_desc
, prim_diag_ahrq_diag_dtl_catgy_desc
from hce_ops_fnl.hce_adr_avtar_like_25_26_f 
limit 500;


select * from tmp_1m.ec_avtar_25_26_3_od where case_id = '290911806';