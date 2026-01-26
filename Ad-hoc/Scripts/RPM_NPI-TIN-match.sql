-- NPI = 1013230648
-- case when proc_cd in ('99453','99454','99457','99458','99091','g0322')  then 'RPM' else 'Others' end as proc_condition



select * from fichsrv.tadm_glxy_provider_npi_detail 
where npi_nbr = '1013230648'
-- prov_sys_id = '1003984126'

select * from fichsrv.tadm_glxy_provider
where prov_sys_id = '1003984126'
-- TIN = '260213733'
-- prov_sys_id = '1003984126'
-- mpin = '6525237'
-- full_nm = 'CADENCE HEALTHCARE LLC'


select * 





select * from hce_ops_fnl.hce_adr_avtar_like_24_25_f 


1003984126

select * from hce_ops_fnl.hce_adr_avtar_like_24_25_f 
where prov_tin = '1003984126'



select * from fichsrv.cosmos_op
where prov_tin like '%260213733%';

select * from fichsrv.cosmos_pr;
where prov_tin like '%260213733%';

select prov_tin from fichsrv.cosmos_pr

select distinct SRVC_PROV_SYS_ID, srvc_prov_npi_fnl, prov_tin, full_nm from fichsrv.cosmos_pr 
where full_nm like '%CADENCE%'


select distinct SRVC_PROV_SYS_ID, srvc_prov_npi_fnl, prov_tin, full_nm from fichsrv.cosmos_op
where full_nm like '%CADENCE%'




select * from fichsrv.cosmos_op
where cast(mpin as varchar) like '%6525237%';

select * from fichsrv.cosmos_pr
where cast(mpin as varchar) like '%6525237%';



select mpin from fichsrv.cosmos_op


select * from fichsrv.cosmos_pr
where mpin like '%6525237%'




where prov_tin like '260213733'
;

select prov_tin from fichsrv.cosmos_op


select * from fichsrv.cosmos_pr
where prov_tin ilike '260213733%'


select * from fichsrv.cosmos_pr
where SRVC_PROV_SYS_ID like '%1003984126%'


select * from fichsrv.cosmos_pr

select * from fichsrv.cosmos_pr
where srvc_prov_npi_fnl ilike '1013230648' or SRVC_PROV_NPI_NBR ilike '%1013230648%'

select * from fichsrv.cosmos_op
where srvc_prov_npi_fnl ilike '1013230648' or SRVC_PROV_NPI_NBR ilike '1013230648'



select * from fichsrv.cosmos_op

select * from hce_ops_fnl.HCE_ADR_AVTAR_LIKE_24_25_F 


1003984126

