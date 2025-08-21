-- Finding duplicate fin_brand per mbi
select  
	fin_mbi_hicn_fnl
	, fin_brand
from fichsrv.tre_membership
group by fin_mbi_hicn_fnl, fin_brand
having count(distinct fin_brand) = 2


select 
	fin_brand 
from tmp_1m.cl_cohort_MMs_25_24
group by fin_brand