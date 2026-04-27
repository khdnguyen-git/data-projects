-- COSMOS Membership
create table tmp_1m.kn_{table} as
select 
	  fin_region1 
	, fin
	, 
	,
from fichsrv.tre_membership
where 1 = 1	
	and fin_brand = "M&R"
	and sgr_source_name = "COSMOS"
	and fin_product_level_3 != "INSTITUTIONAL"
	and tfm_include_flag = 1
	and global_cap = "NA"

