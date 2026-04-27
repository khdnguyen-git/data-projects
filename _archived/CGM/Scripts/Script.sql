

select 
	fst_srvc_month
	, product_level_3_fnl
	, count(*) as n
	, count(distinct gal_mbi_hicn_fnl) as mm
from fichsrv.cosmos_op
where proc_cd in ('K0553','K0554','A9276','A9277','A9278', 'E2101','E2102','E2103','A4239','A4238')


select * from fichsrv.cosmos_op

select * from fichsrv.,cosmos_op
where fst_srvc_month >' 202401'
and proc_cd in ('K0553','K0554','A9276','A9277','A9278', 'E2101','E2102','E2103','A4239','A4238')