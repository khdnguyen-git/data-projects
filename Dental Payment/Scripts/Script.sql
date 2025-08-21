create table tmp_1m.kn_dental_proc_check as 
select
	proc_cd 
	, sum(totaL_allowed) as sum_allowed
	, sum(total_unit_count) as sum_count
from tmp_1m.kn_dental_claims_mbr_v2
group by
	temp_provtin
order by
	sum_allowed desc
;


