drop table tmp_1m.kn_mbm_comparison
create table tmp_1m.kn_mbm_comparison as
with 
	sum_excl as (
	select
		serv_month
		, sum(allowed) as excl_allowed
	from tmp_1y.kn_mbm_episode_2
	where (prov_tin = '820200895' or tin_num = 820200895) and serv_month >= '202401'
	group by
		serv_month
	)
,	sum_all as (
	select
		serv_month
		, sum(allowed) as all_allowed
	from tmp_1y.kn_mbm_episode_2
	where serv_month >= '202401'
	group by
		serv_month
	)
select
	coalesce(a.serv_month, b.serv_month) as month
	, a.excl_allowed as allowed_system
	, b.all_allowed as allowed_all
	, concat(round((a.excl_allowed / b.all_allowed) * 100, 2), '%') as allowed_percent
from sum_excl as a
join sum_all as b
on a.serv_month = b.serv_month
order by
	month
;

select * from tmp_1m.kn_mbm_comparison;

select * from tmp_1y.cl_mbm_episode_1

serv_


select 
	a.*
	, b.gal_prov_npi 
from 
where gal_prov_npi = '1457414948'

select
	prov_tin
	, srvc_prov_npi_nbr
from fichsrv.cosmos_pr
where (srvc_prov_npi_nbr = '1457414948' or prov_tin = '820200895') and fst_srvc_month >= '202401'
group by 
	prov_tin
	, srvc_prov_npi_nbr
;


describe fichsrv.cosmos_pr;

fst_srv