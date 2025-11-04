drop table if exists tmp_7d.kn_mbm_network_pr_op_claims_1;
create table tmp_7d.kn_mbm_network_pr_op_claims_1 as
select * from tmp_1y.cl_network_pr_op_claims_1;

select * from tmp_1y.cl_network_pr_op_claims_1



-- Calculate ep and visits
create or replace table tmp_7d.kn_mbm_network_pr_op_claims_3 as
with cte_lag_date as (
select 
	*
	, lag(fst_srvc_dt) over (partition by component, mbi, prov_tin, category order by fst_srvc_dt) as previous_fst_srvc_dt
from tmp_1y.cl_network_pr_op_claims_1
),
cte_ep_flag as (
select 
	*
	, coalesce(datediff(day, previous_fst_srvc_dt, fst_srvc_dt),0) as visit_lapse_days
	, case
		when previous_fst_srvc_dt is null then 1
		when datediff(day, previous_fst_srvc_dt, fst_srvc_dt) > 30 then 1
	else 0
	end as ep_flag
from cte_lag_date
),
cte_ep_num as (
select 
	*
	, sum(ep_flag) over (partition by component, mbi, category, prov_tin order by fst_srvc_dt rows between unbounded preceding and current row) as ep_num
from cte_ep_flag
),
cte_ep_start_date as (
select 
	*
	, min(fst_srvc_dt) over (partition by component, mbi, prov_tin, category, ep_num) as ep_start_date
from cte_ep_num
where ep_flag = 1
),
cte_final as (
select 
	a.* 
	, b.ep_start_date
	, to_char(b.ep_start_date, 'YYYYMM') as ep_start_month
	, concat_ws('-', a.mbi, a.component, a.prov_tin, b.ep_start_date) as ep_id
	, datediff(day, b.ep_start_date, a.fst_srvc_dt) as visit_ep_days
	, case
		when datediff(day, b.ep_start_date, a.fst_srvc_dt) = 0 then 1
		else ceil(datediff(day, b.ep_start_date, a.fst_srvc_dt) / 30.5)
	end as visit_ep_months
	, concat_ws('-', a.mbi, a.component, a.prov_tin, b.fst_srvc_dt) as visit_id
	, a.fst_srvc_month as visit_month
from cte_ep_num as a
left join cte_ep_start_date as b
	on a.component = b.component
	and a.mbi = b.mbi
	and a.prov_tin = b.prov_tin
	and a.ep_num = b.ep_num 
)
select * from cte_final;


select
	*
from tmp_7d.kn_mbm_network_pr_op_claims_3
where mbi = '9N83TH7HW04'
;

select
	*
from tmp_1y.cl_network_pr_op_claims_3
where mbi = '9N83TH7HW04'
;




	
--add row order numbner to count the visits for each cluster: mbi/component/catergory/prov_tin
DROP TABLE IF EXISTS tmp_7d.kn2_network_pr_op_claims_2; 
CREATE TABLE  tmp_7d.kn2_network_pr_op_claims_2 as
SELECT 
	*
	,row_number() over (PARTITION BY  component, mbi, prov_tin, category  ORDER BY fst_srvc_dt ASC ) as rownum  
FROM tmp_1y.cl_network_pr_op_claims_1
;

--SELECT * FROM tmp_7d.kn2_network_pr_op_claims_2  WHERE mbi = '8M04CA8DN99' AND prov_tin = '370808925'

ALTER TABLE tmp_7d.kn2_network_pr_op_claims_2 
ADD previous_fst_srvc_dt date
	,visit_lapse_days int
	,ep_flag int
;

UPDATE tmp_7d.kn2_network_pr_op_claims_2 
SET previous_fst_srvc_dt = fst_srvc_dt
WHERE rownum = 1 
;

UPDATE tmp_7d.kn2_network_pr_op_claims_2 a
SET a.previous_fst_srvc_dt = b.fst_srvc_dt
FROM tmp_7d.kn2_network_pr_op_claims_2 b
WHERE a.rownum > 1 
	AND a.component = b.component 
	AND a.mbi = b.mbi 
	AND a.prov_tin = b.prov_tin 
	AND a.category = b.category 
	AND a.rownum - 1 = b.rownum
;
	
--SELECT * FROM tmp_7d.kn2_network_pr_op_claims_2  WHERE mbi = '8M04CA8DN99' AND prov_tin = '370808925'
--SELECT * FROM tmp_7d.kn2_network_pr_op_claims_3  WHERE mbi = '9N83TH7HW04' AND prov_tin = '200832622'
	

UPDATE tmp_7d.kn2_network_pr_op_claims_2
SET visit_lapse_days = datediff(DAY,previous_fst_srvc_dt, fst_srvc_dt )
;


UPDATE tmp_7d.kn2_network_pr_op_claims_2
SET ep_flag = CASE WHEN rownum = 1 THEN 1
                   WHEN visit_lapse_days > 30 THEN 1
                   ELSE 0 end
;
                   

--an episode is unique to mbi/component/category/provider                   
DROP TABLE tmp_7d.kn2_network_pr_op_claims_3 ;
create TABLE tmp_7d.kn2_network_pr_op_claims_3 AS
SELECT 
	*
	,sum(ep_flag) over (partition by component, mbi, category, prov_tin order by FST_SRVC_DT rows between unbounded preceding and current row) AS ep_num
FROM tmp_7d.kn2_network_pr_op_claims_2
;
--SELECT * FROM tmp_7d.kn2_network_pr_op_claims_3  WHERE mbi = '9N83TH7HW04' AND prov_tin = '200832622'

--episode start date
DROP TABLE tmp_7d.cl_ep_start_month ;
CREATE TABLE tmp_7d.cl_ep_start_month as
SELECT *
FROM tmp_7d.kn2_network_pr_op_claims_3  
WHERE ep_flag = 1
;

ALTER TABLE tmp_7d.kn2_network_pr_op_claims_3
ADD ep_start_date date
	,ep_start_month varchar(6)
	,ep_id varchar(100)
	,encounter_id varchar(100)
	,visit_month varchar(6)
	,visit_ep_days int
	,visit_ep_months int
	,optum_tin_yn varchar(1)
	,optum_tin_specialty varchar(10)	
;	

UPDATE tmp_7d.kn2_network_pr_op_claims_3 a
SET a.ep_start_date = b.fst_srvc_dt
FROM tmp_7d.cl_ep_start_month b
WHERE 1=1
	AND a.component = b.component
	AND a.mbi = b.mbi
	AND a.category = b.category
	AND a.prov_tin = b.prov_tin 
	AND a.ep_num = b.ep_num
;	
	
UPDATE tmp_7d.kn2_network_pr_op_claims_3 
SET ep_start_month = YEAR(ep_start_date)||lpad(MONTH(ep_start_date),2,'0')
    ,visit_month = fst_srvc_month
    ,visit_ep_days = datediff(DAY,ep_start_date, fst_srvc_dt)
	,visit_ep_months = CASE WHEN datediff(DAY,ep_start_date, fst_srvc_dt) = 0 THEN 1
	                        else ceil(datediff(DAY,ep_start_date, fst_srvc_dt) / 30.5) END     --CEIL vs Floor  CHECK this code TO compare WITH Khang's
;
	                        
UPDATE tmp_7d.kn2_network_pr_op_claims_3 
SET ep_id = concat(mbi,'-',component,'-',category,'-',prov_tin,'-',EP_START_MONTH)
    ,encounter_id = concat(mbi,'-',component,'-',category,'-',prov_tin,'-',FST_SRVC_DT)
;
                                            
	                        
UPDATE tmp_7d.kn2_network_pr_op_claims_3 A
SET a.optum_tin_yn = 'Y'
	,a.optum_tin_specialty = b.Specialty
FROM tmp_7d.kn2_optum_therapy_tins b
WHERE a.PROV_TIN  = b.TIN 
;

UPDATE tmp_7d.kn2_network_pr_op_claims_3 
SET optum_tin_yn = 'N'
WHERE optum_tin_yn is null
;


ALTER TABLE tmp_7d.kn2_network_pr_op_claims_3                      
ADD fin_market varchar(25)
	,primary_diag_cd varchar(25)
	,AHRQ_DIAG_GENL_CATGY_DESC varchar(250)
	,AHRQ_DIAG_DTL_CATGY_DESC  varchar(250)
	,DIAG_DESC varchar(250)
;

--Dx is picked using the episode_start_date; in order to keep Dx unique to episode    
DROP TABLE tmp_7d.cl_network_pr_op_claims_episode_dx;
CREATE TABLE tmp_7d.cl_network_pr_op_claims_episode_dx AS
SELECT DISTINCT 
	component
	,mbi
	,CATEGORY
	,PROV_TIN
	,fst_srvc_dt
FROM tmp_7d.kn2_network_pr_op_claims_3
;


ALTER TABLE tmp_7d.cl_network_pr_op_claims_episode_dx 
ADD Dx varchar(25)
	,fin_market varchar(10)
;

UPDATE tmp_7d.cl_network_pr_op_claims_episode_dx  a
SET a.Dx = b.PRIMARY_diag_cd
	,a.fin_market = b.market_fnl
FROM tmp_7d.kn2_network_pr_op_claims b
WHERE a.component = b.COMPONENT 
	AND a.mbi = b.MBI 
	AND a.CATEGORY = b.CATEGORY 
	AND a.PROV_TIN = b.PROV_TIN 
	AND a.fst_srvc_dt  = b.FST_SRVC_DT 
;


	

UPDATE tmp_7d.kn2_network_pr_op_claims_3 a
SET a.fin_market = b.fin_market
    ,a.PRIMARY_diag_cd = b.Dx
FROM tmp_7d.cl_network_pr_op_claims_episode_dx  b
WHERE 1=1
	AND a.component = b.COMPONENT 
	AND a.mbi = b.MBI 
	AND a.CATEGORY = b.CATEGORY 
	AND a.PROV_TIN = b.PROV_TIN 
	AND a.EP_START_DATE  = b.FST_SRVC_DT 
;


