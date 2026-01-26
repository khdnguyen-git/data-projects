--Therapy - LOPA count tracking: update monthly
--describe formatted TMP_1y.kn_MBM_EPISODE_RO_LAG2
describe formatted tmp_1y.PA_TRCKNG_op_EVNT_LOPA_DTL;
describe formatted tmp_1q.kn_mbm_episode_2_202511;






--create a table with uqinue record for mbi/dos/category, then bring in paid claims status with allowed amt, then lopa status

select * from tmp_1q.kn_mbm_episode_2_202511 limit 2;

select * from fichsrv.tre_membership limit 10;

--unique claims (define by mbi + dos + proc_cd) ; temporarily remove component to avoid duplicate records; bring back at the end together with product type.
drop table if exists tmp_7d.kn_mbm_lopa_tracking_1;
create table tmp_7d.kn_mbm_lopa_tracking_1 as
select distinct
mbi,
-- component, --30k records duplicate
-- b.fin_tfm_product_new, -- 490 records dupulicate 
start_dt,
serv_month,
proc_cd,
case when mbm_deploy_dt = 'National' then 'National' else 'Pilot' end as pilot_national
from tmp_1q.kn_mbm_episode_2_202511
where PROV_PRTCP_STS_CD='P' AND
serv_month > '202109'
;
--97790496 74609604 select count(*) from tmp_7d.kn_mbm_lopa_tracking_1

--bring in category: priority as: OP_Rehab > Office > Chiro > Other (a proc cd could map to multiple category based on place of services)
drop table if exists tmp_7d.kn_mbm_lopa_tracking_1_Rehab;
create table tmp_7d.kn_mbm_lopa_tracking_1_Rehab as
select distinct
mbi,
start_dt,
proc_cd
from tmp_1q.kn_mbm_episode_2_202511
where PROV_PRTCP_STS_CD='P' AND
serv_month > '202109' and
category = 'OP_REHAB'
;
-- 32171888 23965099 select count(*) from tmp_7d.kn_mbm_lopa_tracking_1_Rehab

drop table if exists tmp_7d.kn_mbm_lopa_tracking_1_Office;
create table tmp_7d.kn_mbm_lopa_tracking_1_Office as
select distinct
mbi,
start_dt,
proc_cd
from tmp_1q.kn_mbm_episode_2_202511
where PROV_PRTCP_STS_CD='P' AND
serv_month > '202109' and
category = 'Office'
;
--55705122 42958579 41873188 select count(*) from tmp_7d.kn_mbm_lopa_tracking_1_Office

drop table if exists tmp_7d.kn_mbm_lopa_tracking_1_Chiro ;
create table tmp_7d.kn_mbm_lopa_tracking_1_Chiro as
select distinct
mbi,
start_dt,
proc_cd
from tmp_1q.kn_mbm_episode_2_202511
where PROV_PRTCP_STS_CD='P' AND
serv_month > '202109' and
category = 'Chiro'
;
--7363367 5852250 select count(*) from tmp_7d.kn_mbm_lopa_tracking_1_Chiro


drop table if exists tmp_7d.kn_mbm_lopa_tracking_2_category;
create table tmp_7d.kn_mbm_lopa_tracking_2_category as
select
a.*,
case when b.mbi is not null then 'OP_Rehab'
when c.mbi is not null then 'Office'
when d.mbi is not null then 'Chiro'
else 'Other' end as Category
from tmp_7d.kn_mbm_lopa_tracking_1 a
left join tmp_7d.kn_mbm_lopa_tracking_1_Rehab b
on a.mbi = b.mbi AND
a.start_dt = b.start_dt AND
a.proc_cd = b.proc_cd
left join tmp_7d.kn_mbm_lopa_tracking_1_Office c
on a.mbi = c.mbi AND
a.start_dt = c.start_dt AND
a.proc_cd = c.proc_cd
left join tmp_7d.kn_mbm_lopa_tracking_1_Chiro d
on a.mbi = d.mbi AND
a.start_dt = d.start_dt AND
a.proc_cd = d.proc_cd
;
--74609604 select count(*) from tmp_7d.kn_mbm_lopa_tracking_2_category
-- 97790496
-- 97790496



/*
select Category, count(*) as cnt from tmp_7d.kn_mbm_lopa_tracking_2_category group by category --check agains individual category table and counts seem reasonable.

category cnt
OP_Rehab 23,965,099
Chiro 5,847,137
Other 1,849,037
Office 42,948,331

CATEGORY	CNT
Office	55,693,112
OP_Rehab	32,171,888
Chiro	7,356,699
Other	2,568,797
*/

--bring in Paid/Denied status and allowed amount for Paid
select * from tmp_1q.kn_mbm_episode_2_202511 limit 5;

create table tmp_7d.kn_mbm_lopa_tracking_2_paid as
select
mbi,
start_dt,
proc_cd,
sum(allowed) as allowed_amt
from tmp_1q.kn_mbm_episode_2_202511
where PROV_PRTCP_STS_CD='P' AND
serv_month > '202109' and
claim_status in ('Paid', 'APC-Paid')
group by
mbi,
start_dt,
proc_cd
;
--90032607 70720781 51342718 select count(*) from tmp_7d.kn_mbm_lopa_tracking_2_paid
--select * from tmp_7d.kn_mbm_lopa_tracking_2_paid where allowed_amt <2 Note: keep Paid status even if the paid amount is $0

create table tmp_7d.kn_mbm_lopa_tracking_3_cat_paid as
select
a.*,
case when b.mbi is not null then 'Paid' else 'Denied' end as Claim_Status,
b.allowed_amt
from tmp_7d.kn_mbm_lopa_tracking_2_category a
left join tmp_7d.kn_mbm_lopa_tracking_2_paid b
on a.mbi = b.mbi AND
a.start_dt = b.start_dt AND
a.proc_cd = b.proc_cd
;
/*
select Claim_status, count(*) as cnt from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid group by Claim_status
claim_status cnt
Denied 3,888,823
Paid 70,720,781

Paid	90032607
Denied	7757889
*/

--bring in lopa status for Denied claims
select * from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid limit 10;


create table tmp_7d.kn_mbm_ever_lopa as
select DISTINCT
mbi,
start_dt,
proc_cd,
lopa_flg,
still_lopa,
overturn_lopa
from tmp_1q.kn_mbm_episode_2_202511
where lopa_flg = 1
;
--3910455 1896320 1660318 select count(distinct mbi||start_dt||proc_cd) from tmp_7d.kn_mbm_ever_lopa

--overturn is higher priority than still lopa
create table tmp_7d.kn_mbm_ever_lopa_overturn as
select DISTINCT
mbi,
start_dt,
proc_cd,
lopa_flg,
still_lopa,
overturn_lopa
from tmp_1q.kn_mbm_episode_2_202511
where overturn_lopa = 1
;
--1177257 414235 325411 select count(*) from tmp_7d.kn_mbm_ever_lopa_overturn


create table tmp_7d.kn_mbm_ever_lopa_still as
select DISTINCT
mbi,
start_dt,
proc_cd,
lopa_flg,
still_lopa,
overturn_lopa
from tmp_1q.kn_mbm_episode_2_202511
where still_lopa = 1
;
--2977191 1564966 1334907 select count(*) from tmp_7d.kn_mbm_ever_lopa_still


drop table if exists tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_1;
create table tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_1 as
SELECT
a.*,
case when b.overturn_lopa = 1 then 'Y' else 'N' end as Overturn_Lopa_f --remove condition on a.claim_status = 'Denied' 
from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid a
left join tmp_7d.kn_mbm_ever_lopa_overturn b
on a.mbi = b.mbi AND
a.start_dt = b.start_dt AND
a.proc_cd = b.proc_cd
;
--97790496 74609604 select count(*) from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_1
drop table if exists tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_2;
create table tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_2 as
SELECT
a.*,
case when a.claim_status = 'Denied' and a.overturn_lopa_f = 'N' and c.still_lopa = 1 then 'Y' else 'N' end as Still_Lopa_f
from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_1 a
left join tmp_7d.kn_mbm_ever_lopa_still c
on a.mbi = c.mbi AND
a.start_dt = c.start_dt AND
a.proc_cd = c.proc_cd
;
--97790496 74609604 select count(*) from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_2
drop table if exists tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa;
create table tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa as
SELECT
*,
case when Overturn_Lopa_f = 'Y' or Still_Lopa_f = 'Y' then 'Y' else 'N' end as Ever_Lopa,
1 as recordcnt
from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_2
;
--97790496 74609604 select count(*) from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa
--check
select * from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa where mbi = '1T20WT1FY29' and start_dt = '2023-05-10' and proc_cd = '97110'
select * from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa limit 2;

select count(*) from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa; --74609604

--adding back product and component: if Office and Chiro then give priority to PR, OP_Rehab to OP
drop table if exists tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_4;
create table tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_4 as
select
a.*,
b.fin_tfm_product_new
from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa a
left join fichsrv.tre_membership b
on a.mbi = b.fin_mbi_hicn_fnl and
a.serv_month = b.fin_inc_month
;
select count(*) from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_4; --74609604
select * from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_4 limit 10;

/*
--1/21/2025: bring in market for Tyler/Dan request
drop table if exists tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_4_market
create table tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_4_market as
select 
a.*,
b.fin_market
from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_4 a
left join fichsrv.tre_membership b
on a.mbi = b.fin_mbi_hicn_fnl and
a.serv_month = b.fin_inc_month

select count(*) from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_4_market --74609604

drop table if exists tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_summary_market
create table tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_summary_market as
SELECT 
fin_tfm_product_new, 
fin_market,
year(start_dt) as serv_year,
serv_month,
pilot_national,
category,
claim_status,
ever_lopa,
still_lopa_f as still_lopa,
overturn_lopa_f as overturn_lopa,
sum(recordcnt) as visit_proc_cnt
from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_4_market
where category <> 'Other' and year(start_dt) > '2023'
group by 
fin_tfm_product_new, 
fin_market,
year(start_dt),
serv_month,
pilot_national,
category,
claim_status,
ever_lopa,
still_lopa_f,
overturn_lopa_f

select count(*) from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_summary_market --15304
select * from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_summary_market


*/


--summary for excel
drop table if exists tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_summary;
create table tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_summary as
SELECT
fin_tfm_product_new,
year(start_dt) as serv_year,
serv_month,
pilot_national,
category,
claim_status,
ever_lopa,
still_lopa_f as still_lopa,
overturn_lopa_f as overturn_lopa,
sum(recordcnt) as visit_proc_cnt
from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_4
group by
fin_tfm_product_new,
year(start_dt),
serv_month,
pilot_national,
category,
claim_status,
ever_lopa,
still_lopa_f,
overturn_lopa_f
;

select count(*) from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_summary; --4437
select * from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_summary; --4437
select count(*) from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_summary; --3922
select * from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa_summary;

--checking against Sree's excel
SELECT
category,
sum(recordcnt) as visit_proc_cnt
from tmp_7d.kn_mbm_lopa_tracking_3_cat_paid_lopa
where proc_cd = '97110' and still_lopa_f = 'Y' and serv_month between '202401' and '202412'
group by
category;
