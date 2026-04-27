--##############  Data for Synapse: Transition  ##############
--Expansion markets
--all data extract using HPBP/GroupID list (no consideration for fin_market or fin_state)

--for existing HPBPs
select * from tmp_1y.cl_synapse_expansion_hpbp_group;  --102 
select * from tmp_1y.cl_synapse_hcpcs_20250212;  --676 (removed the 2 codes for cgm supplies K0553,  K0554) 


/*
--Data Extraction for Transition
1	extract distinct MBIs for HPBP/GroupID in 2025 YTD
2	join 1 to cosmos_op to extract DME claims for 202409 onward
3	left join 2 to membership_detail to bring in additional member detail
4	membership detail: detail assingment to each unique mbi in 1 is given priority to the most current working backward; ie,  info from 202503 first,  if not found then use info from 202502,  etc...
*/


--membership: 2025 eligibility
drop table tmp_1m.kn_synapse_expansion_mbi_month;
create table tmp_1m.kn_synapse_expansion_mbi_month as 
select distinct
	case when b.snp = 'Y' then b.stateabbreviation ||'-CSNP' else b.stateabbreviation || b.producttype end as market
	, b.hpbp
	, substr(a.gal_cust_seg_nbr, 5, 5) as group_id	
	, 'Medicare' as line_of_business
	, a.fin_inc_month as month
	, a.fin_mbi_hicn_fnl as unique_member_id
	, a.gal_sbscr_nbr as subscriber_id
	, a.fin_date_of_birth as member_dob
	, a.fin_gender
	, a.gps_first_name
	, a.gps_last_name
	, a.gps_middle_name
	, a.fin_state as member_state
	, a.fin_county_name as member_county
from fichsrv.tre_membership a
join tmp_1y.cl_synapse_expansion_hpbp_group b       
	on    a.fin_contractpbp = b.hpbp
	  and substr(a.gal_cust_seg_nbr, 5, 5) = lpad(b.group_number,  5,  '0')
where 
	a.fin_source_name = 'COSMOS'
	--and migration_source <> 'OAH'
    and a.global_cap='NA'
    and a.fin_inc_year = '2025'
;    
--2396983  1418971   select count(*) from tmp_1m.kn_synapse_expansion_mbi_month

--select month,  count(*) as cnt from tmp_1m.kn_synapse_expansion_mbi_month group by month
month	cnt
202501	472, 267
202502	479, 400
202503	481, 498
202504	485, 594
202505	478, 224

--getting distinct mbi for 2025 YTD
drop table  tmp_1m.kn_synapse_expansion_mbi; 
create table tmp_1m.kn_synapse_expansion_mbi as
select distinct  
	unique_member_id
from tmp_1m.kn_synapse_expansion_mbi_month
;
--527360 496968		select count(distinct unique_member_id)  from tmp_1m.kn_synapse_expansion_mbi


drop table tmp_1m.kn_synapse_expansion_mbi_maxmonth1;
create table tmp_1m.kn_synapse_expansion_mbi_maxmonth1 as
select 
	unique_member_id
	, max(month) as maxmonth
from tmp_1m.kn_synapse_expansion_mbi_month
group by unique_member_id
--527360		select count(*) from tmp_1m.kn_synapse_expansion_mbi_maxmonth

drop table  tmp_1m.kn_synapse_expansion_mbi_maxmonth2; 
create table tmp_1m.kn_synapse_expansion_mbi_maxmonth2 as
select distinct  
	unique_member_id
	, max(month) over (partition by unique_member_id) as maxmonth
from tmp_1m.kn_synapse_expansion_mbi_month;




drop table tmp_1m.kn_synapse_expansion_mbi_detail_unique;
create table tmp_1m.kn_synapse_expansion_mbi_detail_unique as
select DISTINCT 
	a.unique_member_id
	, b.market
	, b.hpbp
	, b.group_id	
	, b.line_of_business
	, b.month
	, b.unique_member_id as mbi
	, b.subscriber_id
	, b.member_dob
	, b.gps_first_name
	, b.gps_last_name
	, b.gps_middle_name
	, b.member_state
	, b.member_county
from tmp_1m.kn_synapse_expansion_mbi_maxmonth a
join tmp_1m.kn_synapse_expansion_mbi_month b
on a.unique_member_id = b.unique_member_id 
and a.maxmonth = b.month
--527360		select count(*) from tmp_1m.kn_synapse_expansion_mbi_detail_unique

select * from tmp_1m.kn_synapse_expansion_mbi_detail_unique limit 2;
 
    
        
--extract claims
drop table tmp_1m.kn_synapse_transition_data_expansion_mbi_claims;
create table tmp_1m.kn_synapse_transition_data_expansion_mbi_claims as
select 
	c.market as market_expansion
	, c.line_of_business
	, c.hpbp	
	, c.group_id
	, c.unique_member_id
	, c.subscriber_id as subscriber_number
	, c.member_dob
	, c.gps_first_name as member_first_name
	, c.gps_last_name as member_last_name
	, c.gps_middle_name as member_middle_name
	, c.fin_gender as member_gender
	, c.member_state
	, c.member_county
	, a.fst_srvc_dt as date_of_service
    , a.fst_srvc_month as service_month
    , a.fst_srvc_year as service_year
	, a.CLM_PL_OF_SRVC_DESC as place_of_service_code
	, a.prov_tin as vendor_tin
	, a.full_nm as vendor_name
	, a.prov_prtcp_sts_cd as vendor_par_status
    , a.proc_cd as hcpcs
    , a.proc_mod1_cd
	, a.proc_mod2_cd
	, a.proc_mod3_cd
	, a.proc_mod4_cd
    , sum(a.tadm_hcta_util) as units
    , sum(a.net_pd_amt_fnl) as paid_amt
from fichsrv.cosmos_op a
join tmp_1y.cl_synapse_hcpcs_20250212 b    --676 (removed the 2 codes for cgm supplies K0553,  K0554) 
	on a.proc_cd = b.hcpcs
join tmp_1m.kn_synapse_expansion_mbi_detail_unique c             --select * from tmp_1m.kn_synapse_expansion_mbi_detail_unique
	on a.gal_mbi_hicn_fnl = c.unique_member_id 
where
	a.global_cap = 'NA'
	--and a.migration_source <> 'OAH'
	and a.GROUP_IND_FNL = 'I'
    and a.hce_service_code = 'OP_DMESUP'
    and a.fst_srvc_month >= '202409'
    and (a.allw_amt_fnl <> 0 or a.tadm_units <> 0)
group by
	c.market
	, c.line_of_business
	, c.hpbp	
	, c.group_id
	, c.unique_member_id
	, c.subscriber_id 
	, c.member_dob
	, c.gps_first_name
	, c.gps_last_name 
	, c.gps_middle_name
	, c.fin_gender 
	, c.member_state
	, c.member_county
	, a.fst_srvc_dt 
    , a.fst_srvc_month
    , a.fst_srvc_year 
	, a.CLM_PL_OF_SRVC_DESC
	, a.prov_tin 
	, a.full_nm 
	, a.prov_prtcp_sts_cd 
    , a.proc_cd 
    , a.proc_mod1_cd
	, a.proc_mod2_cd
	, a.proc_mod3_cd
	, a.proc_mod4_cd
;
--667537  454645   select count(*) from tmp_1m.kn_synapse_transition_data_expansion_mbi_claims 
select * from tmp_1m.kn_synapse_transition_data_expansion_mbi_claims 

create table tmp_1m.kn_synapse_transition_data_expansion_mbi_claims_202508exp_thru202505 as
select * from tmp_1m.kn_synapse_transition_data_expansion_mbi_claims
--667537	select count(*) from tmp_1m.kn_synapse_transition_data_expansion_mbi_claims_202508exp_thru202505  (file created on 6/5/2025)


create table tmp_1m.kn_synapse_transition_data_expansion_mbi_claims_202508exp_thru202503 as
select * from tmp_1m.kn_synapse_transition_data_expansion_mbi_claims
--454645	select count(*) from tmp_1m.kn_synapse_transition_data_expansion_mbi_claims_202508exp_thru202503  (file created on 4/16/2025)
 
--check record count for reasonableness: mbr counts are members with claims
select 
	service_month
	, count(distinct unique_member_id) as mbrs
	, count(*) as recordcnt
	, sum(paid_amt) as paid
from tmp_1m.kn_synapse_transition_data_expansion_mbi_claims_202508exp_thru202503
group by 
	service_month

service_month	mbrs	recordcnt	paid
202409			27, 808	66, 556		3, 341, 485.24
202410			29, 093	70, 823		3, 590, 763.2
202411			28, 868	68, 802		3, 480, 477.09
202412			29, 470	71, 583		3, 554, 462.44
202501			32, 792	78, 781		3, 706, 978.51
202502			32, 094	75, 680		3, 509, 049.4
202503			11, 665	22, 420		1, 007, 713.19
	
select 
	service_month
	, count(distinct unique_member_id) as mbrs
	, count(*) as recordcnt
	, sum(paid_amt) as paid
from tmp_1m.kn_synapse_transition_data_expansion_mbi_claims_202508exp_thru202505
group by 
	service_month	
	
service_month	mbrs	recordcnt	paid
202409			28, 302	67, 760		3, 399, 878.29
202410			29, 623	72, 290		3, 649, 586.11
202411			29, 396	70, 195		3, 562, 949.18
202412			30, 179	73, 545		3, 677, 455.34
202501			35, 573	85, 609		4, 076, 877.41
202502			36, 661	87, 787		4, 229, 513.8
202503			37, 558	91, 409		4, 352, 567.18
202504			36, 347	86, 739		4, 058, 223.48
202505			16, 123	32, 203		1, 443, 601.61	
	

	
--check against the cap rate development file
select 	market_expansion,  service_month,  sum(paid_amt) as paid from tmp_1m.kn_synapse_transition_data_expansion_mbi_claims  group by market_expansion,  service_month;
	
select * from tmp_1m.kn_synapse_transition_data_expansion_mbi_claims limit 1;
	
	
	
