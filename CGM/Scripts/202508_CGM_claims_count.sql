-- Claims stuff

desc formatted mard.m1_claims_f;

-- CGMs claim pull 
drop table tmp_1m.kn_cgm_claims_op;
create temporary table tmp_1m.kn_cgm_claims_op as 
select
	site_clm_aud_nbr
	, sub_aud_nbr
	, dtl_ln_nbr
	, clm_rec_cd
	, component
	, hce_service_code
	, hce_qtr
	, hce_month
	, fst_srvc_year
	, fst_srvc_month
	, fst_srvc_qtr
	, fst_srvc_dt
	, erly_srvc_qtr
	, erly_srvc_dt
	, catgy_rol_up_2_desc
	, brand_fnl
	, eventkey
	, eventkey_procrev
	, clm_dnl_f
	, bil_recv_dt
	, adjd_qtr
	, adjd_dt
	, clm_pd_dt
	, fnl_rsn_cd_sys_id
	, clm_lvl_rsn_cd_sys_id
	, srvc_lvl_rsn_cd_sys_id
	, plan_level_1_fnl
	, plan_level_2_fnl
	, product_level_1_fnl
	, product_level_2_fnl
	, product_level_3_fnl
	, tfm_product_new_fnl
	, region_fnl
	, market_fnl
	, fin_submarket
	, global_cap
	, group_ind_fnl
	, tfm_include_flag
	, migration_source
	, groupnumber
	, segment_name_fnl
	, contractpbp_fnl
	, contract_fnl
	, gal_mbi_hicn_fnl
	, bth_dt
	, gdr_cd
	, mpin
	, srvc_prov_id
	, prov_tin
	, full_nm
	, prov_prtcp_sts_cd
	, clm_pl_of_srvc_desc
	, cos_prov_spcl_cd
	, proc_cd
	, proc_mod1_cd
	, proc_mod1_desc
	, proc_mod2_cd
	, proc_mod2_desc
	, ahrq_proc_genl_catgy_desc
	, ahrq_proc_dtl_catgy_desc
	, primary_diag_cd
	, ahrq_diag_genl_catgy_desc
	, ahrq_diag_dtl_catgy_desc
	, icd_2
	, icd_3
	, icd_4
	, icd_5
	, rvnu_cd
	, icd_ver_cd
	, tadm_hcta_util
	, sbmt_chrg_amt
	, allw_amt_fnl
	, net_pd_amt_fnl
	, srvc_unit_cnt
	, adj_srvc_unit_cnt
	, tadm_units
	, case when proc_cd in ('K0553','K0554','A4239','E2103') then 'Non-Adjunctive CGMs'
		when proc_cd in ('E2102','A4238','A9278','A9276','A9277') then 'Adjuctive CGMs'
	end as CGM_code_type
	, case when (hce_qtr in ('2018Q1','2018Q2') and adjd_dt <'2018-07-20') then '1'
		when (hce_qtr in ('2019Q1','2019Q2') and adjd_dt <'2019-07-20') then '1'
		when (hce_qtr in ('2020Q1','2020Q2') and adjd_dt <'2020-07-20') then '1'
		when (hce_qtr in ('2021Q1','2021Q2') and adjd_dt <'2021-07-20') then '1'
		when (hce_qtr in ('2022Q1','2022Q2') and adjd_dt <'2022-07-20') then '1'
		when (hce_qtr in ('2023Q1','2023Q2') and adjd_dt <'2023-07-20') then '1'
		else '0' 
	end as run_out_adj   -- Creating runout adj flag
from fichsrv.cosmos_op 
where product_level_3_fnl not in ('INSTITUTIONAL') 
	and global_cap = 'NA'
	and tfm_include_flag = 1
	and clm_dnl_f = 'N'
	and proc_cd in ('K0553','K0554','A9276','A9277','A9278', 'E2101','E2102','E2103','A4239','A4238')
	and hce_month between '202401' and '202507';
;

alter table tmp_1m.kn_cgm_claims_op
set tblproperties ('bucketing_version' = '1');

desc formatted tmp_1m.kn_cgm_claims_op;

select count (*) from tmp_1m.kn_cgm_claims_op;
-- 807,248


-- narrowing down medical CGMs to eventually union to pharm CGMs 
drop table tmp_1m.kn_cgm_medical;
create table tmp_1m.kn_cgm_medical stored as orc as 
select
	component
	, 'Medical' as cgm_source
	, hce_service_code
	, fst_srvc_year
	, fst_srvc_month
	, fst_srvc_dt
	, eventkey
	, plan_level_1_fnl
	, plan_level_2_fnl
	, product_level_1_fnl
	, product_level_2_fnl
	, product_level_3_fnl
	, tfm_product_new_fnl
	, market_fnl
	, group_ind_fnl
	, gal_mbi_hicn_fnl
	, prov_prtcp_sts_cd
	, proc_cd
	, proc_mod1_cd
	, proc_mod1_desc
	, proc_mod2_cd
	, proc_mod2_desc
	, case when primary_diag_cd in ('E0800','E0801','E0810','E0811','E0821','E0822','E0829','E08311','E08319','E083211','E083212','E083213','E083219','E083291','E083292','E083293','E083299','E083311',
		'E083312','E083313','E083319','E083391','E083392','E083393','E083399','E083411','E083412','E083413','E083419','E083491','E083492','E083493','E083499','E083511','E083512','E083513','E083519',
		'E083521','E083522','E083523','E083529','E083531','E083532','E083533','E083539','E083541','E083542','E083543','E083549','E083551','E083552','E083553','E083559','E083591','E083592','E083593',
		'E083599','E0836','E0837X1','E0837X2','E0837X3','E0837X9','E0839','E0840','E0841','E0842','E0843','E0844','E0849','E0851','E0852','E0859','E08610','E08618','E08620','E08621','E08622','E08628',
		'E08630','E08638','E08641','E08649','E0865','E0869','E088','E089','E0900','E0901','E0910','E0911','E0921','E0922','E0929','E0911','E0919','E09211','E09212','E09213','E09219','E09291','E09292',
		'E09293','E09299','E09311','E09312','E09313','E09319','E09391','E09392','E09393','E09399','E09411','E09412','E09413','E09419','E09491','E09492','E09493','E09499','E09511','E09512','E09513','E09519',
		'E09521','E09522','E09523','E09529','E09531','E09532','E09533','E09539','E09541','E09542','E09543','E09549','E09551','E09552','E09553','E09559','E09591','E09592','E09593','E09599','E0936','E097X1',
		'E097X2','E097X3','E097X9','E0939','E0940','E0941','E0942','E0943','E0944','E0949','E0951','E0952','E0959','E09610','E09618','E09620','E09621','E09622','E09628','E09630','E09638','E09641','E09649',
		'E0965','E0969','E098','E099','E1010','E1011','E1021','E1022','E1029','E10311','E10319','E10211','E10212','E10213','E10219','E10291','E10292','E10293','E10299','E10311','E10312','E10313','E10319',
		'E10391','E10392','E10393','E10399','E10411','E10412','E10413','E10419','E10491','E10492','E10493','E10499','E1042','E1043','E1044','E1049','E1051','E1052','E1059','E10610','E10618','E10620','E10621',
		'E10622','E10628','E10630','E10638','E10641','E10649','E1065','E1069','E108','E109','E1100','E1101','E1110','E1111','E1121','E1122','E1129','E11311','E11319','E113211','E113212','E113213','E113219',
		'E113291','E113292','E113293','E113299','E113311','E113312','E113313','E113319','E113391','E113392','E113393','E113399','E113411','E113412','E113413','E113419','E113491','E113492','E113493','E113499',
		'E113511','E113512','E113513','E113519','E113521','E113522','E113523','E113529','E113531','E113532','E113533','E113539','E113541','E113542','E113543','E113549','E113551','E113552','E113553','E113559',
		'E113591','E113592','E113593','E113599','E1136','E1137X1','E1137X2','E1137X3','E1137X9','E1139','E1140','E1141','E1142','E1143','E1144','E1149','E1151','E1152','E1159','E11610','E11618','E11620',
		'E11621','E11622','E11628','E11630','E11638','E11641','E11649','E1165','E1169','E118','E119','E1300','E1301','E1310','E1311','E1321','E1322','E1329','E13311','E13319','E133211','E133212','E133213',
		'E133219','E133291','E133292','E133293','E133299','E133311','E133312','E133313','E133319','E133391','E133392','E133393','E133399','E133411','E133412','E133413','E133419','E133491','E133492','E133493',
		'E133499','E133511','E133512','E133513','E133519','E133521','E133522','E133523','E133529','E133531','E133532','E133533','E133539','E133541','E133542','E133543','E133549','E133551','E133552','E133553',
		'E133559','E133591','E133592','E133593','E133599','E1336','E1337X1','E1337X2','E1337X3','E1337X9','E1339','E1340','E1341','E1342','E1343','E1344','E1349','E1351','E1352','E1359','E13610','E13618',
		'E13620','E13621','E13622','E13628','E13630','E13638','E13641','E13649','E1365','E1369','E138','E139','O24011','O24012','O24013','O24019','O2402','O2403','O24111','O24112','O24113','O24119','O2412',
		'O2413','O24311','O24312','O24313','O24319','O2432','O2433','O24811','O24812','O24813','O24819','O2482','O2483','O24911','O24912','O24913','O24919','O2492','O2493') THEN 1 ELSE 0 END AS POLICY_DIAG_FLAG
	, primary_diag_cd
	, tadm_hcta_util
	, allw_amt_fnl
	, net_pd_amt_fnl
	, adj_srvc_unit_cnt
	, cgm_code_type
from tmp_1m.kn_cgm_claims_op
;

select count(*) from tmp_1m.kn_cgm_mbi;
select count(distinct mbi) from tmp_1m.kn_cgm_mbi;


alter table tmp_1m.kn_cgm_medical
set tblproperties ('bucketing_version' = '1');

desc formatted tmp_1m.kn_cgm_medical;

desc formatted mard.m1_claims_f;

alter table tmp_1m.kn_cgm_mbi
set tblproperties ('bucketing_version' = '1');
alter table mard.m1_claims_f
set tblproperties ('bucketing_version' = '1');

--pulling pharm CGMs from bdpaas vs sas (change as of 9/25/2023)
drop table tmp_1m.kn_cgm_pharmacy;
create table tmp_1m.kn_cgm_pharmacy stored as orc 
tblproperties('bucketing_version' = '1') as 
select
    a.mbi
    , a.auth_month
    , a.case_id
    , date_of_service 
    , servicemonth 
    , gpi 
    , gpi_class 
    , drug_name 
    , ndc_hcedtl 
    , product_service_id 
    , market_fnl 
    , case 
        when group_ind = 'Y' then 'G' 
        else 'I' 
    end as group_ind 
    , product_level_1_fnl 
    , product_level_2_fnl 
    , product_level_3_fnl 
    , tadm_tfm_product_new 
    , tadm_allowed 
    , tadm_net_pd 
    , script_cnt 
    , rx31 
    , case 
        when brand_fnl = 'C&S' then 'C&S DUALS'
        when product_level_3_fnl = 'INSTITUTIONAL' then 'M&R Non-FFS'
        when tadm_source = 'COSMOS' and migration_source <> 'OAH' and tadm_global_cap = 'NA' then 'M&R FFS'
        when tadm_source = 'NICE' and nce_tadm_dec_risk_type <> 'GLOBAL' then 'M&R FFS' 
        else 'M&R Non-FFS' 
    end as ffs_flag 
    , case 
        when product_service_id 
            in ('08627009111', '08627001601', '08627005303', '57599080300', '57599080000', '57599000200', 
                '57599000101', '57599083500') then 'Covered'
        else 'Non-Covered' 
    end as cgm_coverage
    , brand_fnl
    , tadm_tfm_include_flag
    , tadm_source
    , tadm_partb_flag
from tmp_1m.kn_cgm_mbi as a
left join mard.m1_claims_f as b
	on a.mbi = b.mbi_hicn_fnl
where product_service_id 
    in ('08627009111', '08627001601', '08627005303', '08627007801', '08627007701', '57599080300', '57599080000', 
        '57599000200', '57599000101', '57599081800', '76300000805', '43169070405', '63000017962', '63000033698', 
        '63000035844', '43169095568', '63000028678', '63000031699', '63000035751', '76300000260', '63000028585', 
        '57599081800', '57599000021', '57599082000', '57599084400', '57599083500', '76300000260', '63000028585', 
        '43169095568', '63000028677', '63000028678', '63000031699', '63000035751', '76300023982', '76300070501', 
        '76300010002', '76300070601', '63000017962', '63000033698', '63000035844', '76300017962', '43169070405', 
        '63000041338', '63000051968', '63000044515', '63000044516')
    -- and brand_fnl = 'M&R'
    -- and tadm_tfm_include_flag = 1
    -- and product_level_3_fnl not in ('INSTITUTIONAL')
    -- and tadm_source = 'COSMOS'
    -- and tadm_partb_flag = 'Y'
    and year(date_of_service) in ('2024', '2025')
;

select tadm_source from mard.m1_claims_f group by tadm_source;

-- month n_auth n_auth with claims within past 6 months 
-- 202501 1000 500 (starting 202408)

select count(distinct mbi) from tmp_1m.kn_cgm_pharmacy;
select count(distinct mbi) from tmp_1m.kn_cgm_mbi;


select count(mbi) from tmp_1m.kn_cgm_pharmacy;

drop table if exists tmp_1m.kn_cgm_202501;
create temporary table tmp_1m.kn_cgm_202501 as
with auth_202501 as (
select distinct
	mbi
	, auth_month
from tmp_1m.kn_cgm_mbi
where auth_month = '202501'
),
claims_202501_lag6m as (
select 
	a.mbi
	, a.auth_month
	, b.claim_month
from auth_202501 as a
left join tmp_1m.kn_cgm_pharmacy as b
	on a.mbi = b.mbi
where b.claim_month between '202408' and '202501'
)
select 
	auth_month
	, claim_month
	, count(mbi) as mm
from claims_202501_lag6m
group by 
	auth_month
	, claim_month
;

select
	auth_month
	, count(distinct case_id) as n_auth
	, count(distinct mbi) as mm
from tmp_1m.kn_cgm_mbi
group by auth_month
order by auth_month
;










select count(*) from tmp_1m.kn_cgm_pharmacy;

alter table tmp_1m.kn_cgm_pharmacy
set tblproperties ('bucketing_version' = '1');

desc formatted tmp_1m.kn_cgm_pharmacy;

select count(distinct mbi) from tmp_1m.kn_cgm_auth;

select count(distinct mbi) from tmp_1m.kn_cgm_mbi

select * from tmp_1m.kn_cgm_mbi;

drop table tmp_1m.kn_cgm_mgi

-- V1: include the current month in the timeframe
-- If the observations are 20250117, 20250217, 20250317, 20250617
-- Then the results are 1, 2, 3, 4 claims 
-- In the 6 months timeframe from 20250117, there is 1 claim in 20250117
drop table if exists tmp_1m.kn_cgm_pharmacy_count_6m_v1;
create table tmp_1m.kn_cgm_pharmacy_count_6m_v1 as
with distinct_pharm as (
	select distinct
		mbi
		, date_of_service as service_date
		, cast(servicemonth as varchar(10)) as service_month
from tmp_1m.kn_cgm_pharmacy
where mbi is not null
)
select
	a.mbi
	, a.service_date
	, a.service_month
	, date_sub(a.service_date, 180) as service_date_lag6m
	, count(b.date_of_service) as n_claims_6m
from distinct_pharm as a 
left join tmp_1m.kn_cgm_pharmacy as b
	on a.mbi = b.mbi
	and b.date_of_service between date_sub(a.service_date, 180) and a.service_date
group by
	a.mbi
	, a.service_date
	, a.service_month
;
alter table tmp_1m.kn_cgm_pharmacy_count_6m_v1
set tblproperties ('bucketing_version' = '1');

desc formatted tmp_1m.kn_cgm_pharmacy_count_6m_v1;

select * from tmp_1m.kn_cgm_pharmacy_count_6m_v1;
where mbi = '1A00U23XD31';

select * from tmp_1m.kn_cgm_pharmacy
where mbi = '1A00U23XD31';



-- V2: exclude the current month in the timeframe
-- If the observations are 20250117, 20250217, 20250317, 20250617
-- Then the results are 0, 1, 2, 3 claims 
-- 6 months BEFORE 20250117, there are no claims
drop table if exists tmp_1m.kn_cgm_pharmacy_count_6m_v2;
create table tmp_1m.kn_cgm_pharmacy_count_6m_v2 as
with distinct_pharm as (
	select distinct
		mbi
		, date_of_service as service_date
		, cast(servicemonth as varchar(10)) as service_month
from tmp_1m.kn_cgm_pharmacy
where mbi is not null
)
select
	a.mbi
	, a.service_date
	, a.service_month
	, date_sub(a.service_date, 180) as service_date_lag6m
	, count(b.date_of_service) as n_claims_6m
from distinct_pharm as a 
left join tmp_1m.kn_cgm_pharmacy as b
	on a.mbi = b.mbi
	and b.date_of_service between date_sub(a.service_date, 180) and date_sub(a.service_date, 1)
group by
	a.mbi
	, a.service_date
	, a.service_month
;

alter table tmp_1m.kn_cgm_pharmacy_count_6m_v2
set tblproperties ('bucketing_version' = '1');

desc formatted tmp_1m.kn_cgm_pharmacy_count_6m_v2;

select * from tmp_1m.kn_cgm_pharmacy_count_6m_v2
where mbi = '1A00U23XD31';

select * from tmp_1m.kn_cgm_pharmacy
where mbi = '1A00U23XD31'
;

select service_month
	, sum(n_claims_6m) as n_v1
from tmp_1m.kn_cgm_pharmacy_count_6m_v1
group by service_month
;

--202401	75817
--202402	112005
--202403	160306
--202404	204862
--202405	255941
--202406	284577
--202407	339724
--202408	338309
--202409	332049
--202410	365468
--202411	345420
--202412	387766
--202501	379388
--202502	362513
--202503	429168
--202504	433068
--202505	455664
--202506	487209




select service_month
	, sum(n_claims_6m) as n_v2
from tmp_1m.kn_cgm_pharmacy_count_6m_v2
group by service_month
;
--202401	9685
--202402	45274
--202403	89151
--202404	134182
--202405	180837
--202406	212357
--202407	260015
--202408	259894
--202409	254631
--202410	278709
--202411	264511
--202412	297745
--202501	273756
--202502	262727
--202503	313993
--202504	322356
--202505	343170
--202506	368608


select sum(n_claims_6m) as n_v1
from tmp_1m.kn_cgm_pharmacy_count_6m_v1
;
-- 5749254


select sum(n_claims_6m) as n_v2
from tmp_1m.kn_cgm_pharmacy_count_6m_v2
;
-- 4171601

-- Auth;

drop table if exists tmp_1m.kn_cgm_mbi;
create table tmp_1m.kn_cgm_mbi stored as orc as
select distinct mbi from tmp_1m.kn_pcgm_auth
where mbi != 'NULL' or mbi is not null
;


select count(*) from tmp_1m.kn_cgm_mbi;









































































































































--select max(date_of_service) from tmp_1m.kn_cgm_rx;

-- narrowing down pharm CGMs to eventually union to medical CGMs 
drop table tmp_1m.kn_cgm_pharmacy_tounion ;
create table tmp_1m.kn_cgm_pharmacy_tounion stored as orc as
select
    'PHARM' as component
    , 'Pharmacy' as cgm_source
    , 'Pharmacy' as hce_service_code
    , cast(year(date_of_service) as varchar(10)) as fst_srvc_year
    , cast(servicemonth as          varchar(10)) as servicemonth
    , date_of_service
    , concat(mbi, date_of_service) as event_key
    , 'Pharmacy' as plan_level_1_fnl
    , 'Pharmacy' as plan_level_2_fnl
    , product_level_1_fnl
    , product_level_2_fnl
    , product_level_3_fnl
    , tadm_tfm_product_new
    , market_fnl
    , group_ind as group_ind_fnl
    , mbi
    , 'NA' as prov_prtcp_sts_cd
    , product_service_id as proc_cd
    , 'NA' as proc_mod1_cd
    , 'NA' as proc_mod1_desc
    , 'NA' as proc_mod2_cd
    , 'NA' as proc_mod2_desc
    , 0 as policy_diag_flag
    , 'PHARM' primary_diag_cd
    , script_cnt as tadm_hcta_util
    , tadm_allowed as allw_amt_fnl
    , tadm_net_pd as net_pd_amt_fnl
    , rx31 as adj_srvc_unit_cnt
    , case 
        when product_service_id = '08627001601' then 'DEXCOM G6 TRANSMITTER'
        when product_service_id = '08627005303' then 'DEXCOM G6 SENSOR'
        when product_service_id = '08627007701' then 'DEXCOM G7 SENSOR'
        when product_service_id = '08627007801' then 'DEXCOM G7 RECEIVER'
        when product_service_id = '08627009111' then 'DEXCOM G6 RECEIVER'
        when product_service_id = '57599000101' then 'FREESTYLE LIBRE 14 DAY/SENSOR/FLASH MONITORING SYSTEM'
        when product_service_id = '57599000200' then 'FREESTYLE LIBRE 14 DAY/READER/FLASH MONITORING SYSTEM'
        when product_service_id = '57599080000' then 'FREESTYLE LIBRE 2/SENSOR/FLASH GLUCOSE MONITORING SYSTEM'
        when product_service_id = '57599080300' then 'FREESTYLE LIBRE 2/READER/FLASH GLUCOSE MONITORING SYSTEM'
        when product_service_id = '57599081800' then 'FREESTYLE LIBRE 3/SENSOR/GLUCOSE MONITORING SYSTEM'
        when product_service_id = '57599083500' then 'FREESTYLE LIBRE 2 SENSOR PLUS KIT'
        when product_service_id = '57599000021' then 'ABBOTT Reader'
        when product_service_id = '76300000260' then 'Guardian Transmitter'
        when product_service_id = '63000028585' then 'GUARDIAN CONNECT TRANSMITTER KIT'
        when product_service_id = '43169095568' then 'GUARDIAN LINK 3 TRANSMITTER KIT'
        when product_service_id = '63000028677' then 'GUARDIAN LINK 3 TRANSMITTER KIT'
        when product_service_id = '63000028678' then 'GUARDIAN LINK 3 TRANSMITTER KIT'
        when product_service_id = '63000031699' then 'GUARDIAN LINK 3 TRANSMITTER KIT'
        when product_service_id = '63000035751' then 'GUARDIAN LINK 3 TRANSMITTER KIT'
        when product_service_id = '76300023982' then 'GUARDIAN LINK 3 TRANSMITTER KIT'
        when product_service_id = '76300070501' then 'GUARDIAN REAL-TIME CHARGER REPLACEMENT'
        when product_service_id = '76300010002' then 'GUARDIAN REAL-TIME REPLACEMENT MONITOR PEDIATRIC'
        when product_service_id = '76300070601' then 'GUARDIAN REAL-TIME TEST PLUG REPLACEMENT'
        when product_service_id = '63000017962' then 'GUARDIAN SENSOR (3)'
        when product_service_id = '63000033698' then 'GUARDIAN SENSOR (3)'
        when product_service_id = '63000035844' then 'GUARDIAN SENSOR (3)'
        when product_service_id = '76300017962' then 'GUARDIAN SENSOR (3)'
        when product_service_id = '43169070405' then 'GUARDIAN SENSOR (3)'
        when product_service_id = '63000041338' then 'GUARDIAN 4 GLUCOSE SENSOR'
        when product_service_id = '63000051968' then 'GUARDIAN 4 GLUCOSE SENSOR'
        when product_service_id = '63000044515' then 'GUARDIAN 4 TRANSMITTER KIT'
        when product_service_id = '63000044516' then 'GUARDIAN 4 TRANSMITTER KIT'
        else 'Pharmacy' 
    end as cgm_code_type
    , '0' as
from tmp_1m.kn_cgm_pharmacy
where mbi is not null
    and ffs_flag = 'M&R FFS'
;

alter table tmp_1m.kn_cgm_pharmacy_tounion
set tblproperties ('bucketing_version' = '1');

desc formatted tmp_1m.kn_cgm_pharmacy_tounion;

--union of Medical and Pharmacy CGMs 
drop table tmp_1m.kn_cgm_medical_pharmacy;
create table tmp_1m.kn_cgm_medical_pharmacy stored as orc as
select
    * 
from tmp_1m.kn_cgm_medical
union all 
select
    * 
from tmp_1m.kn_cgm_pharmacy_tounion
;

alter table tmp_1m.kn_cgm_medical_pharmacy
set tblproperties ('bucketing_version' = '1');

desc formatted tmp_1m.kn_cgm_medical_pharmacy;

--2125646
--select count(*) from tmp_1m.kn_cgm_medical_pharmacy;




















-- Non claims stuff;
--distinct member list with dos 
drop table tmp_1m.kn_CGM_mems;
CREATE TABLE tmp_1m.kn_CGM_mems stored as orc as 
	select distinct gal_mbi_hicn_fnl
		,fst_srvc_dt
	from  tmp_1m.kn_cgm_medical_pharmacy
;

--Creating unique flag for start dates per member
drop table tmp_1m.kn_CGM_mems1;
CREATE TABLE tmp_1m.kn_CGM_mems1 as
SELECT	
	gal_mbi_hicn_fnl 
	,min(fst_srvc_dt) as first_start 
from tmp_1m.kn_cgm_medical_pharmacy
where gal_mbi_hicn_fnl is not null 
group by gal_mbi_hicn_fnl
;

--tagging on member start date
drop table tmp_1m.kn_CGM_mems2;
CREATE TABLE tmp_1m.kn_CGM_mems2 stored as orc as 
select a.*
		,b.first_start
	from tmp_1m.kn_CGM_mems as a
	left join tmp_1m.kn_CGM_mems1 as b
		on a.gal_mbi_hicn_fnl = b.gal_mbi_hicn_fnl
;


--Creating first start vs continuing 
drop table tmp_1m.kn_CGM_mems3;                   
CREATE TABLE tmp_1m.kn_CGM_mems3 stored as orc as 
SELECT
	*
	,case when fst_srvc_dt = first_start then 'First Start'
		when fst_srvc_dt > first_start then 'Continuing User' else 'NA' end as total_status
from tmp_1m.kn_CGM_mems2
;

--Medical dos member list 
drop table tmp_1m.kn_CGM_mems_med; 
	create table tmp_1m.kn_CGM_mems_med stored as orc as
	select distinct gal_mbi_hicn_fnl
		,fst_srvc_dt
	from tmp_1m.kn_cgm_medical_pharmacy
	where component='OP'
;

--Medical First starts
drop table tmp_1m.kn_CGM_mems_med1;
CREATE TABLE tmp_1m.kn_CGM_mems_med1 stored as orc as
SELECT
	gal_mbi_hicn_fnl 
	,min(fst_srvc_dt) as new_st_dt_med
from tmp_1m.kn_CGM_mems_med
where gal_mbi_hicn_fnl is not null 
group by gal_mbi_hicn_fnl 
;

--medical join 
drop table tmp_1m.kn_CGM_mems_med2;
CREATE TABLE tmp_1m.kn_CGM_mems_med2 stored as orc as 
SELECT a.*
		,b.new_st_dt_med as first_start_med
	from tmp_1m.kn_CGM_mems_med as a
	left join tmp_1m.kn_CGM_mems_med1 as b
		on a.gal_mbi_hicn_fnl = b.gal_mbi_hicn_fnl
;

--first start vs continuing med user
drop table tmp_1m.kn_CGM_mems_med3;                   
CREATE TABLE tmp_1m.kn_CGM_mems_med3 stored as orc as 
SELECT
	*
	,case when fst_srvc_dt = first_start_med then 'First Med Start'
		when fst_srvc_dt > first_start_med then 'Continuing Med User' else 'NA' end as status_med
from tmp_1m.kn_CGM_mems_med2
;


--Pharm dos member list 
drop table tmp_1m.kn_CGM_mems_pharm; 
	create table tmp_1m.kn_CGM_mems_pharm stored as orc as
	select distinct gal_mbi_hicn_fnl
		,fst_srvc_dt
	from tmp_1m.kn_cgm_medical_pharmacy
	where component='PHARM'
;

--Medical First starts
drop table tmp_1m.kn_CGM_mems_pharm1;
CREATE TABLE tmp_1m.kn_CGM_mems_pharm1 stored as orc as
SELECT
	gal_mbi_hicn_fnl 
	,min(fst_srvc_dt) as new_st_dt_pharm 
from tmp_1m.kn_CGM_mems_pharm
where gal_mbi_hicn_fnl is not null 
group by gal_mbi_hicn_fnl 
;

--medical join 
drop table tmp_1m.kn_CGM_mems_pharm2;
CREATE TABLE tmp_1m.kn_CGM_mems_pharm2 stored as orc as 
SELECT a.*
		,b.new_st_dt_pharm as first_start_pharm
	from tmp_1m.kn_CGM_mems_pharm as a
	left join tmp_1m.kn_CGM_mems_pharm1 as b
		on a.gal_mbi_hicn_fnl = b.gal_mbi_hicn_fnl
;

--first start vs continuing med user
drop table tmp_1m.kn_CGM_mems_pharm3;                   
CREATE TABLE tmp_1m.kn_CGM_mems_pharm3 stored as orc as 
SELECT
	*
	,case when fst_srvc_dt = first_start_pharm then 'First Pharm Start'
		when fst_srvc_dt > first_start_pharm then 'Continuing Pharm User' else 'NA' end as status_pharm
from tmp_1m.kn_CGM_mems_pharm2
;

--Users join 
drop table tmp_1m.kn_cgm_mems_firststart; 
create table tmp_1m.kn_cgm_mems_firststart stored as orc as
	select a.gal_mbi_hicn_fnl
		,a.fst_srvc_dt
		,a.total_status
		,b.status_med
		,c.status_pharm
	from tmp_1m.kn_CGM_mems3 as a
	left join tmp_1m.kn_CGM_mems_med3 as b
		on a.gal_mbi_hicn_fnl =b.gal_mbi_hicn_fnl
		and a.fst_srvc_dt = b.fst_Srvc_dt
	left join tmp_1m.kn_CGM_mems_pharm3 as c
		on a.gal_mbi_hicn_fnl =c.gal_mbi_hicn_fnl
		and a.fst_srvc_dt = c.fst_Srvc_dt
;

--Swictes vs starts
drop table tmp_1m.kn_cgm_mems_firststart1; 
create table tmp_1m.kn_cgm_mems_firststart1 stored as orc as
	select
	*
	,case when total_status='Continuing User' and status_pharm='First Pharm Start' then 'Pharm Switch' else status_pharm end as new_status_pharm
	,case when total_status='Continuing User' and status_med='First Med Start' then 'Med Switch' else status_med end as new_status_med
	,case when total_status='Continuing User' then '2' else '1' end as status_key
from tmp_1m.kn_cgm_mems_firststart
;
	
--Rolling up on Month 
drop table tmp_1m.kn_cgm_mems_firststart2; 
create table tmp_1m.kn_cgm_mems_firststart2 stored as orc as
select
	gal_mbi_hicn_fnl
	,concat(lpad(year(fst_srvc_dt),4,0),lpad(month(fst_srvc_dt),2,0)) as fst_srvc_month
	,min(status_key) as status_key
	,max(new_status_med) as status_med
	,max(new_status_pharm) as status_pharm
from tmp_1m.kn_cgm_mems_firststart1
group by gal_mbi_hicn_fnl
	,concat(lpad(year(fst_srvc_dt),4,0),lpad(month(fst_srvc_dt),2,0))
	;

drop table tmp_1m.kn_cgm_mems_firststart3; 
create table tmp_1m.kn_cgm_mems_firststart3 stored as orc as
select
	*
	,case when status_key=1 then 'First Start'
		when status_key=2 then 'Continuing User' else 'NA' end as status_total
from tmp_1m.kn_cgm_mems_firststart2
;

--joining on first start flags 
drop table tmp_1m.kn_CGM_5a;
CREATE TABLE tmp_1m.kn_CGM_5a  as 
SELECT
	a.* 
	,b.status_med
	,b.status_pharm
from tmp_1m.kn_cgm_medical_pharmacy as a 
left join tmp_1m.kn_cgm_mems_firststart3 as b
on a.gal_mbi_hicn_fnl=b.gal_mbi_hicn_fnl 
and a.fst_srvc_month=b.fst_srvc_month 
;
--2221573
select count(*) from tmp_1m.kn_CGM_5a;

drop table tmp_1m.kn_CGM_5b;
CREATE TABLE tmp_1m.kn_CGM_5b stored as orc as 
SELECT
	a.* 
	,b.status_total

from tmp_1m.kn_CGM_5a as a 
left join tmp_1m.kn_cgm_mems_firststart3 as b
on a.gal_mbi_hicn_fnl=b.gal_mbi_hicn_fnl 
and a.fst_srvc_month=b.fst_srvc_month 
;
--2221573
select count(*) from tmp_1m.kn_CGM_5b;

--distincting join 
drop table tmp_1m.kn_CGM_5;
CREATE TABLE tmp_1m.kn_CGM_5 stored as orc as 
SELECT distinct
	*
from  tmp_1m.kn_CGM_5b
;

--Diabetes Members 
--all but 2019 (ccw mising 2019/2020) 
drop table tmp_1m.kn_diabetes_1;
create table tmp_1m.kn_diabetes_1 stored as orc as 
select distinct
	hicn as mbi,
	year_mo,
	substring(year_mo, 1,4) as year,
	ccw_qualified_diab 
	,1 as diabetes_flag
from fichsrv.mnr_ccw_conditions_202502 
where ccw_qualified_diab = '1'
	and substring(year_mo, 1,4) in ('2018','2021','2022','2023','2024','2025');
--97629896
select count (*) from tmp_1m.kn_diabetes_1;

--pulling 2019/2020 from an archived table 
drop table tmp_1m.kn_diabetes_19;
create table tmp_1m.kn_diabetes_19 stored as orc as 
select distinct
	hicn as mbi,
	year_mo,
	substring(year_mo, 1,4) as year,
	ccw_qualified_diab*1 as ccw_qualified_diab
	,1 as diabetes_flag
from fichsrv.mnr_ccw_conditions_19_22
where ccw_qualified_diab = '1'
	and substring(year_mo, 1,4) in ('2019','2020');
--41201897
select count (*) from tmp_1m.kn_diabetes_19;

--Combining all years with 2019/2020
drop table tmp_1m.kn_diabetes_2;
create table tmp_1m.kn_diabetes_2 stored as orc as 
select 
* from tmp_1m.kn_diabetes_1 
union all 
select * from tmp_1m.kn_diabetes_19
; 
--138831793
select count (*) from tmp_1m.kn_diabetes_2;

--for CGM Forecast Diabetic Members column: 
select count(DISTINCT mbi), year_mo from tmp_1m.kn_diabetes_2 where ccw_qualified_diab =1 group by year_mo ; 

--adding diabetes flag to cgm members 
drop table tmp_1m.kn_CGM_6;
CREATE TABLE tmp_1m.kn_CGM_6 stored as orc as 
SELECT
	a.*
	,b.diabetes_flag
from tmp_1m.kn_CGM_5 as a
left join tmp_1m.kn_diabetes_2 as b 
on a.gal_mbi_hicn_fnl=b.mbi 
and a.fst_srvc_month=b.year_mo 
; 
--2221462
select count(*) from tmp_1m.kn_CGM_6;


--Insulin Flag 
--change as of 9/25 pulling insulin from BDPaas vs SAS 
drop table tmp_1m.kn_cgm_insulin1;
create table tmp_1m.kn_cgm_insulin1 as select
mbi_hicn_fnl as mbi, 
date_of_service,
servicemonth,
gpi,
gpi_class, 
NDC_HCEDTL,
product_service_id,
market_fnl,
group_ind,
PRODUCT_LEVEL_1_FNL,
PRODUCT_LEVEL_2_FNL,
PRODUCT_LEVEL_3_FNL,
tadm_tfm_product_new,
plan_drug_status,
tadm_allowed,
tadm_dayssupply,
script_cnt,
rx31
from mard.m1_claims_f 
where gpi_class = 'INSULIN'
and brand_fnl='M&R'
and tadm_tfm_include_flag=1
and product_level_3_fnl NOT IN ('INSTITUTIONAL') 
and tadm_source='COSMOS'
;

--Converting tadm_dayssupply to INT
drop table tmp_1m.kn_cgm_insulin_1a;
create table tmp_1m.kn_cgm_insulin_1a as
select
	*,
	cast(tadm_dayssupply as INT) as supply_days
from tmp_1m.kn_cgm_insulin1;

--adding number of days in supply to date of service 
drop table tmp_1m.kn_cgm_insulin_1;
create table tmp_1m.kn_cgm_insulin_1 as
select
	*
	,date_add(date_of_service,supply_days) as supply_thru_dt
from tmp_1m.kn_cgm_insulin_1a;

--Adding insulin start and end months
drop table tmp_1m.kn_cgm_insulin_2;
create table tmp_1m.kn_cgm_insulin_2 as
select
	*
	,cast(substring(date_of_service,1,4)||substring(date_of_service,6,2) as varchar(6)) as insulin_start_mo
	,cast(substring(supply_thru_dt,1,4)||substring(supply_thru_dt,6,2) as varchar(6)) as insulin_end_mo
from tmp_1m.kn_cgm_insulin_1;


--Pulling a list of months
drop table tmp_1m.kn_cgm_months;
create table tmp_1m.kn_cgm_months as
select distinct 
	fin_inc_month as month
from fichsrv.tre_membership 
where fin_inc_month between '201801' and '202507';



--Pulling list of members on insulin and all months they are on an insulin supply
drop table tmp_1m.kn_cgm_insulin_3;
create table tmp_1m.kn_cgm_insulin_3 as
select
	a.mbi
	,case when a.ndc_hcedtl ='RAPID-ACTING INSULINS' then 'Fast Acting Insulin' else 'Non-Fast Insulin' end as insulin_type
	,a.insulin_start_mo
	,a.insulin_end_mo
	,b.month
from tmp_1m.kn_cgm_insulin_2 as a
inner join tmp_1m.kn_cgm_months as b 
	on b.month between a.insulin_start_mo and a.insulin_end_mo 
order by mbi, month;
--28604017
select count (*) from tmp_1m.kn_cgm_insulin_3;

--time on insulin
drop table tmp_1m.kn_cgm_insulin_3a;
create table tmp_1m.kn_cgm_insulin_3a as
select  
	mbi
	,min(insulin_start_mo) as start_mo
	,max(insulin_end_mo) as end_mo
from tmp_1m.kn_cgm_insulin_3
group by mbi 
;
--741527
select count(*) from tmp_1m.kn_cgm_insulin_3a;


drop table tmp_1m.kn_cgm_insulin_3b;
create table tmp_1m.kn_cgm_insulin_3b as
select  
	mbi
	,start_mo
	,end_mo
	,case when substring(start_mo,1,4)='2025' and substring(end_mo,1,4)='2025' then (end_mo-start_mo) 
		when substring(start_mo,1,4)='2024' and substring(end_mo,1,4)='2025' then (end_mo-start_mo-88) 
		when substring(start_mo,1,4)='2023' and substring(end_mo,1,4)='2025' then ((end_mo-start_mo)-176)
		when substring(start_mo,1,4)='2022' and substring(end_mo,1,4)='2025' then ((end_mo-start_mo)-264)
		when substring(start_mo,1,4)='2021' and substring(end_mo,1,4)='2025' then ((end_mo-start_mo)-352)
		when substring(start_mo,1,4)='2020' and substring(end_mo,1,4)='2025' then ((end_mo-start_mo)-440)
		when substring(start_mo,1,4)='2019' and substring(end_mo,1,4)='2025' then ((end_mo-start_mo)-528)
		when substring(start_mo,1,4)='2024' and substring(end_mo,1,4)='2024' then (end_mo-start_mo) 
		when substring(start_mo,1,4)='2023' and substring(end_mo,1,4)='2024' then (end_mo-start_mo-88) 
		when substring(start_mo,1,4)='2022' and substring(end_mo,1,4)='2024' then ((end_mo-start_mo)-176)
		when substring(start_mo,1,4)='2021' and substring(end_mo,1,4)='2024' then ((end_mo-start_mo)-264)
		when substring(start_mo,1,4)='2020' and substring(end_mo,1,4)='2024' then ((end_mo-start_mo)-352)
		when substring(start_mo,1,4)='2019' and substring(end_mo,1,4)='2024' then ((end_mo-start_mo)-440)
		when substring(start_mo,1,4)='2018' and substring(end_mo,1,4)='2024' then ((end_mo-start_mo)-528)
		when substring(start_mo,1,4)='2023' and substring(end_mo,1,4)='2023' then (end_mo-start_mo) 
		when substring(start_mo,1,4)='2022' and substring(end_mo,1,4)='2023' then ((end_mo-start_mo)-88)
		when substring(start_mo,1,4)='2021' and substring(end_mo,1,4)='2023' then ((end_mo-start_mo)-176)
		when substring(start_mo,1,4)='2020' and substring(end_mo,1,4)='2023' then ((end_mo-start_mo)-264)
		when substring(start_mo,1,4)='2019' and substring(end_mo,1,4)='2023' then ((end_mo-start_mo)-352)
		when substring(start_mo,1,4)='2018' and substring(end_mo,1,4)='2023' then ((end_mo-start_mo)-440)
		when substring(start_mo,1,4)='2022' and substring(end_mo,1,4)='2022' then (end_mo-start_mo) 
		when substring(start_mo,1,4)='2021' and substring(end_mo,1,4)='2022' then ((end_mo-start_mo)-88)
		when substring(start_mo,1,4)='2020' and substring(end_mo,1,4)='2022' then ((end_mo-start_mo)-176)
		when substring(start_mo,1,4)='2019' and substring(end_mo,1,4)='2022' then ((end_mo-start_mo)-264)
		when substring(start_mo,1,4)='2018' and substring(end_mo,1,4)='2022' then ((end_mo-start_mo)-352)
		when substring(start_mo,1,4)='2021' and substring(end_mo,1,4)='2021' then (end_mo-start_mo) 
		when substring(start_mo,1,4)='2020' and substring(end_mo,1,4)='2021' then ((end_mo-start_mo)-88)
		when substring(start_mo,1,4)='2019' and substring(end_mo,1,4)='2021' then ((end_mo-start_mo)-176)
		when substring(start_mo,1,4)='2018' and substring(end_mo,1,4)='2021' then ((end_mo-start_mo)-264)
		when substring(start_mo,1,4)='2020' and substring(end_mo,1,4)='2020' then (end_mo-start_mo) 
		when substring(start_mo,1,4)='2019' and substring(end_mo,1,4)='2020' then ((end_mo-start_mo)-88)
		when substring(start_mo,1,4)='2018' and substring(end_mo,1,4)='2020' then ((end_mo-start_mo)-176)
		when substring(start_mo,1,4)='2019' and substring(end_mo,1,4)='2019' then (end_mo-start_mo) 
		when substring(start_mo,1,4)='2018' and substring(end_mo,1,4)='2019' then ((end_mo-start_mo)-88)
		when substring(start_mo,1,4)='2018' and substring(end_mo,1,4)='2018' then (end_mo-start_mo) 
		else 0 end as insulin_months
from tmp_1m.kn_cgm_insulin_3a
;
--741527
select count(*) from tmp_1m.kn_cgm_insulin_3b;

select * from tmp_1m.kn_cgm_insulin_3b where substring(start_mo,1,4)='2023' ;


--Distinct list of members and the months they are on an insulin supply
drop table tmp_1m.kn_cgm_insulin_4;
create table tmp_1m.kn_cgm_insulin_4 as
select distinct 
	mbi
	,MONTH
	,1 as insulin_month_flag
from tmp_1m.kn_cgm_insulin_3
order by mbi, month;
--16167489
select count (*) from tmp_1m.kn_cgm_insulin_4;

select count (distinct mbi) from tmp_1m.kn_cgm_insulin_4;
--741526

--Distinct list of members and the type they are on an insulin supply
drop table tmp_1m.kn_cgm_insulin_5;
create table tmp_1m.kn_cgm_insulin_5 as
select distinct 
	mbi
	,MONTH 
	,1 as fast_acting_insulin
from tmp_1m.kn_cgm_insulin_3
where insulin_type ='Fast Acting Insulin'
order by mbi, month;	
--6305645
select count (*) from tmp_1m.kn_cgm_insulin_5;

--adding insulin flag to cgm members 
drop table tmp_1m.kn_CGM_7a;
CREATE TABLE tmp_1m.kn_CGM_7a stored as orc as 
SELECT
	a.*
	,b.insulin_month_flag as insulin_flag
	,c.fast_acting_insulin
from tmp_1m.kn_CGM_6 as a
left join tmp_1m.kn_cgm_insulin_4 as b 
on a.gal_mbi_hicn_fnl=b.mbi 
and a.fst_srvc_month=b.month 
left join tmp_1m.kn_cgm_insulin_5 as c 
on a.gal_mbi_hicn_fnl=c.mbi 
and a.fst_srvc_month=c.month 
; 
--2125646
select count(*) from tmp_1m.kn_CGM_7a;

--adding insulin months flag to cgm members 
drop table tmp_1m.kn_CGM_7;
CREATE TABLE tmp_1m.kn_CGM_7 stored as orc as 
SELECT
	a.*
	,b.insulin_months 
from tmp_1m.kn_CGM_7a as a
left join tmp_1m.kn_cgm_insulin_3b as b
on a.gal_mbi_hicn_fnl=b.mbi 
;
--1819958
select count(*) from tmp_1m.kn_CGM_7;

--fixing null values in flags to 0s and adding mapd vs ma and demographics
drop table tmp_1m.kn_CGM_8;
CREATE TABLE tmp_1m.kn_CGM_8 stored as orc as 
SELECT
	a.component
	,a.cgm_source
	,a.hce_service_code
	,a.fst_srvc_year
	,a.fst_srvc_month
	,a.fst_srvc_dt
	,a.eventkey
	,a.plan_level_1_fnl
	,a.plan_level_2_fnl
	,a.product_level_1_fnl
	,a.product_level_2_fnl
	,a.product_level_3_fnl
	,a.tfm_product_new_fnl
	,a.market_fnl
	,a.group_ind_fnl
	,a.gal_mbi_hicn_fnl
	,a.prov_prtcp_sts_cd
	,a.proc_cd
	,a.proc_mod1_cd
	,a.proc_mod1_desc
	,a.proc_mod2_cd
	,a.proc_mod2_desc
	,a.policy_diag_flag
	,a.primary_diag_cd
	,a.tadm_hcta_util
	,a.allw_amt_fnl
	,a.net_pd_amt_fnl
	,a.adj_srvc_unit_cnt
	,case when a.proc_cd in ('A4239','A9276','A9277', 'E2102', 'E2103', 'K0553', 'K0554') then a.cgm_code_type
		when a.proc_cd in ('08627001601', '08627005303', '08627009111','57599080000', '57599080300', '57599081800','57599000101','57599000200','57599083500') then 'Non-Adjunctive CGMs' else 'Adjuctive CGMs' 
		end as cgm_code_type
	,case when a.proc_cd in ('E2103','E2102','08627009111','08627007801','57599080300','57599000200','K0554', 'A9278','76300070501','76300010002','76300070601') then 'Reciever'
		when a.proc_cd in ('A4239','A4238','K0553','A9276', 'A9277','08627005303','08627001601','57599080000', '57599000101','08627007701','57599081800','57599083500','76300000260','63000028585','43169095568','63000028677','63000028678','63000031699','63000035751',
	'76300023982','63000017962','63000033698','63000035844','76300017962','43169070405','63000041338','63000051968',
	'63000044515','63000044516') then 'Supplies' else 'Supplies' end as supply_v_reciever
	,a.
	,a.status_med
	,a.status_pharm
	,a.status_total
	,case when a.diabetes_flag is null then 0 else a.diabetes_flag end as diabetes_flag
	,case when a.insulin_flag is null then 0 else a.insulin_flag end as insulin_flag 
	,case when a.fast_acting_insulin is null then 0 else a.fast_acting_insulin end as fast_acting_insulin 
	,case when a.insulin_months is null then 0 else a.insulin_months end as insulin_months
	,b.fin_ma_mapd 
	,b.tadm_cohort 
	  ,CASE 
        WHEN FLOOR(b.AGE) < 65 THEN '0-64'
        WHEN FLOOR(b.AGE) = 65 THEN '65-69'        
        WHEN FLOOR(b.AGE) BETWEEN 65 and 69 THEN '65-69'
        WHEN FLOOR(b.AGE) BETWEEN 70 and 74 THEN '70-74'
        WHEN FLOOR(b.AGE) BETWEEN 75 and 79 THEN '75-79'
        WHEN FLOOR(b.AGE) BETWEEN 80 and 84 THEN '80-84'
        WHEN FLOOR(b.AGE) BETWEEN 85 and 89 THEN '85-89'
        WHEN FLOOR(b.AGE) >= 90 THEN '90+'
        ELSE 'OTHER' END AS AGE_RANGE 
	,case when b.fin_race_cd is null then 'Unknown'
		when b.fin_race_cd = '0' then 'Unknown'
		when b.fin_race_cd = '1' then 'White'
		when b.fin_race_cd = '2' then 'African American'
		when b.fin_race_cd = '3' then 'Other'
		when b.fin_race_cd = '4' then 'Asian'
		when b.fin_race_cd = '5' then 'Hispanic'
		when b.fin_race_cd = '6' then 'N. American Native'
		else 'Unknown' end as race 
	,b.fin_gender 
from tmp_1m.kn_CGM_7 as a 
left join fichsrv.tre_membership as b
on a.gal_mbi_hicn_fnl=b.fin_mbi_hicn_fnl 
and a.fst_srvc_month=b.fin_inc_month 
; 
--1819958
select count(*) from tmp_1m.kn_CGM_8;

--looking for hypoglycemia before CGM 
drop table tmp_1m.kn_CGM_hypo;
CREATE TABLE tmp_1m.kn_CGM_hypo stored as orc as 
SELECT  
	gal_mbi_hicn_fnl
	,fst_srvc_month
	,1 as hypoglycemia 
from fichsrv.cosmos_pr 
where primary_diag_cd in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_2 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_3 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_4 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_5 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_6 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_7 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_8 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_9 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_10 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_11 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_12 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_13 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_14 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_15 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_16 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_17 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_18 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_19 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_20 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_21 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_22 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_23 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_24 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_25 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
union all 
select  
	gal_mbi_hicn_fnl
	,fst_srvc_month
	,1 as hypoglycemia
from fichsrv.cosmos_op 
where primary_diag_cd in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_2 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_3 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_4 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_5 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_6 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_7 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_8 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_9 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_10 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_11 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_12 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_13 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_14 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_15 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_16 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_17 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_18 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_19 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_20 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_21 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_22 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_23 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_24 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
or icd_25 in ('E08641','E08649','E09641','E09649','E10641','E10649','E11641','E11649','E13641','E13649','E15','E160','E161','E162','T383X1A','T383X1D','T383X1S','T383X2A','T383X2D','T383X2S','T383X3A','T383X3D','T383X3S','T383X4A','T383X4D','T383X4S','T383X5A','T383X5D','T383X5S')
;

--creating a distinct list of hypoglycemia members 
drop table tmp_1m.kn_CGM_hypo_1;
CREATE TABLE tmp_1m.kn_CGM_hypo_1 stored as orc as 
SELECT  DISTINCT 
	* 
	
from tmp_1m.kn_cgm_hypo 
; 

--joining onto cgm members 
drop table tmp_1m.kn_CGM_hypo_2;
CREATE TABLE tmp_1m.kn_CGM_hypo_2 stored as orc as 
SELECT    
	a.gal_mbi_hicn_fnl 
	,a.fst_srvc_month 
	,case when b.hypoglycemia is null then 0 else 1 end as hypoglycemia
from tmp_1m.kn_CGM_8 as a 
left join tmp_1m.kn_cgm_hypo_1 as b 
on a.gal_mbi_hicn_fnl=b.gal_mbi_hicn_fnl 
and a.fst_srvc_month>b.fst_srvc_month 
;

--creating a distinct list 
drop table tmp_1m.kn_CGM_hypo_3;
CREATE TABLE tmp_1m.kn_CGM_hypo_3 stored as orc as 
SELECT   
	gal_mbi_hicn_fnl
	,fst_srvc_month
	,max(hypoglycemia) as hypoglycemia
	
from tmp_1m.kn_cgm_hypo_2 
group by gal_mbi_hicn_fnl ,fst_srvc_month 
; 

--adding on hypoglycemia flag 
drop table tmp_1m.kn_CGM_8a;
CREATE TABLE tmp_1m.kn_CGM_8a stored as orc as 
SELECT  
	a.* 
	,b.hypoglycemia
from tmp_1m.kn_cgm_8 as a
left join tmp_1m.kn_cgm_hypo_3 as b 
on a.gal_mbi_hicn_fnl=b.gal_mbi_hicn_fnl 
and a.fst_srvc_month=b.fst_srvc_month 
;


--rolling up data 
drop table tmp_1m.kn_CGM_9;
CREATE TABLE tmp_1m.kn_CGM_9 stored as orc as 
SELECT
	component
	,cgm_source
	,hce_service_code
	,fst_srvc_year
	,fst_srvc_month
	,plan_level_1_fnl
	,plan_level_2_fnl
	,product_level_1_fnl
	,product_level_2_fnl
	,product_level_3_fnl
	,tfm_product_new_fnl
	,market_fnl
	,group_ind_fnl
	,prov_prtcp_sts_cd
	,proc_cd
	,policy_diag_flag
--	,primary_diag_cd
	,cgm_code_type
	,supply_v_reciever
	,
	,status_med
	,status_pharm
	,status_total
	,diabetes_flag
	,insulin_flag
	,fast_acting_insulin
	,insulin_months
	,hypoglycemia
	,fin_ma_mapd
	,tadm_cohort
	,age_range
--	,race
--	,fin_gender
	,sum(tadm_hcta_util) as units
	,sum(allw_amt_fnl) as allowed
	,sum(net_pd_amt_fnl) as netpaid
	,sum(adj_srvc_unit_cnt) as adj_srvc_unit
	,0 as mm
	,0 as members
from tmp_1m.kn_CGM_8a
where fst_srvc_year in ('2022','2023','2024','2025')
group by 
	component
	,cgm_source
	,hce_service_code
	,fst_srvc_year
	,fst_srvc_month
	,plan_level_1_fnl
	,plan_level_2_fnl
	,product_level_1_fnl
	,product_level_2_fnl
	,product_level_3_fnl
	,tfm_product_new_fnl
	,market_fnl
	,group_ind_fnl
	,prov_prtcp_sts_cd
	,proc_cd
	,policy_diag_flag
--	,primary_diag_cd
	,cgm_code_type
	,supply_v_reciever
	,
		,status_med
	,status_pharm
	,status_total
	,diabetes_flag
	,insulin_flag
	,fast_acting_insulin
	,insulin_months
		,hypoglycemia
	,fin_ma_mapd
	,tadm_cohort
	,age_range
--	,race
--	,fin_gender
	;
--1505026
select count(*) from tmp_1m.kn_CGM_9;

--Member Months pull 
drop table tmp_1m.kn_CGM_mm;
CREATE TABLE tmp_1m.kn_CGM_MM stored as orc as 
SELECT
	'MM' as component
	,'MM' as cgm_source
	,'MM' as hce_service_code
	,fin_inc_year as fst_srvc_year
	,fin_inc_month as fst_srvc_month
	,FIN_PLAN_LEVEL_1 
	,FIN_PLAN_LEVEL_2 
	,FIN_PRODUCT_LEVEL_1 
	,FIN_PRODUCT_LEVEL_2 
	,FIN_PRODUCT_LEVEL_3 
	,fin_tfm_product_new 
	,fin_market as market_fnl
	,FIN_G_I as group_ind_fnl
	,'MM' as prov_prtcp_sts_cd
	,'MM' as proc_cd
	,0 as policy_diag_flag
--	,'MM' as primary_diag_cd
	,'MM' as cgm_code_type
	,'MM' as supply_v_reciever
	,'0' as 
	,'MM' as status_med
	,'MM' as status_pharm
	,'MM' as status_total
	,0 as diabetes_flag
	,0 as insulin_flag
	,0 as fast_acting_insulin
	,0 as insulin_months
	,0 as hypoglycemia
	,fin_ma_mapd
	,cohort 
	,CASE 
        WHEN FLOOR(AGE) < 65 THEN '0-64'
        WHEN FLOOR(AGE) = 65 THEN '65-69'        
        WHEN FLOOR(AGE) BETWEEN 65 and 69 THEN '65-69'
        WHEN FLOOR(AGE) BETWEEN 70 and 74 THEN '70-74'
        WHEN FLOOR(AGE) BETWEEN 75 and 79 THEN '75-79'
        WHEN FLOOR(AGE) BETWEEN 80 and 84 THEN '80-84'
        WHEN FLOOR(AGE) BETWEEN 85 and 89 THEN '85-89'
        WHEN FLOOR(AGE) >= 90 THEN '90+'
        ELSE 'OTHER' END AS AGE_RANGE 
--	,case when fin_race_cd is null then 'Unknown'
--		when fin_race_cd = '0' then 'Unknown'
--		when fin_race_cd = '1' then 'White'
--		when fin_race_cd = '2' then 'African American'
--		when fin_race_cd = '3' then 'Other'
--		when fin_race_cd = '4' then 'Asian'
--		when fin_race_cd = '5' then 'Hispanic'
--		when fin_race_cd = '6' then 'N. American Native'
--		else 'Unknown' end as race 
--	,fin_gender
	,0 as units
	,0 as allowed
	,0 as netpaid
	,0 as adj_srvc_unit
	,sum(fin_member_cnt) as mm 
	,0 as members
from 
	tadm_tre_cpy.gl_rstd_gpsgalnce_f_202507
WHERE		
	TFM_INCLUDE_FLAG = '1'
	and GLOBAL_CAP = 'NA'	
	and FIN_PRODUCT_LEVEL_3 <> 'INSTITUTIONAL'	
	and fin_inc_year in ('2022','2023','2024','2025')
	and SGR_SOURCE_NAME = 'COSMOS'
	and FIN_BRand = 'M&R'
GROUP BY 
	fin_inc_year 
	,fin_inc_month 
	,FIN_PLAN_LEVEL_1 
	,FIN_PLAN_LEVEL_2 
	,FIN_PRODUCT_LEVEL_1 
	,FIN_PRODUCT_LEVEL_2 
	,FIN_PRODUCT_LEVEL_3 
	,fin_tfm_product_new 
	,fin_market 
	,FIN_G_I 
	,fin_ma_mapd
	,cohort 
	,CASE 
        WHEN FLOOR(AGE) < 65 THEN '0-64'
        WHEN FLOOR(AGE) = 65 THEN '65-69'        
        WHEN FLOOR(AGE) BETWEEN 65 and 69 THEN '65-69'
        WHEN FLOOR(AGE) BETWEEN 70 and 74 THEN '70-74'
        WHEN FLOOR(AGE) BETWEEN 75 and 79 THEN '75-79'
        WHEN FLOOR(AGE) BETWEEN 80 and 84 THEN '80-84'
        WHEN FLOOR(AGE) BETWEEN 85 and 89 THEN '85-89'
        WHEN FLOOR(AGE) >= 90 THEN '90+'
        ELSE 'OTHER' END 
--	,case when fin_race_cd is null then 'Unknown'
--		when fin_race_cd = '0' then 'Unknown'
---		when fin_race_cd = '1' then 'White'
--		when fin_race_cd = '2' then 'African American'
--		when fin_race_cd = '3' then 'Other'
--		when fin_race_cd = '4' then 'Asian'
--		when fin_race_cd = '5' then 'Hispanic'
--		when fin_race_cd = '6' then 'N. American Native'
--		else 'Unknown' end 
--	,fin_gender
;
--548204
select count (*) from tmp_1m.kn_CGM_MM ;





--Unique member counts by quarter total 
drop table tmp_1m.kn_CGM_qtrmbrs;
CREATE TABLE tmp_1m.kn_CGM_qtrmbrs stored as orc as 
SELECT
	gal_mbi_hicn_fnl
	,fst_srvc_year 
	,case when fst_srvc_month in ('201901','201902','201903') then '2019Q1'
		when fst_srvc_month in ('201904','201905','201906') then '2019Q2'
		when fst_srvc_month in ('201907','201908','201909') then '2019Q3'
		when fst_srvc_month in ('201910','201911','201912') then '2019Q4'
		when fst_srvc_month in ('202001','202002','202003') then '2020Q1'
		when fst_srvc_month in ('202004','202005','202006') then '2020Q2'
		when fst_srvc_month in ('202007','202008','202009') then '2020Q3'
		when fst_srvc_month in ('202010','202011','202012') then '2020Q4'
		when fst_srvc_month in ('202101','202102','202103') then '2021Q1'
		when fst_srvc_month in ('202104','202105','202106') then '2021Q2'
		when fst_srvc_month in ('202107','202108','202109') then '2021Q3'
		when fst_srvc_month in ('202110','202111','202112') then '2021Q4'
		when fst_srvc_month in ('202201','202202','202203') then '2022Q1'
		when fst_srvc_month in ('202204','202205','202206') then '2022Q2'
		when fst_srvc_month in ('202207','202208','202209') then '2022Q3'
		when fst_srvc_month in ('202210','202211','202212') then '2022Q4'
		when fst_srvc_month in ('202301','202302','202303') then '2023Q1'
		when fst_srvc_month in ('202304','202305','202306') then '2023Q2'
		when fst_srvc_month in ('202307','202308','202309') then '2023Q3'
		when fst_srvc_month in ('202310','202311','202312') then '2023Q4'
		when fst_srvc_month in ('202401','202402','202403') then '2024Q1'
		when fst_srvc_month in ('202404','202405','202406') then '2024Q2'
		when fst_srvc_month in ('202407','202408','202409') then '2024Q3'
		when fst_srvc_month in ('202410','202411','202412') then '2024Q4'
		when fst_srvc_month in ('202501','202502','202503') then '2025Q1'
		when fst_srvc_month in ('202504','202505','202506') then '2025Q2'
		when fst_srvc_month in ('202507','202508','202509') then '2025Q3'
		when fst_srvc_month in ('202510','202511','202512') then '2025Q4'
		else '2018' end as fst_srvc_qtr
	from  tmp_1m.kn_cgm_8
	;


--For unique utilizer metric in row 66
select count(distinct gal_mbi_hicn_fnl), fst_srvc_qtr from tmp_1m.kn_CGM_qtrmbrs group by fst_srvc_qtr ;

--For unique utilizer metric YTD in row 66
select count(distinct gal_mbi_hicn_fnl), fst_srvc_year from tmp_1m.kn_CGM_qtrmbrs where fst_srvc_qtr in ('2024Q1','2025Q1') group by fst_srvc_year ;


--Unique member counts by quarter med vs pharm 
drop table tmp_1m.kn_CGM_qtrmbrs_split;
CREATE TABLE tmp_1m.kn_CGM_qtrmbrs_split stored as orc as 
SELECT
	gal_mbi_hicn_fnl
	,cgm_source
	,fst_srvc_year 
	,case when fst_srvc_month in ('201901','201902','201903') then '2019Q1'
		when fst_srvc_month in ('201904','201905','201906') then '2019Q2'
		when fst_srvc_month in ('201907','201908','201909') then '2019Q3'
		when fst_srvc_month in ('201910','201911','201912') then '2019Q4'
		when fst_srvc_month in ('202001','202002','202003') then '2020Q1'
		when fst_srvc_month in ('202004','202005','202006') then '2020Q2'
		when fst_srvc_month in ('202007','202008','202009') then '2020Q3'
		when fst_srvc_month in ('202010','202011','202012') then '2020Q4'
		when fst_srvc_month in ('202101','202102','202103') then '2021Q1'
		when fst_srvc_month in ('202104','202105','202106') then '2021Q2'
		when fst_srvc_month in ('202107','202108','202109') then '2021Q3'
		when fst_srvc_month in ('202110','202111','202112') then '2021Q4'
		when fst_srvc_month in ('202201','202202','202203') then '2022Q1'
		when fst_srvc_month in ('202204','202205','202206') then '2022Q2'
		when fst_srvc_month in ('202207','202208','202209') then '2022Q3'
		when fst_srvc_month in ('202210','202211','202212') then '2022Q4'
		when fst_srvc_month in ('202301','202302','202303') then '2023Q1'
		when fst_srvc_month in ('202304','202305','202306') then '2023Q2'
		when fst_srvc_month in ('202307','202308','202309') then '2023Q3'
		when fst_srvc_month in ('202310','202311','202312') then '2023Q4'
		when fst_srvc_month in ('202401','202402','202403') then '2024Q1'
		when fst_srvc_month in ('202404','202405','202406') then '2024Q2'
		when fst_srvc_month in ('202407','202408','202409') then '2024Q3'
		when fst_srvc_month in ('202410','202411','202412') then '2024Q4'
		when fst_srvc_month in ('202501','202502','202503') then '2025Q1'
		when fst_srvc_month in ('202504','202505','202506') then '2025Q2'
		when fst_srvc_month in ('202507','202508','202509') then '2025Q3'
		when fst_srvc_month in ('202510','202511','202512') then '2025Q4'
		else '2018' end as fst_srvc_qtr
	from  tmp_1m.kn_cgm_8
	;

--For unique utilizer metric in row 67&68
select count(distinct gal_mbi_hicn_fnl), cgm_source, fst_srvc_qtr from tmp_1m.kn_CGM_qtrmbrs_split group by cgm_source, fst_srvc_qtr ;

--For unique utilizer metric in row 67&68 YTD version 
select count(distinct gal_mbi_hicn_fnl), cgm_source, fst_srvc_year from tmp_1m.kn_CGM_qtrmbrs_split where fst_srvc_qtr in ('2024Q1','2025Q1','2024Q2','2025Q2') group by cgm_source, fst_srvc_year ;

--For DME PA Analysis 
drop table tmp_1m.kn_CGM_qtrmbrs_split_dmepa;
CREATE TABLE tmp_1m.kn_CGM_qtrmbrs_split_dmepa stored as orc as 
SELECT
	gal_mbi_hicn_fnl
	,prov_prtcp_sts_cd
	,group_ind_fnl 
	,cgm_source 
	,case when fst_srvc_month in ('201901','201902','201903') then '2019Q1'
		when fst_srvc_month in ('201904','201905','201906') then '2019Q2'
		when fst_srvc_month in ('201907','201908','201909') then '2019Q3'
		when fst_srvc_month in ('201910','201911','201912') then '2019Q4'
		when fst_srvc_month in ('202001','202002','202003') then '2020Q1'
		when fst_srvc_month in ('202004','202005','202006') then '2020Q2'
		when fst_srvc_month in ('202007','202008','202009') then '2020Q3'
		when fst_srvc_month in ('202010','202011','202012') then '2020Q4'
		when fst_srvc_month in ('202101','202102','202103') then '2021Q1'
		when fst_srvc_month in ('202104','202105','202106') then '2021Q2'
		when fst_srvc_month in ('202107','202108','202109') then '2021Q3'
		when fst_srvc_month in ('202110','202111','202112') then '2021Q4'
		when fst_srvc_month in ('202201','202202','202203') then '2022Q1'
		when fst_srvc_month in ('202204','202205','202206') then '2022Q2'
		when fst_srvc_month in ('202207','202208','202209') then '2022Q3'
		when fst_srvc_month in ('202210','202211','202212') then '2022Q4'
		when fst_srvc_month in ('202301','202302','202303') then '2023Q1'
		when fst_srvc_month in ('202304','202305','202306') then '2023Q2'
		when fst_srvc_month in ('202307','202308','202309') then '2023Q3'
		when fst_srvc_month in ('202310','202311','202312') then '2023Q4'
		when fst_srvc_month in ('202401','202402','202403') then '2024Q1'
		when fst_srvc_month in ('202404','202405','202406') then '2024Q2'
		when fst_srvc_month in ('202407','202408','202409') then '2024Q3'
		when fst_srvc_month in ('202410','202411','202412') then '2024Q4'
		when fst_srvc_month in ('202501','202502','202503') then '2025Q1'
		when fst_srvc_month in ('202504','202505','202506') then '2025Q2'
		when fst_srvc_month in ('202507','202508','202509') then '2025Q3'
		when fst_srvc_month in ('202510','202511','202512') then '2025Q4'
		else '2018' end as fst_srvc_qtr
		,tadm_hcta_util 
	from  tmp_1m.kn_cgm_8
	;

--For unique utilizer metric in DME PA Analysis 
select count(distinct gal_mbi_hicn_fnl) as unique_utilizers, sum(tadm_hcta_util) as units, fst_srvc_qtr, prov_prtcp_sts_cd , group_ind_fnl  from tmp_1m.kn_CGM_qtrmbrs_split_dmepa where fst_srvc_qtr='2023Q1' group by fst_srvc_qtr, prov_prtcp_sts_cd , group_ind_fnl  ;


	

--getting diabetes flag start and end months per member
drop table tmp_1m.kn_CGM_mems3a;
CREATE TABLE tmp_1m.kn_CGM_mems3a stored as orc as 
SELECT
	mbi 
	,min(year_mo) as diab_startmo
	,max(year_mo) as diab_endmo
from tmp_1m.kn_diabetes_2
group by mbi; 

--joining onto CGM list 
drop table tmp_1m.kn_CGM_mems3;
CREATE TABLE tmp_1m.kn_CGM_mems3 stored as orc as 
SELECT
	a.*
	,b.diab_startmo as diabetic_start
	,b.diab_endmo as diabetic_end 
from tmp_1m.kn_cgm_mems2 as  a 
left join tmp_1m.kn_cgm_mems3a as b 
on a.gal_mbi_hicn_fnl=b.mbi; 

--getting insulin start and end months per member 
drop table tmp_1m.kn_CGM_mems4a;
CREATE TABLE tmp_1m.kn_CGM_mems4a stored as orc as 
SELECT
	mbi 
	,min(month) as insulin_startmo
	,max(month) as insulin_endmo
from tmp_1m.kn_cgm_insulin_4
group by mbi; 

drop table tmp_1m.kn_CGM_mems4b;
CREATE TABLE tmp_1m.kn_CGM_mems4b stored as orc as 
SELECT
	mbi 
	,min(month) as fainsulin_startmo
	,max(month) as fainsluin_endmo
from tmp_1m.kn_cgm_insulin_5
group by mbi; 


--joining onto CGM list 
drop table tmp_1m.kn_CGM_mems4;
CREATE TABLE tmp_1m.kn_CGM_mems4 stored as orc as 
SELECT
	a.*
	,b.insulin_startmo as insulin_start
	,b.insulin_endmo as insulin_end 
	,c.fainsulin_startmo as fast_act_in_start
	,c.fainsluin_endmo as fast_act_in_end
from tmp_1m.kn_cgm_mems3 as  a 
left join tmp_1m.kn_cgm_mems4a as b 
on a.gal_mbi_hicn_fnl=b.mbi
left join tmp_1m.kn_CGM_mems4b as c 
on a.gal_mbi_hicn_fnl=c.mbi; 

--finding CGM start and end dates
drop table tmp_1m.kn_CGM_mems5a;
CREATE TABLE tmp_1m.kn_CGM_mems5a stored as orc as 
SELECT
	gal_mbi_hicn_fnl 
	,min(fst_srvc_month) as cgm_start
	,max(fst_srvc_month) as cgm_end 
from tmp_1m.kn_cgm_8
group by gal_mbi_hicn_fnl ; 

drop table tmp_1m.kn_CGM_mems5;
CREATE TABLE tmp_1m.kn_CGM_mems5 stored as orc as 
SELECT
	a.*
	,b.cgm_start
	,b.cgm_end
from tmp_1m.kn_CGM_mems4 as a
left join tmp_1m.kn_cgm_mems5a as b 
on a.gal_mbi_hicn_fnl=b.gal_mbi_hicn_fnl
; 
	
drop table tmp_1m.kn_CGM_mems6a;
CREATE TABLE tmp_1m.kn_CGM_mems6a stored as orc as 
SELECT DISTINCT 
	gal_mbi_hicn_fnl
	,fst_srvc_month
	,fst_srvc_year 
from tmp_1m.kn_cgm_5 ;

drop table tmp_1m.kn_CGM_mems6;
CREATE TABLE tmp_1m.kn_CGM_mems6 stored as orc as 
SELECT
	a.* 
	,case when a.fst_srvc_month between b.diabetic_start and b.diabetic_end then 1 else 0 end as diabetes_flag
	,case when a.fst_srvc_month between b.insulin_start and b.insulin_end then 1 else 0 end as insulin_flag
	,case when a.fst_srvc_month between b.fast_act_in_start and b.fast_act_in_end then 1 else 0 end as fast_acting_insulin
	,c.fin_ma_mapd
	,c.fin_market 
	,c.fin_g_i 
	,c.fin_plan_level_1 
	,c.fin_plan_level_2 
	,c.fin_product_level_1 
	,c.fin_product_level_2 
	,c.fin_product_level_3
	,c.fin_tfm_product_new 
	,c.tadm_cohort 
from tmp_1m.kn_cgm_mems6a as a 
left join tmp_1m.kn_cgm_mems5 as b 
on a.gal_mbi_hicn_fnl=b.gal_mbi_hicn_fnl
left join fichsrv.tre_membership as c 
on a.gal_mbi_hicn_fnl=c.fin_mbi_hicn_fnl 
and a.fst_srvc_month =c.fin_inc_month

;

--distincting both joins 
drop table tmp_1m.kn_CGM_mems6b;
CREATE TABLE tmp_1m.kn_CGM_mems6b stored as orc as 
SELECT DISTINCT 
	*
from tmp_1m.kn_CGM_mems6;


--joining on status variables (doing it seperately cause bdpaas told me the query was too large in total and refused to run it....)
drop table tmp_1m.kn_CGM_mems6c;
CREATE TABLE tmp_1m.kn_CGM_mems6c stored as orc as 
SELECT
	a.*
	,b.status_med
from tmp_1m.kn_cgm_mems_firststart3 as b
left join  tmp_1m.kn_cgm_mems6b as a 
on a.gal_mbi_hicn_fnl =b.gal_mbi_hicn_fnl 
and a.fst_srvc_month =b.fst_srvc_month
;

drop table tmp_1m.kn_CGM_mems6d;
CREATE TABLE tmp_1m.kn_CGM_mems6d stored as orc as 
SELECT
	a.*
	,b.status_pharm
from tmp_1m.kn_cgm_mems6c as a 
left join tmp_1m.kn_cgm_mems_firststart3 as b 
on a.gal_mbi_hicn_fnl =b.gal_mbi_hicn_fnl 
and a.fst_srvc_month =b.fst_srvc_month
;

drop table tmp_1m.kn_CGM_mems6e;
CREATE TABLE tmp_1m.kn_CGM_mems6e stored as orc as 
SELECT
	a.*
	,b.status_total
from tmp_1m.kn_cgm_mems6d as a 
left join tmp_1m.kn_cgm_mems_firststart3 as b 
on a.gal_mbi_hicn_fnl =b.gal_mbi_hicn_fnl 
and a.fst_srvc_month =b.fst_srvc_month
;

--distincting joins 
drop table tmp_1m.kn_CGM_mems6f;
CREATE TABLE tmp_1m.kn_CGM_mems6f stored as orc as 
SELECT DISTINCT 
	*
from tmp_1m.kn_CGM_mems6e;

drop table tmp_1m.kn_CGM_mems7;
CREATE TABLE tmp_1m.kn_CGM_mems7 stored as orc as 
SELECT
	a.* 
	,b.cgm_source 
	,c.hypoglycemia
from tmp_1m.kn_cgm_mems6f as a 
left join tmp_1m.kn_cgm_5 as b 
on a.gal_mbi_hicn_fnl=b.gal_mbi_hicn_fnl
and a.fst_srvc_month=b.fst_srvc_month
left join tmp_1m.kn_cgm_hypo_3 as c 
on a.gal_mbi_hicn_fnl =c.gal_mbi_hicn_fnl 
and a.fst_srvc_month=c.fst_srvc_month 
; 

drop table tmp_1m.kn_CGM_mems7a;
CREATE TABLE tmp_1m.kn_CGM_mems7a stored as orc as 
SELECT DISTINCT 
	*
from tmp_1m.kn_CGM_mems7;


--Unique member counts by month and year 
drop table tmp_1m.kn_CGM_monthlymbrs;
CREATE TABLE tmp_1m.kn_CGM_monthlymbrs stored as orc as 
SELECT
	'Monthly Members' as component
	,cgm_source
	,'Monthly Members' as hce_service_code
	,fst_srvc_year
	,fst_srvc_month
	,fin_plan_level_1 as plan_level_1_fnl
	,fin_plan_level_2 as plan_level_2_fnl
	,fin_product_level_1 as product_level_1_fnl
	,fin_product_level_2 as product_level_2_fnl
	,fin_product_level_3 as product_level_3_fnl
	,fin_tfm_product_new as fin_tfm_product_new_fnl
	,fin_market as market_fnl
	,fin_g_i as group_ind_fnl
	,'Members' as prov_prtcp_sts_cd
	,'Members' as proc_cd
	,0 as policy_diag_flag
	,'Members' as cgm_code_type
	,'Members' as supply_v_reciever
	,'0' as 
	,status_med
	,status_pharm
	,status_total
	,diabetes_flag
	,insulin_flag
	,fast_acting_insulin
	,0 as insulin_months
	,hypoglycemia
	,fin_ma_mapd
	,tadm_cohort
	,'Members' as age_range
--	,'Members' as race
--	,'Members' as fin_gender
	,0 as units
	,0 as allowed
	,0 as netpaid
	,0 as adj_srvc_unit
	,0 as mm
	,count(distinct gal_mbi_hicn_fnl) as monthly_members
from tmp_1m.kn_CGM_mems7a
where fst_srvc_year in ('2022','2023','2024','2025')
group by 

	fst_srvc_year
	,cgm_source
	,fst_srvc_month
	,fin_market
	,fin_g_i
	,fin_ma_mapd
	,tadm_cohort
	,fin_plan_level_1 
	,fin_plan_level_2 
	,fin_product_level_1 
	,fin_product_level_2 
	,fin_product_level_3 
	,fin_tfm_product_new
	,status_med
	,status_pharm
	,status_total
	,diabetes_flag
	,insulin_flag
	,fast_acting_insulin
		,hypoglycemia
	;

--Unique member counts by month and year 
drop table tmp_1m.kn_CGM_yearlymbrs;
CREATE TABLE tmp_1m.kn_CGM_yearlymbrs stored as orc as 
SELECT
	'Yearly Members' as component
	,cgm_source
	,'Yearly Members' as hce_service_code
	,fst_srvc_year
	,'NA' as fst_srvc_month
	,fin_plan_level_1 as plan_level_1_fnl
	,fin_plan_level_2 as plan_level_2_fnl
	,fin_product_level_1 as product_level_1_fnl
	,fin_product_level_2 as product_level_2_fnl
	,fin_product_level_3 as product_level_3_fnl
	,fin_tfm_product_new as tfm_product_new_fnl
	,fin_market as market_fnl
	,fin_g_i as group_ind_fnl
	,'Members' as prov_prtcp_sts_cd
	,'Members' as proc_cd
	,0 as policy_diag_flag
	,'Members' as cgm_code_type
	,'Members' as supply_v_reciever
	,'0' as 
	,status_med
	,status_pharm
	,status_total
	,diabetes_flag
	,insulin_flag
	,fast_acting_insulin
	,0 as insulin_months
	,hypoglycemia
	,fin_ma_mapd
	,tadm_cohort
	,'Members' as age_range
--	,'Members' as race
--	,'Members' as fin_gender
	,0 as units
	,0 as allowed
	,0 as netpaid
	,0 as adj_srvc_unit
	,0 as mm
	,count(distinct gal_mbi_hicn_fnl) as monthly_members
from tmp_1m.kn_CGM_mems7
where fst_srvc_year in ('2022','2023','2024','2025')
group by 

	fst_srvc_year
	,cgm_source
	,fin_market
	,fin_g_i
	,fin_ma_mapd
	,tadm_cohort
	,fin_plan_level_1 
	,fin_plan_level_2 
	,fin_product_level_1 
	,fin_product_level_2 
	,fin_product_level_3 
	,fin_tfm_product_new
	,status_med
	,status_pharm
	,status_total
	,diabetes_flag
	,insulin_flag
	,fast_acting_insulin 
		,hypoglycemia
	;

select sum(monthly_members) ,fst_srvc_year from tmp_1m.kn_cgm_yearlymbrs group by fst_srvc_year ;
select count(distinct gal_mbi_hicn_fnl), fst_srvc_year from tmp_1m.kn_cgm_8 group by fst_srvc_year ;

--union of claims and MM and members for export 
drop table tmp_1m.kn_CGM_FNL;
CREATE TABLE tmp_1m.kn_CGM_FNL stored as orc as 
SELECT
	* from tmp_1m.kn_cgm_9 
	where fst_srvc_year in ('2023','2024','2025')
	union all select 
	* from tmp_1m.kn_cgm_mm
	union all select 
	* from tmp_1m.kn_CGM_monthlymbrs
	where fst_srvc_year in ('2023','2024','2025')
	union all select 
	* from tmp_1m.kn_cgm_yearlymbrs 
	where fst_srvc_year in ('2023','2024','2025')
; 
--3341494
select count(*) from tmp_1m.kn_cgm_fnl;

