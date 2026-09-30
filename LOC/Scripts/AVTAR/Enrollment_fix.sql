drop table IF EXISTS HCE_OPS_STAGE.HCEOPS_avtar_enrollment_fix;
create table HCE_OPS_STAGE.HCEOPS_avtar_enrollment_fix  as
select	fin_source_name
	   ,Migration_source
	   ,fin_product_level_3
	   ,tfm_include_flag 
	   ,global_cap 
	   ,nce_tadm_dec_risk_type 
	   ,fin_contractpbp
	   ,fin_contract_nbr
	   ,fin_pbp
	   ,fin_submarket
	   ,fin_market
	   ,fin_region
	   ,fin_state
	   ,fin_plan_level_2
	   ,fin_g_i
	   ,fin_brand
       ,gal_cust_seg_nbr
       ,"HIERARCHY"
	   ,acp_network_number
	   ,acp_network_name
	   ,fin_segment_name
       ,fin_tfm_product
	   ,fin_mbi_hicn_fnl
	   ,to_char(current_date,'YYYYMM') as fin_inc_month
	   ,fin_inc_year
	   ,gps_zip_cd
	   ,sgr_source_name
	   ,fin_tfm_product_new
	   ,FIN_PS9_BUSINESS_UNIT
	   ,FIN_PS9_LOCATION
           ,FIN_PS9_OPERATING_UNIT
           ,FIN_PS9_PRODUCT
from 
	HCE_OPS_ARCHV.GL_RSTD_GPSGALNCE_F_202606
where 
	fin_inc_month=202606;
	




--select * from HCE_OPS_STAGE.HCEOPS_avtar_enrollment_fix;
drop TABLE IF EXISTS  HCE_OPS_STAGE.HCEOPS_avtar_enrollment_fix_1;
create table HCE_OPS_STAGE.HCEOPS_avtar_enrollment_fix_1  as
select fin_source_name
	   ,Migration_source
	   ,fin_product_level_3
	   ,tfm_include_flag 
	   ,global_cap 
	   ,nce_tadm_dec_risk_type 
	   ,fin_contractpbp
	   ,fin_contract_nbr
	   ,fin_pbp
	   ,fin_submarket
	   ,fin_market
	   ,fin_region
	   ,fin_state
	   ,fin_plan_level_2
	   ,fin_g_i
	   ,fin_brand
       ,gal_cust_seg_nbr
       ,"HIERARCHY"
	   ,acp_network_number
	   ,acp_network_name
	   ,fin_segment_name
       ,fin_tfm_product
	   ,fin_mbi_hicn_fnl
	   ,fin_inc_month
	   ,fin_inc_year
	   ,gps_zip_cd
	   ,sgr_source_name
	   ,fin_tfm_product_new
	   ,FIN_PS9_BUSINESS_UNIT
	   ,FIN_PS9_LOCATION
           ,FIN_PS9_OPERATING_UNIT
           ,FIN_PS9_PRODUCT

 from HCE_OPS_ARCHV.GL_RSTD_GPSGALNCE_F_202606
union all
select *
 from HCE_OPS_STAGE.HCEOPS_avtar_enrollment_fix;



 
 