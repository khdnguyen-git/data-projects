%create_user_credentials_file;
%let warehouse = VING_PRD_MNR_HCE_DATAINFRA_WH;
%let role = AZu_SDRP_ving_prd_developer_role;
%let schema = FICHSRV;
%let database = VING_PRD_TREND_DB;
%put &server;
%let source=snfl_zzz;
%get_system_credentials(source="&source.", envir="&env.");

/* These 4 variables will not change, once the rsa key is set up for the id.*/
%LET SERVER=uhgdwaas.east-us-2.azure.snowflakecomputing.com;
%LET UID = &userid;
%LET PRIV_KEY_FILE_UNENCR = &spwd;
%LET PRIV_KEY_FILE_PWD = ;

%put &spwd;

libname asnow2 sasiosnf
server = "&SERVER"
role = "&role"
warehouse = "&warehouse"
database = "&database"
schema = "TMP_1M"
bulkload = yes
bl_internal_stage = user
conopts = "
AUTHENTICATOR = SNOWFLAKE_JWT;
UID = khang.nguyen@uhc.com;
PRIV_KEY_FILE = {&PRIV_KEY_FILE_UNENCR};
PRIV_KEY_FILE_PWD = ;
ODBC_USE_STANDARD_TIMESTAMP_COLUMNSIZE = TRUE;
readbuff = 32767
insertbuff = 32767
dbcommit = 0
"
;


proc contents data = asnow2.KN_IP_DATASET_LOC_05132026_OD;
run;

libname x "/hpsasfin/int/nas/fin360/phi2/hcx/COMMON/KN";


data x.LOC_IP_5_13_26_OD (compress = yes); 
set asnow2.KN_IP_DATASET_LOC_05132026_OD;
run;

proc contents data = x.LOC_IP_5_13_26_OD;
run;

proc sql;
select 
	admit_act_month
	, sum(membership)
from x.LOC_IP_5_13_26_OD
where mnr_total_ffs_flag = 1 and admit_act_month >= '202501'
group by admit_act_month
order by admit_act_month
;
quit;

