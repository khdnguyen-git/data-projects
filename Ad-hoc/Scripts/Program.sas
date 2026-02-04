%create_user_credentials_file;



%let warehouse = VING_PRD_MNR_HCE_DATAINFRA_WH;
%let role = AZu_SDRP_ving_prd_developer_role;
%let schema = FICHSRV;
%let database = VING_PRD_TREND_DB;


%put &server;

libname x "/hpsasfin/int/users/knguy139/data";


%let source=snfl_zzz;
%get_system_credentials(source="&source.", envir="&env.");

/* These 4 variables will not change, once the rsa key is set up for the id.*/
%LET SERVER=uhgdwaas.east-us-2.azure.snowflakecomputing.com;
%LET UID = &userid;
%LET PRIV_KEY_FILE_UNENCR = &spwd;
%LET PRIV_KEY_FILE_PWD = ;

%put &spwd;


/* These may/can change, depending on the warehouse, database, schema, and role:*/
%let warehouse=VING_PRD_MNR_HCE_DATAINFRA_WH;
%let role=AZu_SDRP_ving_prd_developer_role;
%let schema=FICHSRV;
%let database=VING_PRD_TREND_DB;


proc contents data = asnow2.TRE_MEMBERSHIP;
run;


libname asnow2 sasiosnf
server="&SERVER"
role="&role"
warehouse="&warehouse"
database="&database"
schema="&schema"
bulkload=yes
bl_internal_stage=user
conopts="
AUTHENTICATOR=SNOWFLAKE_JWT;
UID=khang.nguyen@uhc.com;
PRIV_KEY_FILE={&PRIV_KEY_FILE_UNENCR};
PRIV_KEY_FILE_PWD=;
ODBC_USE_STANDARD_TIMESTAMP_COLUMNSIZE=TRUE;
readbuff=32767
insertbuff=32767
dbcommit=0
";


proc sql;
select count(distinct FIN_MBI_HICN_FNL) from asnow2.TRE_MEMBERSHIP
where FIN_INC_MONTH = '202501'
;
run;

proc print data =  asnow2.cosmo_op (obs = 10);
run;



proc sql;
    select 1 as test from asnow2.INFORMATION_SCHEMA.TABLES;
quit;



proc contents data=asnow2.TRE_MEMBERSHIP ;
run;
