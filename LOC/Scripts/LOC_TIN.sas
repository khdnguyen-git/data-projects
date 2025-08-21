
libname tmp_1m hadoop server='rp000062286'  CONFIG='/sas/hadoop/bdpaas_prod_5_2/fin360/cfg-site.xml' port=10009 
SCHEMA = tmp_1m
user='knguy139' /*Change to your username & Password*/
PWD="Infinity3914!!!!" subprotocol=hive2
DBMAX_TEXT = 255;

proc contents data = tmp_1m.kn_ip_dataset_loc_07162025;
run;

proc export data 
;
libname kn "/hpsasfin/int/users/knguy139";

data kn.kn_ip_dataset_loc_07162025;
set tmp_1m.kn_ip_dataset_loc_07162025;
run;