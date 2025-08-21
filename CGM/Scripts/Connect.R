library(odbc)
library(DBI)

# DSN
con <- dbConnect(
  odbc::odbc()
  , "Hive_64bit"
)

#pwd = "rstudioapi::askForPassword("Database password")",

# Driver
con <- dbConnect(
  odbc::odbc(),
  driver = "Cloudera ODBC Driver for Apache Hive",
  host   = "rp000062286",
  port   = 10009,
  schema = "default",
  uid = "knguy139",
  pwd = rstudioapi::askForPassword("Database password")
)


library(tidyverse)
library(rio)
library(lubridate)
library(janitor)






cgm_auth <- import("C:\\Users\\knguy139\\Documents\\Excel\\CGM_auth.csv") %>% 
  clean_names() %>% 
  mutate(month_str = format(disposition_date, "%Y%m")) %>% 
  select(mbi, case_id, month_str)

cgm_mbi <- cgm_auth %>% 
  select(mbi) %>%
  distinct() %>%
  filter(mbi != "NULL" & !is.na(mbi))

view(
cgm_mbi %>% 
  count(mbi)
)

dbWriteTable(
  conn = con,
  name = DBI::Id(catalog = "HIVE", schema = "tmp_1m", table = "kn_pcgm_auth"),
  value = cgm_auth,
  overwrite = TRUE
)

cgm_auth %>% 


dbWriteTable(
  conn = con,
  name = DBI::Id(catalog = "HIVE", schema = "tmp_1m", table = "kn_pcgm_auth"),
  value = cgm_auth,
  overwrite = TRUE
)



view(
  cgm_auth %>% 
    summarize(n = n(), n_distinct = n_distinct(mbi))
)
# 62615 60068

mbi_feb <- cgm_auth %>% 
  filter(month_str == "202502") %>% 
  distinct(mbi) %>% 
  select(mbi)

mbi_mar <- cgm_auth %>% 
  filter(month_str == "202503") %>% 
  distinct(mbi) %>% 
  select(mbi)

mbi_apr <- cgm_auth %>% 
  filter(month_str == "202504") %>% 
  distinct(mbi) %>% 
  select(mbi)

only_mar <- mbi_mar %>% 
  anti_join(mbi_apr, by = "mbi")

only_apr <- mbi_apr %>% 
  anti_join(mbi_mar, by = "mbi")

mar_and_apr <- mbi_mar %>% 
  inner_join(mbi_apr, by = "mbi")

nrow(only_mar) # 5723
nrow(only_apr) # 12823
nrow(mar_and_apr) # 4


only_feb <- mbi_feb %>% 
  anti_join(mbi_apr, by = "mbi")

only_apr <- mbi_apr %>% 
  anti_join(mbi_feb, by = "mbi")

feb_and_apr <- mbi_feb %>% 
  inner_join(mbi_apr, by = "mbi")

nrow(only_feb) # 5047
nrow(only_apr) # 12763
nrow(feb_and_apr) # 64













mbi_month <- cgm_auth %>% 
  select(MBI, disposition_month) %>% 
  rename(mbi = MBI, mbi_month =  disposition_month) %>%
  group_by(mbi, mbi_month) %>% 
  filter(mbi != 'NULL') %>% 
  distinct() %>% 
  mutate(mbi_month_v2 = paste0(mbi_month, "_only"))

view(
  mbi_month %>% 
    distinct() %>% 
    count()
)

view(mbi_month)

view(
  cgm_auth %>% 
    select(MBI, disposition_month) %>% 
    rename(mbi = MBI, mbi_month =  disposition_month) %>% 
    group_by(mbi) %>% 
    filter(mbi != 'NULL') %>% 
    count()
)


