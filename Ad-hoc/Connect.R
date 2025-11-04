install.packages(c("rio", "dbi", "odbc", "tidyverse"))

library(DBI)
library(odbc)
library(rio)

con <- dbConnect(odbc(), "Hive_64bit")

hf_mbi <- import("C:/Users/knguy139/Documents/Projects/Data/Input/HF_PR_mbi.txt")

dbWriteTable(
  conn = con,
  name = DBI::Id(catalog = "HIVE", schema = "tmp_1m", table = "kn_hf_mbi"),
  value = hf_mbi,
  append = TRUE
)

con_snowflake <- dbConnect(odbc(), "UHG.UHGDWAAS")

dbReadTable(
  conn = con_snowflake,
  name = DBI::Id(schema = "TMP_1M", table = "kn_hf_mbi")
)



# Group comparison
install.packages("gtsummary")
library(gtsummary)
library(stringr)
library(tidyverse)

cgm_groups <- dbReadTable(
  conn = con,
  name = Id(catalog = "HIVE", schema = "tmp_1m", table = "kn_cgm_mm_group_12")
) %>% 
  rename_with(~ str_remove(.x, ".*\\."))  


cgm_mm <- cgm_groups %>%
  group_by(across(-mbi)) %>%
  summarise(mm = n_distinct(mbi)) %>% 
  ungroup()

cgm_mm_pivot <- cgm_mm %>% 
  group_by(across(everything())) %>% 
  summarise(total_mm = sum(mm)) %>% 
  ungroup()


cgm_mm_pivot %>% 
  select(-mm) %>% 
  tbl_summary(by = groupnum,
              statistic = list(all_continuous() ~ "{sum}")) %>% 
  add_p(test = list(all_categorical() ~ "chisq.test"), test.args = list(simulate.p.value = TRUE))



group_id_cnt <- cgm_groups %>% 
  count(groupnum)

cgm_groups %>% 
  select(-mbi) %>% 
  tbl_summary(by = groupnum,
              statistic = list(all_continuous() ~ "{sum}")) %>% 
  add_p(test.args = list(simulate.p.value = TRUE))

# 
# 202501	6922	3140	45.36
# 202502	5110	2186	42.77
# 202503	5726	2634	46.00
# 202504	12826	8269	64.47
# 202505	10873	6377	58.64
# 202506	9696	5241	54.05
# 202507	10984	6112	55.64

view(
cgm_groups %>% 
  group_by(fin_contractpbp) %>% 
  count()
)

install.packages("rsconnect")

rsconnect::setAccountInfo(name='khdnguyen',
                          token='060B093DA968435ABE391BF73460FEF8',
                          secret='d7UYQ7I4tqEsa3xaz3MGtZzp5UUuJhAKgi7C+GKb')