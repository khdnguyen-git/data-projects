install.packages(c("DBI", "dplyr","dbplyr","odbc"))

library(DBI)
library(dplyr)
library(dbplyr)
library(odbc)

sf_conn <- DBI::dbConnect(odbc::odbc()
                          , dsn = "UHG.UHGDWAAS"
                          , uid = "khang.nguyen@uhc.com"
                          , pwd = "Infinity3914!!!!")

avtar24_25 <- DBI::dbGetQuery(sf_conn,
    "select * from hce_ops_fnl.hce_adr_avtar_like_24_25_f
    limit 100"
)


