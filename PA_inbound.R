# PA Migration Data Cleaning Script
# 
# Analyzes IRS Migration data from 2017-2018 and cleans for visualization
# 5/17/20 Amanda Kmetz
# 
# Part 1: Imports data, filters to PA results, cleans column names, formats county FIPS, and computes IRS totals
# Part 2: Connects to database, writes DF into new table, removes invalid numbers, pivots table,
#         saves pivoted results to new table, removes null values, checks for totaling errors, and writes to DF
# Part 3: Imports shapefile, joins shp to DF, cleans columns, computes percentages, and writes to file


rm(list=ls()) #clears environment
cat("\014")  #clears console

wd="C:/.../PA_Migration/"
setwd(wd)


library(tidyverse)
library(sf)
library(DBI)


# -------------------
# -------------------  17-18 DATA
# -------------------


# ------------------- PART 1: Initial cleanup
# packages used: readr, dplyr (tidyverse)

# read csv - county inflow 17-18
county_inbound_1718 <-read_csv("countyinflow1718.csv", col_types = cols(
  y2_statefips = col_integer(),
  y2_countyfips = col_integer(),
  y1_statefips = col_integer(),
  y1_countyfips = col_integer(),
  y1_state = col_character(),
  y1_countyname = col_character(),
  n1 = col_integer(),
  n2 = col_integer(),
  agi = col_integer()
))

# filter to PA counties only + rename columns
pa_inbound_1718 <- county_inbound_1718 %>%
  filter(y2_statefips == 42) %>%
  select(dest_st = y2_statefips,
         dest_cty = y2_countyfips,
         origin_st = y1_statefips,
         origin_cty = y1_countyfips,
         origin_st_name = y1_state,
         origin_cty_name = y1_countyname,
         returns = n1,
         exemptions = n2,
         agi)

# pad with zeros
pa_inbound_1718$dest_cty <- formatC(pa_inbound_1718$dest_cty, width=3, format="d", flag="0") 
pa_inbound_1718$origin_st <- formatC(pa_inbound_1718$origin_st, width=2, format="d", flag="0") 
pa_inbound_1718$origin_cty <- formatC(pa_inbound_1718$origin_cty, width=3, format="d", flag="0") 

# add irs_pop column to use for pivot
pa_inbound_1718 <- pa_inbound_1718 %>%
  mutate(irs_pop = returns + exemptions)

# write to csv
# write_csv(pa_inbound_1718, "pa_inbound_1718.csv")

# ------------------- PART 2: SQLite
# packages used: DBI

# connect to DB
pa_mig_db <- dbConnect(RSQLite::SQLite(), "pa_migration.db")

# write dataframe to new table
dbWriteTable(pa_mig_db, "pa_inbound_1718", pa_inbound_1718)

# preview table entries
dbGetQuery(pa_mig_db, 'SELECT * FROM pa_inbound_1718 LIMIT 100')

# set irs_pop to 0 where it is set as the -1 flag
dbExecute(pa_mig_db, 'UPDATE pa_inbound_1718 
                      SET irs_pop = 0 
                      WHERE irs_pop < 0')

# test new column extraction 
# non-migrants, 67 results
dbGetQuery(pa_mig_db, 'SELECT dest_cty, origin_st, origin_cty, origin_cty_name, irs_pop as non_mig 
                      FROM pa_inbound_1718
                      WHERE origin_cty_name LIKE "%Non-migrants%"')

# total migrants, 68 results
dbGetQuery(pa_mig_db, 'SELECT dest_cty, origin_st, origin_cty, origin_cty_name, irs_pop as tot_mig
                      FROM pa_inbound_1718
                      WHERE origin_cty_name LIKE "%Total Migration-US and Foreign%"')

# same state totals, 68 results
dbGetQuery(pa_mig_db, 'SELECT dest_cty, origin_st, origin_cty, origin_cty_name, irs_pop as ss_mig
                      FROM pa_inbound_1718
                      WHERE origin_cty_name LIKE "%Total Migration-Same State%"')

# different state totals, 68 results
dbGetQuery(pa_mig_db, 'SELECT dest_cty, origin_st, origin_cty, origin_cty_name, irs_pop as ds_mig
                      FROM pa_inbound_1718
                      WHERE origin_cty_name LIKE "%Total Migration-Different State%"')

# foreign totals, 61 results
dbGetQuery(pa_mig_db, 'SELECT dest_cty, origin_st, origin_cty, origin_cty_name, irs_pop as for_mig
                      FROM pa_inbound_1718
                      WHERE origin_cty_name LIKE "%Total Migration-Foreign%"')


# preview pivoted results
dbGetQuery(pa_mig_db, 'SELECT a.dest_cty, (non_mig + tot_mig) as tot_irs, 
                      non_mig,  tot_mig, ss_mig, ds_mig, for_mig
                      FROM(
                         SELECT dest_cty, origin_st, origin_cty, origin_cty_name, irs_pop as non_mig
                         FROM pa_inbound_1718
                         WHERE origin_cty_name LIKE "%Non-migrants%") as a
                          LEFT JOIN(
                                  SELECT dest_cty, origin_st, origin_cty, origin_cty_name, irs_pop as tot_mig 
                                  FROM pa_inbound_1718
                                  WHERE origin_cty_name LIKE "%Total Migration-US and Foreign%") as b
                          ON a.dest_cty = b.dest_cty
                          LEFT JOIN(
                                  SELECT dest_cty, origin_st, origin_cty, origin_cty_name, irs_pop as ss_mig 
                                  FROM pa_inbound_1718
                                  WHERE origin_cty_name LIKE "%Total Migration-Same State%") as c
                          ON a.dest_cty = c.dest_cty
                          LEFT JOIN(
                                  SELECT dest_cty, origin_st, origin_cty, origin_cty_name, irs_pop as ds_mig
                                  FROM pa_inbound_1718
                                  WHERE origin_cty_name LIKE "%Total Migration-Different State%") as d
                          ON a.dest_cty = d.dest_cty
                          LEFT JOIN(
                                  SELECT dest_cty, origin_st, origin_cty, origin_cty_name, irs_pop as for_mig
                                  FROM pa_inbound_1718
                                  WHERE origin_cty_name LIKE "%Total Migration-Foreign%") as e
                          ON a.dest_cty = e.dest_cty;')

# write pivoted results to new table
dbExecute(pa_mig_db, 'CREATE TABLE pa_inbound_1718_wide AS
                      SELECT a.dest_cty, (non_mig + tot_mig) as tot_irs, 
                      non_mig,  tot_mig, ss_mig, ds_mig, for_mig
                      FROM(
                         SELECT dest_cty, origin_st, origin_cty, origin_cty_name, irs_pop as non_mig
                         FROM pa_inbound_1718
                         WHERE origin_cty_name LIKE "%Non-migrants%") as a
                          LEFT JOIN(
                                  SELECT dest_cty, origin_st, origin_cty, origin_cty_name, irs_pop as tot_mig 
                                  FROM pa_inbound_1718
                                  WHERE origin_cty_name LIKE "%Total Migration-US and Foreign%") as b
                          ON a.dest_cty = b.dest_cty
                          LEFT JOIN(
                                  SELECT dest_cty, origin_st, origin_cty, origin_cty_name, irs_pop as ss_mig 
                                  FROM pa_inbound_1718
                                  WHERE origin_cty_name LIKE "%Total Migration-Same State%") as c
                          ON a.dest_cty = c.dest_cty
                          LEFT JOIN(
                                  SELECT dest_cty, origin_st, origin_cty, origin_cty_name, irs_pop as ds_mig
                                  FROM pa_inbound_1718
                                  WHERE origin_cty_name LIKE "%Total Migration-Different State%") as d
                          ON a.dest_cty = d.dest_cty
                          LEFT JOIN(
                                  SELECT dest_cty, origin_st, origin_cty, origin_cty_name, irs_pop as for_mig
                                  FROM pa_inbound_1718
                                  WHERE origin_cty_name LIKE "%Total Migration-Foreign%") as e
                          ON a.dest_cty = e.dest_cty;')

# preview new pivoted table
dbGetQuery(pa_mig_db, 'SELECT * FROM pa_inbound_1718_wide LIMIT 100')

# set NA to 0
dbExecute(pa_mig_db, 'UPDATE pa_inbound_1718_wide
                      SET for_mig = 0
                      WHERE for_mig is Null')

# add alternative total + flag columns to check for known totaling errors
dbExecute(pa_mig_db, 'ALTER TABLE pa_inbound_1718_wide
                      ADD COLUMN tot_mig_alt INTEGER;')

dbExecute(pa_mig_db, 'ALTER TABLE pa_inbound_1718_wide
                      ADD COLUMN tot_mig_flag INTEGER;')

# update alternative total + flag columns
dbExecute(pa_mig_db, 'UPDATE pa_inbound_1718_wide 
                      SET tot_mig_alt = (ss_mig + ds_mig + for_mig);')

dbExecute(pa_mig_db, 'UPDATE pa_inbound_1718_wide
                      SET tot_mig_flag = (tot_mig_alt - tot_mig);')

# preview final table
dbGetQuery(pa_mig_db, 'SELECT * FROM pa_inbound_1718_wide LIMIT 100')


# retrieve table as DF
df_sql_pivot_wide <- as.data.frame(dbGetQuery(pa_mig_db, 'SELECT * FROM pa_inbound_1718_wide'))

# disconnect from db
dbDisconnect(pa_mig_db)

# ------------------- PART 3: Join, Compute Percentages, and Export
# packages used: sf, dplyr

# read shapefile
pa_counties <- st_read("PaCounty2020/PaCounty2020_01.shp")

# join to shapefile
pa_inbound_1718_join <- inner_join(pa_counties, df_sql_pivot_wide, by=c("FIPS_COUNT" = "dest_cty"), copy=FALSE,suffix = c(".county", ".irs"))                                         

# clean up columns - 
pa_inbound_1718_join <- pa_inbound_1718_join %>%
  select(cty_num = COUNTY_NUM,
         cty_name = COUNTY_NAM,
         cty_fips = FIPS_COUNT,
         area_sqmi = AREA_SQ_MI,
         tot_irs, non_mig, tot_mig,
         ss_mig, ds_mig, for_mig, 
         tot_mig_alt, tot_mig_flag) %>%
  mutate(pct_mig = (tot_mig_alt/tot_irs),
         pct_non = (non_mig/tot_irs),
         pct_ds_pop = (ds_mig/tot_irs),
         pct_ss_pop = (ss_mig/tot_irs),
         pct_ds_mig = (ds_mig/tot_mig_alt),
         pct_ss_mig = (ss_mig/tot_mig_alt))
  
st_write(pa_inbound_1718_join, "pa_mig_in_1718.shp")

