# Install required packages
if (!requireNamespace('config', quietly = TRUE)){
  install.packages('config')
}

if (!requireNamespace('DBI', quietly = TRUE)){
  install.packages('DBI')
}

if (!requireNamespace('RODBC', quietly = TRUE)){
  install.packages('RODBC')
}

if (!requireNamespace('odbc', quietly = TRUE)){
  install.packages('odbc')
}

if (!requireNamespace('dplyr', quietly = TRUE)){
  install.packages('dplyr')
}

if (!requireNamespace('dbplyr', quietly = TRUE)){
  install.packages('dbplyr')
}

if (!requireNamespace('pacman', quietly = TRUE)){
  install.packages('pacman')
}

if (!requireNamespace('tictoc', quietly = TRUE)){
  install.packages('tictoc')
}

if (!requireNamespace('nflfastR', quietly = TRUE)) {
  install.packages('nflfastR')
}

# Import required packages
library(config)
library(DBI)
library(RODBC)
library(odbc)
library(dplyr)
library(dbplyr)
library(nflfastR)

# load data

tictoc::tic()
progressr::with_progress({
  try({ # to avoid CRAN test problems
    nfl_full_pbp <- nflfastR::load_pbp(1999:2025)
  })
})
tictoc::toc()

glue::glue("{nrow(nfl_full_pbp)} rows of nfl play-by-play data from {length(unique(nfl_full_pbp$game_id))} games.")

dplyr::glimpse(nfl_full_pbp)

# Add date/time to identify when data was last refreshed
nfl_full_pbp$etl_loaded_at <- Sys.time()

# Configure and test connection to Postgres
dw <- config::get("postgres")

con_test <- dbCanConnect(RPostgres::Postgres(),
                         dbname = dw$database,
                         host = dw$server,
                         port = dw$port,
                         user = dw$uid,
                         password = dw$pwd)

# Print test results
con_test

# Connect to Database
con <- dbConnect(RPostgres::Postgres(),
                 host=dw$server,
                 port=dw$port,
                 dbname=dw$database,
                 user=dw$uid,
                 password=dw$pwd)

dbCreateTable(con, SQL('"raw"."play_by_play_full"'), nfl_full_pbp)

dbWriteTable(con, name = Id(catalog = "raw", table = "play_by_play_full"), nfl_full_pbp)
