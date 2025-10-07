# Fantasy-Football
**Work in progress:** _don't sue me_

Goal is to process NFL data into a Postgres SQL server. ETL/ELT is then performed to create a mart layer that will serve a dashboard or two (Power BI or Tableau, currently undecided right now).
Might insert dataiku step for some data to do some predictive analytics and serve dashboards using a `reporting` schema.

## Software Stack
### Ingest Data
All the actual NFL data - primarily play-by-play and player information. Eventually may add additional tables not available via NFL Verse, which will be listed separatly.
- Data Source:
  - [NFL Verse](https://github.com/nflverse/nflverse-data)
- R Packages:
  - [nflreader](https://github.com/nflverse/nflreadr/)
  - [nflfastR](https://github.com/nflverse/nflfastR/)
### ETL/ELT
Modification of the raw data from NFL Verse. `stg_` ingests data, `int_` is an intermediate step, and `rep_` is for the eventual reporting out of data. While still building, only a select number of years are being used.
- Database: PostgreSQL
- SQL syntax: [dbt](https://www.getdbt.com/)
### Reporting
- PowerBI or Tableau? Something else? Looking to see what is the cheapest to maintain. Ideal solution meets the following criteria:
  - Is Free (or at least a free option exists e.g. Tableau Public)
  - Can be self-hosted (personal prefence)
  - Is commonly used by professionals (less important)
  - Git integration (ideal, not a deal breaker)
  - *Currently looking into Grafana*
### ML Models
- *Dataiku (I'm still not solidified on this yet. I have an instance up and running, a couple of models built based on data from 2021 - 2024, but nothing is used downstream or has appeared dramatically insightfull yet.)*

### Sporadic Notes
- League scoring can be adjusted by adding your league scoring to the [seed_league_scoring](dbt/seeds/seed_league_scoring.csv) seed and modifying the variable active_league in [dbt_project.yml](dbt/dbt_project.yml)
    - When calculating fantasy points scored at a play-by-play level, fractional points scoring is always assumed as true, as fractional scoring matters only at the end of the game, and not on the individual play-by-play level
- Currently the `raw_` schema has multiple tables for each NFL season that are then later unioned together into a consolidated model. A new R script has enabled me to do an initial load to build one large consolidated play-by-play and players table, however this hasn't been updated within dbt yet.
