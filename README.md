# Fantasy-Football
**Work in progress:** _don't sue me_

Goal is to process NFL data sourced via [NFL Verse](https://github.com/nflverse/nflverse-data) and stored on a Postgres SQL server. ETL/ELT done via [dbt](https://www.getdbt.com/) to create a mart layer that will serve a dashboard or two (Power BI or Tableau, currently undecided right now).
Might insert dataiku step for some data to do some predictive analytics and server dashboards using a `reporting` schema.