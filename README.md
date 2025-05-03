# Fantasy-Football
**Work in progress:** _don't sue me_

Goal is to process NFL data sourced via [NFL Verse](https://github.com/nflverse/nflverse-data) and stored on a Postgres SQL server. ETL/ELT done via [dbt](https://www.getdbt.com/) to create a mart layer that will serve a dashboard or two (Power BI or Tableau, currently undecided right now).
Might insert dataiku step for some data to do some predictive analytics and server dashboards using a `reporting` schema.

### Sporadic Notes
- League scoring can be adjusted by adding your league scoring to the [seed_league_scoring](dbt/seeds/seed_league_scoring.csv) seed and modifying the variable active_league in [dbt_project.yml](dbt/dbt_project.yml)
    - When calculating fantasy points scored at a play-by-play level, fractional points scoring is always assumed as true, as fractional scoring matters only at the end of the game, and not on the individual play-by-play level