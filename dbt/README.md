## Fantasy Footbal | dbt
This section of the repo is focused on the data transformation of NFL data provided by [NFL Verse](https://github.com/nflverse).

### Pipeline Structure
Data is stored in a locally hosted PostgreSQL server. dbt works on top of the Postgre database `fantasyfootball` to transform data and prepare it for consumption. There will be multiple "layers" that represent logical steps in the transformation process:
- `src` (Sources): Raw data downloaded from NFL Verse. This data lives in the `raw` schema of the database. A significant amount of source testing is conducted in `dbt/models/staging/Sources.yml`
- `stg_` (Staging): The initial staging of source/raw data. Minimal transformation takes place here outside of re-ordering columns.
- `int_` (Intermediate): Transformational models that clean/blend/pivot/adjust the data. Always built off of staging or upstream intermediate models. Examples of intermediate models might be `int_games` with one record per game, or `int_player_stats_weekly` with one record per player per week with their consolidated stat line.
- `rep_` (Reporting): Final models intended to be used by analytics and visualization tools such as Power BI, Dataiku, or a web application.
- `out_` (Outbound): Non-reporting models for use by an external tool. Data may be used by a predictive analtyics tool such as Dataiku or Python where SQL is not ideal. Outbound models do not have any downstream models, however, they are only used when the data being sent out is expected to be re-ingested for additional transformation
- `inb_` (Inbound): Non-raw inbound data that has been processed by an external tool. It may be staged, but must eventually join back to the intermediate model that created it's Outbound counterpart.

### Database Structure
Within the `fantasyfootball` database, there are several schemas being utilized:
- `raw`: Raw data downloaded from NFL Verse.
- `qa`: Intermediate, Outbound, and Reporting data attached to the `qa` branch of the repository
- `analytics`: Intermediate, Outbound, and Reporting data attached to the `main` branch of the repository
- `dbt_<username>`: Dedicated developer schema for the dbt developer. Builds off the `qa` branch
- `dbt_pr_<pr_id>`: Temporary schemas used for pull request CI/CD jobs

### Branching Strategy
This project uses a multi-trunk branching stratgy. The `main` pranch is the production branch. The `qa` branch is sourced from `main` and acts as the baseline branch for all development work. Individual development branches are merged back into `qa`, and testing is executed on the `qa` branch before pushing to production, ensuring individually developed features are tested together.

### Additional dbt Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](https://getdbt.com/community) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
