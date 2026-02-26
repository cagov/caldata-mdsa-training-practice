# CalData MDSA training practice repo

This is the dbt project for CalData MDSA training exercises using California water quality data.

## Quick start

### Prerequisites

- Python 3.10 or higher
- [uv](https://docs.astral.sh/uv/) installed
- Snowflake account access
- Git

!!! Note
    Your team may already be using a different package to manage Python virtual environments like [pixi](https://pixi.prefix.dev/latest/) or [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html). Although this guide uses `uv`, other package managers will work with modifications.

### Setup

1. **Clone this repository**

2. **Install dependencies**
   ```bash
   cd transform
   uv sync
   ```

3. **Configure dbt profiles**

   Create `~/.dbt/profiles.yml` with your Snowflake credentials:

   ```yaml
   caldata_mdsa_training:
     target: dev
     outputs:
       dev:
         type: snowflake
         account: <org_name>-<account_name>
         user: <your-username>
         password: <your-password>  # omit if using externalbrowser
         authenticator: externalbrowser  # or username_password_mfa
         role: TRANSFORMER_DEV
         database: TRANSFORM_DEV
         warehouse: TRANSFORMING_XS_DEV
         schema: DBT_<first-initial-lastname>  # e.g., DBT_JDOE
         threads: 4
   ```

4. **Test your setup**
   ```bash
   uv run dbt debug
   ```

   You should see "All checks passed!" at the end.

## Usage

### Running dbt

```bash
# Run all models
uv run dbt run

# Run a specific model
uv run dbt run --select stg_water_quality__stations

# Test your models
uv run dbt test

# Build (run + test)
uv run dbt build

# Generate and serve documentation
uv run dbt docs generate
uv run dbt docs serve
```

### Loading data with Python

**Note:** This is only required for at least one trainee to load the raw data. Most trainees can skip this section and work with data that's already been loaded.

#### Setup for Python script

Before running the Python script, you need to set environment variables for Snowflake authentication. Add these to your shell config (`~/.zshrc`, `~/.bashrc`, or `~/.bash_profile`):

```bash
export SNOWFLAKE_ACCOUNT=<org_name>-<account_name>
export SNOWFLAKE_USER=<your-username>
export SNOWFLAKE_PASSWORDE=<your-password> #not needed if SNOWFLAKE_AUTHENTICATOR=externalbrowser
export SNOWFLAKE_AUTHENTICATOR=externalbrowser  # or username_password_mfa
export SNOWFLAKE_DATABASE=RAW_DEV
export SNOWFLAKE_WAREHOUSE=<warehouse-name>
export SNOWFLAKE_ROLE=LOADER_DEV
```

Open a new terminal or run `source ~/.zshrc` (or your shell config file) to apply the changes.

#### Running the script

To load water quality lab results data into Snowflake:

```bash
uv run python python/load_water_quality_data.py
```

This script:
- Downloads lab results CSV from data.ca.gov
- Cleans column names for Snowflake compatibility
- Creates a table in `RAW_DEV.WATER_QUALITY.LAB_RESULTS_TEST_2026`
- Loads the data using PUT/COPY commands

## Project structure

```
transform/
├── dbt_project.yml          # dbt project configuration
├── packages.yml             # dbt package dependencies
├── models/                  # dbt data models
│   ├── _sources.yml        # Source definitions
│   ├── staging/            # Staging layer models
│   │   └── training/
│   └── intermediate/       # Intermediate layer models
│       └── training/
├── macros/                  # Custom dbt macros
└── python/                  # Python scripts
    └── load_water_quality_data.py
```

## Learn more

For detailed training materials, see the [CalData MDSA Training documentation](https://github.com/cagov/caldata-mdsa-training).

Topics covered:
- dbt fundamentals and best practices
- Staging, intermediate, and mart model layers
- Testing and documentation
- Materializations and performance optimization
- Version control with Git and GitHub

## Troubleshooting

**dbt debug fails:**
- Verify `~/.dbt/profiles.yml` profile name matches `dbt_project.yml` (should be `caldata_mdsa_training`)
- Check Snowflake credentials and permissions
- Ensure warehouse is running

**Python script fails:**
- Verify environment variables are set: `env`
- Check Snowflake role has permissions to create tables in `RAW_DEV`
- Ensure you have the `LOADER_DEV` role

## License

MIT
