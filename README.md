# CalData MDSA training practice repo

This is the dbt project for CalData MDSA training exercises here: https://cagov.github.io/caldata-mdsa-training/

## Prerequisites

- Python 3.10 or higher
- [uv](https://docs.astral.sh/uv/) installed
- Snowflake account access
- Git

!!! Note
    Your team may already be using a different package to manage Python virtual environments like [pixi](https://pixi.prefix.dev/latest/) or [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html). Although this guide uses `uv`, other package managers will work with modifications.

## Setup

1. **Clone this repository**

2. **Install dependencies**
   ```bash
   cd transform
   uv sync
   ```

3. **Follow the steps from the training**

Follow the learning path here: https://github.com/cagov/caldata-mdsa-training/learning-path/
