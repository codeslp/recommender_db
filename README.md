## README - Project Overview

This project comprises three core parts aimed at simplifying database interactions and offering an analysis framework.

### Part 1: Analyst Schema and Environment

Location: `src/da_rawschema`

This part is dedicated to analysts who need access to the raw data. It facilitates both ad hoc queries and structured analyses through Python notebooks.

1. **Load Raw Tables**: `load_raw_csvs_1.ipynb` - This notebook is responsible for ingesting raw tables and populating them into a non-relational schema.
2. **User Credentials Creation**: `create_DA_creds_2.ipynb` - Utilize this notebook to generate a user with read privileges to the raw tables.
3. **Analyst Notebook**: `analyst_env_3.ipynb` - An environment for analysts to run read queries against the raw schema in the recommender database.

### Part 2: Relational Schema and Local Database API

Location: `src/ds_relational_schema` and `src/db_api`

The relational schema is built and managed through:

1. **Relational Schema Creation**: `relational_schema_1.ipynb` - This notebook creates tables in the relational schema within the recommender database.
2. **DS User Credential Creation**: `create_DS_creds_2.ipynb` - It's designed to create a user with full control over the relational schema.

Following the schema creation, the local database API allows interaction with the relational schema:

1. **Local Database API**: `db_local_api.py` and `local_query.py` - This Python API permits read and write queries.
2. **Demonstration Recommender**: `demo_local_recommender.py` - A simple recommender system to illustrate the functionality of the local API.

### Part 3: Web API

Location: `src/ds_api/ds_web_api`

1. **FastAPI Web API**: `ds_web_api` - This is a FastAPI powered API offering read, write, and delete endpoints (update functionality coming soon) that interact with the database through JSON payloads.
2. **Data Models**: `models.py` - Validates both incoming requests and responses using SQLModel to ensure integrity.

### Testing Suite

Location: `src/ds_api/tests`

This testing suite, while still under development, provides foundational tests for:

1. **DB Connection**: `conftest.py`
2. **Local Python API**: `test_db_local_api.py`
3. **Web API**: `test_db_web_api.py`

### Additional Resources

1. **Database ERD**: Find an image representation of our latest database Entity-Relationship Diagram (ERD) in the `artifacts` directory.
2. **Documentation**: Refer to `Doc/plan.ipynb` for initial plans, decision-making notes, and checklists to grasp our development trajectory.
