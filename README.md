## README - Project Overview

### Introduction

This project utilizes the Kaggle Netflix movies and shows dataset, which can be found [here](https://www.kaggle.com/datasets/thedevastator/the-ultimate-netflix-tv-shows-and-movies-dataset). In our environment, we have transformed this data into a relational schema. This schema not only embodies the original dataset but also fabricates additional tables and data related to users, user ratings, viewing sessions, and recommendations. The primary intent behind this project is dual-faceted: 
1. To provide analysts with access to raw data for analytical pursuits.
2. To facilitate data scientists in their endeavors, especially while simulating or developing machine learning recommender systems.

### Tools Used

- **Database**: PostgreSQL
- **API Framework**: FastAPI
- **ORM**: SQLModel
- **Analysis and Development**: Python/Python Notebooks

### Getting Started

1. **Dependencies Installation**: 
   - Navigate to the `artifacts` folder and locate the `requirements.txt` file.
   - Install the necessary Python packages by running: 
     ```
     pip install -r artifacts/requirements.txt
     ```

2. **Setting up the Environment**:
   - If you're keen on accessing the system either as an analyst or a data scientist, ensure you create and configure your postgre db first, and then put your admin credentials in a .env file in your project directory. 
   - Run the files in the order they are listed below.
   - Analysts have the flexibility to interact using the provided notebook or tools like pgAdmin.
   - Data scientists can harness either the local API or the web API to perform simulations, especially when conceptualizing a recommender system.

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

To run the tests, navigate to the `src/ds_api/` directory and run:

```
export PYTHONPATH=/Users/bfaris96/Desktop/turing-proj/recommender_db/src/ds_api:$PYTHONPATH
``` 

Then, run:

```
pytest
```

### Additional Resources

1. **Database ERD**: Find an image representation of our latest database Entity-Relationship Diagram (ERD) in the `artifacts` directory.
2. **Documentation**: Refer to `Doc/plan.ipynb` for initial plans, decision-making notes, and checklists to grasp our development trajectory.
