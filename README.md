# Data Modeling with PostgreSQL
### Project overview
A fictional startup **Sparkify** in the music streaming industry looks to provide a great service to the community that 
enjoys its music streaming app. Sparkify holds a collection of event data and metadata (logs in JSON format) output 
from the activity of its users. As data engineers we have been asked to perform the necessary data wrangling and 
analytics in order to support data teams (scientists and analysts) needs, discovering the insights (i.e. behaviours, 
interactions) between users and the music.
For this job we have adopted Postgres as the database platform, built a data model under the star schema (dimensions and 
fact tables), and built an ETL pipeline to extract-transform-load data from the local files to a **sparkifydb** (Postgres) 
database.

![Logo](https://github.com/abreufreire/sparkify-postgres/blob/master/graphics/logo.png)


### Database schema
![Schema](https://github.com/abreufreire/sparkify-postgres/blob/master/graphics/erd_sparkifydb.png)


### Project files
```
sparkify-postgres
|  .gitignore                   # Config file for Git
|  README.md                    # Repository description
|  analytics.ipynb              # Sample queries to run on sparkifydb
|  create_tables.py             # Creates sparkifydb tables (drops content if exists)
|  etl.py                       # Runs ETL pipeline to process data
|  requirements.txt             # Contains libraries needed to run scripts
|  sql_queries.py               # Queries to build sparkifydb tables
|  test_etl.ipynb               # Tests pipeline operations
|  test_sql_queries.ipynb       # Tests queries on sparkifydb
|  transaction_postgres.py      # Defines connection & methods to sparkifydb
|
└--graphics
  |  docker_hub.png             # Postgres image reference in Docker Hub
  |  erd_sparkifydb.png         # Sparkifydb star schema (Postgres)
  |  logo.png                   # Sparkify logo
```


### How to run
Clone this project, and to up and running it **locally** you can use Docker, because in its repository/hub there is an 
image with Postgres installed and the Sparkify raw data to be explored 
(image ref: **postgres-student-image**, thanks to user *onekenken*). For the sake of this project the data and 
metadata under munging is part of the Million Song Dataset<sup>1</sup>.

![Dockerhub](https://github.com/abreufreire/sparkify-postgres/blob/master/graphics/docker_hub.png)


- Install Docker 
- Login Docker Hub 
- Pull image: postgres-student-image
- Run container: postgres-student-image
- On terminal run:
```
python3 create_tables.py

python3 etl.py
```
- Stop container (& Remove container)


### Data & metadata source 
<sup>1</sup>[Million Song Dataset](http://millionsongdataset.com/)
