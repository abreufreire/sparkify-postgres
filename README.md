![Logo](https://github.com/abreufreire/sparkify-postgres/blob/master/graphics/logo.png)

# Data Modeling with PostgreSQL
### Project overview
A fictional startup **Sparkify** in the music streaming industry looks to provide a great service to the community that 
enjoys its music streaming app. Sparkify holds a collection of event data and metadata (logs in JSON format) output 
from the activity of its users. As data engineers we have been asked to perform the necessary data processing and 
analytics in order to support data teams (scientists and analysts) needs, discovering the insights (i.e. behaviours, 
interactions) between users and the music.
For this job we have adopted Postgres as the database platform, built a data model under the star schema (dimensions and 
fact tables), and built an ETL pipeline to extract-transform-load data from the local files to a **sparkifydb** (Postgres) 
database.


### Project files
```
sparkify-postgres
|  README.md                    # Project description
|  analytics.ipynb              # Sample queries to run on sparkifydb
|  create_tables.py             # Creates sparkifydb tables (drops content if exists)
|  etl.py                       # Runs ETL pipeline to process data
|  sql_queries.py               # Queries to build sparkifydb tables
|  test_etl.ipynb               # Tests pipeline operations
|  test_sql_queries.ipynb       # Tests queries on sparkifydb
|  transaction_postgres.py      # Defines connection & methods to sparkifydb
|  requirements.txt             # Contains libraries needed to run scripts
|
└--graphics
  |  docker_hub.png             # Postgres image reference in Docker Hub
  |  erd_sparkifydb.png         # Sparkifydb star schema (Postgres)
  |  logo.png                   # Sparkify logo
```


### How to run
Clone this project, and to up and running it **locally** you can use Docker, because in its repository/hub there is an image - 
**postgres-student-image** - with the database system installed and the raw data<sup>1</sup> in the Sparkify app 
to be explored (thanks to user *onekenken*).

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


### Database schema
![Schema](https://github.com/abreufreire/sparkify-postgres/blob/master/graphics/erd_sparkifydb.png)


### Raw data source
<sup>1</sup>[Million Song Dataset](http://millionsongdataset.com/)