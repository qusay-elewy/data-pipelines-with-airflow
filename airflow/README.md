# Data Pipelines with Airflow

This project creates an ETL for a music streaming compan,Sparkify, using Apache Airflow. It extracts data from JSON files that are stored on Amazon S3 and stages it on Amazon Redshift. 

Star schema is used to build our data model where songplays represents that fact table, whcih is linked to a number of dimenssion tables, namely users, time, artists, and songs.

## Structure

### DAG & Operators

This ETL has only on DAG which is composed of a number of tasks that use custom-made operators, described as follows:

* *StageToRedshiftOperator:* This operator is defined in stage_redshift.py and it is used to read data files from an S3 buckets and save it into relational database on Redshift.
* *LoadFactOperator:* This operator is defined in load_fact.py and it is used to extract fact table data from the stagging tables and load it into out fact table.
* *LoadDimensionOperator*: This operator is defined in load_dimenssion.py and it is used to extract the dimession tables data from the stagging tables and load it into the defined dimmenssion tables.
* *DataQualityOperator:* This operator is defined in data_quality.py and it is used to perform some quality checks on our Redshift data tables.


### SQL Queries

Most of the SQL queries are seperated from the Python files for clarity and reusability. The DDL queries are defined in create_tables.py, whereas the ETL queries are defined in sql_queries.py.


## Prerequisites

* User needs to create two connection variables in Apache Airflow before running this ETL. One connection variable is needed to connect to Amazon Web Services, and the other needs to connect to Redshift.
