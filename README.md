# Data Warehouse on Redshift Project

A music streaming startup, Sparkify, has grown their user base and song database and wants to move their processes 
and data onto the cloud. Their data resides in S3, in a directory of JSON logs of user activity on the app, as well as in a directory with JSON metadata on the songs in the app.

The data warehouse project builds an ETL pipeline that extracts Sparkify's data from S3, stages them in Redshift, 
and transforms data into a set of dimensional tables for the analytics team to continue finding insights in 
what songs Sparkify users are listening to. 


## Files in the project:

1. create_tables.py: The script that creates the Redshift staging and analytical tables.
2. dwh.cfg: Contains Redshift config settings.
3. etl.py: Loads staging tables from log files and inserts into analytical tables from the staging tables.
4. sql_queries.py: The module that contains code for creating and inserting into Redshift database tables.


## Database schema
The data warehouse follows a star schema design because it is simple, efficient and reduces the number of
joins required to execute business query.

| Table | Description |
| ---- | ---- |
| tbl_staging_events | Stores the data extracted from event log files |
| tbl_staging_songs | Saves the data extracted from song log files |
| tbl_songplay | Fact table for song playing events | 
| tbl_user | Dimension table for app users | 
| tbl_song | Dimension table for songs played in app | 
| tbl_artists | Dimension table for song artists | 
| tbl_time | Dimension table for timestamps | 

### Instructions:

Run the following commands in the project's root directory to set up the database.

    - Create staging and analytical tables by running the code below
        `python create_tables.py`
    - To load data from log files and populate staging table. As well as transferring data from staging 
	  tables to analytical tables. Run the command:
        `python etl.py`