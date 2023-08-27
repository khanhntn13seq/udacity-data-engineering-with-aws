# Sparkify Data Warehouse

## Table of Content
1. [Project Description](#1-project-description)
2. [Data warehouse schema](#2-data-warehouse-schema)
3. [How to run project](#3-how-to-run-project)
4. [File Structure](#4-file-structure)

## 1. Project Description
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

![Alt text](images/sparkify-s3-to-redshift-etl.png)


### Database:
There are 3 datasets that reside in S3. Here are the S3 links for each:

* Song data: `s3://udacity-dend/song_data`
* Log data: `s3://udacity-dend/log_data`
* Metdata file `s3://udacity-dend/log_json_path.json` contains the meta information that is required by AWS to correctly load `s3://udacity-dend/log_data`

#### 1. Song dataset
It is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset.

    song_data/A/B/C/TRABCEI128F424C983.json
    song_data/A/A/B/TRAABJL12903CDCF1A.json

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

    {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

#### 2. Log Datase
The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are file paths to two files in this dataset.

    log_data/2018/11/2018-11-12-events.json
    log_data/2018/11/2018-11-13-events.json

And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.

![Alt text](images/log-data.png)

#### 3. Log Json Meta Information
Log Json Meta Information

![Alt text](images/log-json-path.png)

## 2. Data warehouse schema
### Fact Table
1. **songplays** - records in event data associated with song plays i.e. records with page `NextSong`

*songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

### Dimension Tables
2. **users** - users in the app

*user_id, first_name, last_name, gender, level*

3. **songs** - songs in music database

*song_id, title, artist_id, year, duration*

4. **artists** - artists in music database

artist_id, name, location, lattitude, longitude

5. **time** - timestamps of records in songplays broken down into specific units

*start_time, hour, day, week, month, year, weekday*

## 3. How to run project
### Installations 

The below installations is for window 11:
* Python 3.8
* PostpreSQL 15.4

### How to run:

* Step 1: Launch a redshift cluster and create an IAM role that has read access to S3.


* Step 2: Add redshift database and IAM role info to `dwh.cfg`.

* Step 3: Run `create_tables.py` file
```
python create_tables.py
```

* Step 4: Run `etl.py` file
```
python etl.py
```

## 4. File Structure

The project template includes four files:

* `create_table.py` is where you'll create your fact and dimension tables for the star schema in Redshift.

* `etl.py` is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.

* `sql_queries.py` is where you'll define you SQL statements, which will be imported into the two other files above.

* `README.md` is where you'll provide discussion on your process and decisions for this ETL pipeline.