# STEDI Lakehouse

## Table of content

1. [Project Description](#1-project-description)
2. [How to run project](#2-how-to-run-project)
3. [File Structure](#3-file-structure)

## 1. Project Description

The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

trains the user to do a STEDI balance exercise;
and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
has a companion mobile app that collects customer data and interacts with the device sensors.

In this project, we will extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

### Project Environment
You'll use the data from the STEDI Step Trainer and mobile app to develop a lakehouse solution in the cloud that curates the data for the machine learning model using:

* Python and Spark
* AWS Glue
* AWS Athena
* AWS S3

### Project Data
STEDI has three JSON data sources to use from the Step Trainer. Check out the JSON data in the following folders in the Github repo linked above:
* customer
* step_trainer
* accelerometer

## 2. How to run project

Step 1: Configuring AWS Glue: S3 VPC Gateway Endpoint

Step 2: Creating the Glue Service Role

Step 3: Cloning data from github to local AWS

```
git clone https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises.git
```

Step 4: Copy data from local AWS to S3
```
aws s3 cp ./customer-keep-1655293787679.json s3://<bucket-name>/customer/landing/

aws s3 sync . s3://<bucket-name>/accelerometer/landing/

aws s3 sync . s3://<bucket-name>/step-trainer/landing/

```
Step 5: Create the Glue Table by DDL in landing zone 

Step 6: Run ETL code to transform data from landing zone to trusted zone

Step 7: Run ETL code to transform data from trusted zone to curated zone

> Note: please change the bucket name in code as yours.

## 3. File Structure
```
-- ddl: contain ddl to create the GLue Tables
---- customer_landing.sql 
---- accelerometer_landing.sql

-- etl: contain .py file to transform data from landing zone to trusted  zone, curated zone
---- accelerometer_landing_to_trusted.py
---- customer_landing_to_trusted.py
---- customer_trusted_to_curated.py
---- step_trainer_landing_to_trusted.py
---- step_trainer_trusted_to_ml_curated.py

-- screenshot: screenshots of Athena query of tables: 
---- customer_landing.jpg
---- accelerometer_landing.jpg
---- customer_trusted.jpg

-- README.md
```




































































