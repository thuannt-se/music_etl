# MUSIC ETL Project
## General info
A simple source for ETL coding in python (Project from Udacity)
## Technologies
Project is created with:
* python
* postgre database
## Setup
* Install all requirement library for source (For pandas: at least **version 0.24.0**)
* Run create_tables.py to create database and tables.
* Run etl.py to perform extract data and insert into tables.
## Result
After successful executing create_tables.py and etl.py, it should create 5 tables: songplays, times, users, songs, artists and storing record extracted from dataset.

*Note: There's only one record that has artist_id and song_id in songplays table*