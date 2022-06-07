# Project: Data Lake On AWS


## Description
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
To do: Build pipeline that reach to AWS input zip files, read json files, stage them on Redshift and ETL create data model for the topic music app data.


## Project Datasets
Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data
Log data json path: s3://udacity-dend/log_json_path.json

song data and log data are zip files which contain JSON files.

Example of JSON files in song data input path:
`song_data/A/B/C/TRABCEI128F424C983.json`
`song_data/A/A/B/TRAABJL12903CDCF1A.json`


## connect to Hadoop from local machine:
1. create IAM role
2. create  pairkey and download it to local machine
3. create EMR version above 5.25 (to be able to see SparkUI after ETL)
4. fix EMR inbound security group to accept SSH (when the EMR status turned to waiting)
5. use PuTTy with pairkey to connect to ERM by SSH (setup tunnel to allow forward ...)
6. open sparkUI from the EMR web interface


## make S3 bucket
input data S3 bucket: `aws s3 mb s3://udacity-dend --profile production`
output data S3 bucket: `aws s3 mb s3://sparkifi-output --profile production`


## copy input to input data bucket
copy input song-data.zip to s3://udacity-dend: 
`aws s3 cp <your local song-data.zip file> s3://sparkifi-output`
copy input log-data.zip to s3://udacity-dend: 
`aws s3 cp <your local log-data.zip file> s3://sparkifi-output`
copy input etl.py to s3://udacity-dend: 
`aws s3 cp <your local etl.py file> s3://sparkifi-output`


## etl the input data
copy etl file to Hadoop: `aws s3 cp s3://udacity-dend/lower_songs.py .`
submit to Spark: `spark-submit lower_songs.py`
