import os
import configparser
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F
from zipfile import ZipFile
import time



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']






def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def unzipRawdata(data_folder, unzipped_data):
    """
    unzip the raw data 
    and return the unzip data to the pointed folder
    """
    with ZipFile(data_folder, 'r') as zip:
        zip.extractall(unzipped_data)
    
    return unzipped_data

        
def process_song_data(spark, input_data, output_data):
    
    song_schema = T.StructType([
        T.StructField("artist_id", T.StringType()),
        T.StructField("artist_latitude", T.DoubleType()),
        T.StructField("artist_location", T.StringType()),
        T.StructField("artist_longitude", T.StringType()),
        T.StructField("artist_name", T.StringType()),
        T.StructField("duration", T.DoubleType()),
        T.StructField("num_songs", T.IntegerType()),
        T.StructField("title", T.StringType()),
        T.StructField("year", T.IntegerType()),
    ])
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data, song_schema)

    
    
    # extract columns to create songs table
    song_fields = ["title", "artist_id", "year", "duration"]
    songs_table = df.select(song_fields).dropDuplicates().withColumn("song_id", F.monotonically_increasing_id())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs")

    
    
    # extract columns to create artists table
    artists_fields = ["artist_id", 
                      "artist_name as name", 
                      "artist_location as location", 
                      "artist_latitude as latitude",
                      "artist_longitude as longitude"]
    artists_table = df.selectExpr(artists_fields).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists')


    
    
def process_log_data(spark, input_data, output_data):
    
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*.json')

    # read log data file and filter by actions for song plays
    log_df = spark.read.json(log_data)
    log_df = log_df.filter(log_df.page == 'NextSong')

    # extract columns for users table    
    users_fields = ["userId as user_id", 
                     "firstName as first_name", 
                     "lastName as last_name", 
                     "gender", 
                     "level"]
    users_table = log_df.selectExpr(users_fields).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x: x / 1000, T.TimestampType())
    log_df = log_df.withColumn("timestamp", get_timestamp(log_df.ts))

    # create datetime column from original timestamp column
    get_datetime = F.udf(lambda x: datetime.fromtimestamp(x), T.TimestampType())
    log_df = log_df.withColumn("start_time", get_datetime(log_df.timestamp))
    
    # enrich data for time table
    log_df = log_df.withColumn("hour", F.hour("start_time")) \
                   .withColumn("day", F.dayofmonth("start_time")) \
                   .withColumn("week", F.weekofyear("start_time")) \
                   .withColumn("month", F.month("start_time")) \
                   .withColumn("year", F.year("start_time")) \
                   .withColumn("weekday", F.dayofweek("start_time"))
    
    # extract columns to create time table
    time_table = log_df.select("start_time", "hour", "day", "week", "month", "year", "weekday", "ts")

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time")

    
    # filter time columns from log_df to advoid conflict with time column in the final step 
    # and optimize join performance in next join step with other DataFrame
    log_df = log_df.select(F.col('artist'), F.col('auth'), F.col('firstName'),
                       F.col('gender'), F.col('itemInSession'), F.col('lastName'),
                       F.col('length'), F.col('level'), F.col('location'),
                       F.col('method'), F.col('page'), F.col('registration'),
                       F.col('sessionId'), F.col('song'), F.col('status'),
                       F.col('userAgent'),F.col('userId'), F.col('ts')
                      ) \
                      .dropDuplicates()
    
    
    # read in song data to use for songplays table
    songs_df = spark.read.parquet(os.path.join(output_data, "songs/*/*/*"))

    # join song_df with log_df to create songs_logs table
    songs_logs = log_df.join(songs_df, log_df.song == songs_df.title, 'left') \
                       .drop(songs_df.title)
    
    # read artist table and choose only need columns to enhance performance
    artists_df = spark.read.parquet(os.path.join(output_data, "artists"))
    artists_df = artists_df.select(F.col('artist_id'),
                  F.col('name'),
                  F.col('latitude'),
                  F.col('longitude')
                 )
    
    # create artists_songs_logs from songs_logs table and artists_df table
    artists_songs_logs = songs_logs.join(artists_df, songs_logs.artist == artists_df.name, 'left') \
                                   .drop(artists_df.name)

    # create songplays from artists_songs_logs and time_table
    songplays = artists_songs_logs.join(
        time_table,
        artists_songs_logs.ts == time_table.ts,
        'left'
        ) \
        .drop(artists_songs_logs.ts)
    
    
    # extract columns from songplays table to create songplays table 
    songplays_table = songplays.select(
            F.col('start_time'),
            F.col('userId').alias('user_id'),
            F.col('level'),
            F.col('song_id'),
            F.col('artist_id'),
            F.col('sessionId').alias('session_id'),
            F.col('userAgent').alias('user_agent'),
            F.col('year'),
            F.col('month'),
            F.col('location')
            ) \
            .repartition("year", "month") \
            .dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    #input_data = "./data/"
    output_data = "s3://sparkifi-output/"
    #output_data = "./output-data/"
    
    zipped_song_data = os.path.join(input_data, 'song-data.zip')
    zipped_log_data = os.path.join(input_data, 'log-data.zip') # OK
    unzipRawdata(zipped_song_data, input_data) # + 'song_data'
    unzipRawdata(zipped_log_data, input_data + 'log_data')   # + 'log_data'
    
    time.sleep(10)  # wait for data folder to be unzip

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
