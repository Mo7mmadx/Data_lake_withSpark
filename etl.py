import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['keys']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['keys']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark



def process_song_data(spark, input_data, output_data):
    """
        To read song data from S3 then generate songs and artist tables then write tables to parquet files S3 again 
        
            spark = refers to Spark Session
            input_data  = the path of song_data 
            output_data = the path of storing data after processing 
            
    """

    song_data = input_data + 'song_data/*/*/*/*.json'
    
    df = spark.read.json(song_data)
    
    df.createOrReplaceTempView("song_data_table")
    songs_table = spark.sql("""
                            SELECT st.song_id, 
                            st.title,
                            st.artist_id,
                            st.year,
                            st.duration
                            FROM song_data_table st
                            WHERE song_id IS NOT NULL
                        """)
    
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs_table/')
        
        
    artists_table = spark.sql("""
                                SELECT DISTINCT at.artist_id, 
                                at.artist_name,
                                at.artist_location,
                                at.artist_latitude,
                                at.artist_longitude
                                FROM song_data_table at
                                WHERE at.artist_id IS NOT NULL
                            """)
    
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')
    

def process_log_data(spark, input_data, output_data):
    """To read log data from S3 then generate tables then write tables to parquet files S3 again 
        
            spark = refers to Spark Session
            input_data  = the path of log_data 
            output_data = the path of storing data after processing
    """

    log_path = input_data + 'log_data/*/*/*.json'
    df = spark.read.json(log_path)
    df = df.filter(df.page == 'NextSong')
    df.createOrReplaceTempView("log_data_table")
    
    users_table = spark.sql("""
                            SELECT DISTINCT logs.userId as user_id, 
                            logs.firstName as first_name,
                            logs.lastName as last_name,
                            logs.gender as gender,
                            logs.level as level
                            FROM log_data_table logs
                            WHERE logs.userId IS NOT NULL
                        """)
    
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')

    
    
    time_table = spark.sql("""
                            SELECT 
                            all.start_time_sub as start_time,
                            hour(all.start_time_sub) as hour,
                            dayofmonth(all.start_time_sub) as day,
                            weekofyear(all.start_time_sub) as week,
                            month(all.start_time_sub) as month,
                            year(all.start_time_sub) as year,
                            dayofweek(all.start_time_sub) as weekday
                            FROM
                            (SELECT to_timestamp(l.ts/1000) as start_time_sub
                            FROM log_data_table l
                            WHERE l.ts IS NOT NULL
                            ) all
                        """)
    
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time_table/')

    
    
    
    song_df = spark.read.parquet(output_data+'songs_table/')
    
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                to_timestamp(lt.ts/1000) as start_time,
                                month(to_timestamp(lt.ts/1000)) as month,
                                year(to_timestamp(lt.ts/1000)) as year,
                                lt.userId as user_id,
                                lt.level as level,
                                st.song_id as song_id,
                                st.artist_id as artist_id,
                                lt.sessionId as session_id,
                                lt.location as location,
                                lt.userAgent as user_agent
                                FROM log_data_table lt
                                JOIN song_data_table st
                                on lt.artist = st.artist_name and lt.song = st.title
                            """)

    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table/')

def main():
    spark = create_spark_session()
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://aws-emr-resources-362377671657-us-east-2/output/"

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
