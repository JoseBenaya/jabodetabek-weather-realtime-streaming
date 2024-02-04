from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import to_timestamp,to_date
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import split,from_json,col,to_json
from pyspark.sql.types import StructType,StructField,StringType,DoubleType,IntegerType,DataType,TimestampType
import json
from dotenv import load_dotenv
import os
from pathlib import Path


dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

def main():
    spark = SparkSession \
        .builder \
        .appName("Weather data") \
        .config("spark.sql.shuffle.partitions", 4) \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.postgresql:postgresql:42.3.5") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('error')

    messages = spark \
        .readStream \
        .format(source="kafka") \
        .option("kafka.bootstrap.servers", f"{kafka_host}:9092") \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", 'latest') \
        .load() \
        .selectExpr('CAST(value as string)') \

    schema=StructType() \
            .add("creation_time",TimestampType(),True) \
            .add('country',StringType(),True) \
            .add('temp_diff',DoubleType(),True) \
            .add('city_name',StringType(),True) \
            .add('temperature',DoubleType(),True) \
            .add('humidity',DoubleType(),True) \
            .add('temp_min',DoubleType(),True) \
            .add('temp_max',DoubleType(),True) \
            .add('feel_like',DoubleType(),True) \
            .add('wind_speed',DoubleType(),True) \

    schema_json=messages.select(from_json(col('value'),schema).alias("json"))
    schema_json = schema_json.selectExpr("json.*") \
    .selectExpr('creation_time as ts','city_name','temperature','temp_min','temp_max','feel_like','wind_speed','humidity') \
    .withWatermark('ts','5 minutes')

    schema_json.printSchema()
    schema_json.createOrReplaceTempView("details")

    result = spark.sql("""
      SELECT   city_name,
                window.start AS start_timestamp,
                window.end AS end_timestamp,
                COUNT(*) AS num_observations_in_window,
                MIN(temp_min) AS min_temperature_in_window,
                MAX(temp_max) AS max_temperature_in_window,
                (MAX(temp_max)-MIN(temp_min)) as temp_difference,
                AVG(temperature) AS avg_temperature_in_window,
                humidity,
                (max(humidity)-min(humidity)) as humidity_difference,
                wind_speed,
                (max(wind_speed)-min(wind_speed)) as wind_speed_difference
                
      FROM      details AS t
      GROUP BY city_name,wind_speed,humidity, WINDOW(t.ts , '10 Second')
      """);

    query =result \
        .writeStream \
        .outputMode('append') \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 5) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    db_host = 'ep-dark-morning-a165t2n5.ap-southeast-1.aws.neon.tech'
    db_name = 'mydb'
    db_url = f"jdbc:postgresql://{db_host}/{db_name}"
    db_driver = "org.postgresql.Driver"
    db_username = "josebenaya"
    db_password = "5IjZWNCnH6Al"

    def write_batch_to_db(batch_dataframe: DataFrame, batch_id):
        print(f"writing {batch_dataframe.count()} rows to {db_url}")
        batch_dataframe \
            .write \
            .format("jdbc") \
            .mode("append") \
            .option("url", db_url) \
            .option("driver", db_driver) \
            .option("user", db_username) \
            .option("password", db_password) \
            .option("dbtable", "weather_data") \
            .option("truncate", False) \
            .save()

    result.writeStream \
        .foreachBatch(write_batch_to_db) \
        .outputMode("append") \
        .start() \
        .awaitTermination()

    query.awaitTermination(20000)

if __name__ == '__main__':
    main()