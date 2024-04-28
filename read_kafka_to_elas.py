# Import necessary libraries
from pyspark.sql import SparkSession, functions as F 
from pyspark.sql.functions import col
from elasticsearch import Elasticsearch
import time
from pyspark.sql.types import LongType, IntegerType, FloatType, StringType, TimestampType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("kafka_consumer") \
    .config("spark.sql.streaming.failOnDataLoss", "false") \
    .getOrCreate()

# Set log level to ERROR
spark.sparkContext.setLogLevel('ERROR')
print("Spark version:", spark.version)

# Define Elasticsearch index settings and mappings
system_index = {
    "settings": {
        "index": {
            "number_of_replicas": 0,  
            "analysis": {
                "analyzer": {
                    "custom_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "custom_edge_ngram", "asciifolding"]
                    }
                },
                "filter": {
                    "custom_edge_ngram": {
                        "type": "edge_ngram",
                        "min_gram": 2,
                        "max_gram": 10
                    }
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "event_ts_min": {"type": "date"},
            "ts_min_bignt": {"type": "integer"},
            "room_ID": {"type": "keyword"},
            "co2": {"type": "integer"},
            "light": {"type": "float"},
            "temperature": {"type": "float"},
            "humidity": {"type": "float"},
            "pir": {"type": "float"}
        }
    }
}

# Read data from Kafka
df1 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "office-input") \
    .load()

# Select key and value columns
df2 = df1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Split the value column and cast to appropriate types
df3 = df2.withColumn("ts_min_bignt", F.split(F.col("value"), ",")[0].cast(IntegerType())) \
         .withColumn("temperature", F.split(F.col("value"), ",")[1].cast(FloatType())) \
         .withColumn("pir", F.split(F.col("value"), ",")[2].cast(FloatType())) \
         .withColumn("light", F.split(F.col("value"), ",")[3].cast(FloatType())) \
         .withColumn("humidity", F.split(F.col("value"), ",")[4].cast(FloatType())) \
         .withColumn("co2", F.split(F.col("value"), ",")[5].cast(IntegerType())) \
         .withColumn("room_ID", F.split(F.col("value"), ",")[6].cast(StringType())) \
         .withColumn("event_ts_min", F.split(F.col("value"), ",")[7].cast(TimestampType())) \
         .drop("value")
    
# Convert ts_min_bignt to milliseconds and drop the original column
df3 = df3.withColumn("ts_min", F.col("ts_min_bignt").cast(LongType()) * 1000).drop("ts_min_bignt")

# Print the schema of the DataFrame
df3.printSchema()

# Initialize Elasticsearch connection
es = Elasticsearch("http://es:9200")

# Define checkpoint directory
checkpointDir = "/home/train/00_elk_spark_kafka/"

# Define room numbers
room_numbers = ['776', '754', '752']

# Loop through room numbers
for room_number in room_numbers:
    index_name = "smart-sytem-office-" + room_number
    try:
        # Delete existing index if any
        es.indices.delete(index=index_name)
        print(f"Index {index_name} deleted.")
    except:
        print(f"No index {index_name}")

    # Create new index with settings and mappings
    es.indices.create(index=index_name, body=system_index)

# Define function to write data to Elasticsearch
def write_to_sinks(df, batchId):
    room_numbers = ['776', '754', '752']
    for room_number in room_numbers:
        room_df = df.filter(df["room_ID"] == room_number)
        index_name = "smart-sytem-office-" + room_number
        room_df.write \
            .format("org.elasticsearch.spark.sql") \
            .mode("append") \
            .option("es.nodes", "es") \
            .option("es.port", "9200") \
            .option("es.resource", index_name) \
            .option("es.net.http.header.Content-Type", "application/json") \
            .save()

# Define the streaming query
streamingQuery = (df3
                  .writeStream
                  .foreachBatch(write_to_sinks)
                  .outputMode("append")
                  .option("checkpointLocation", checkpointDir)
                  .start())

# Await termination of the streaming query
streamingQuery.awaitTermination()




