from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# Schema must match the JSON structure in your 'processed' Kafka topic
processed_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("page_title", StringType(), True)
])


def main():
    # 1. Initialize Spark Session with Cassandra connector configs
    spark = SparkSession.builder \
        .appName("WikiToCassandra") \
        .config("spark.cassandra.connection.host", "cassandra-db") \
        .config("spark.cassandra.connection.port", "9042") \
        .config("spark.sql.caseSensitive", "false") \
        .getOrCreate()

    # Set log level to avoid excessive console output
    spark.sparkContext.setLogLevel("WARN")

    # 2. Read Stream from Kafka 'processed' topic
    processed_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-server:9092") \
        .option("subscribe", "processed") \
        .option("startingOffsets", "earliest") \
        .load()

    # 3. Parse JSON and explicitly convert types
    # Primary keys in Cassandra cannot be NULL, so we cast created_at to Timestamp correctly
    parsed_df = processed_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), processed_schema).alias("data")) \
        .select(
        col("data.user_id"),
        col("data.domain"),
        # Convert ISO string to Spark Timestamp
        to_timestamp(col("data.created_at")).alias("created_at"),
        col("data.page_title")
    )

    # 4. Critical: Filter out records where Primary Keys are null
    # This prevents the 'missing primary key' error you encountered
    final_df = parsed_df.filter(
        col("created_at").isNotNull() &
        col("domain").isNotNull()
    )

    # 5. Write the resulting stream to Cassandra
    query = final_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "wiki_space") \
        .option("table", "processed_pages") \
        .option("checkpointLocation", "/tmp/checkpoint_cassandra") \
        .outputMode("append") \
        .start()

    # Wait for the stream to finish (runs until interrupted)
    query.awaitTermination()


if __name__ == "__main__":
    main()