from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

# Correct schema reflecting actual Wikipedia event structure
schema = StructType([
    StructField("meta", StructType([
        StructField("domain", StringType(), True),
        StructField("dt", StringType(), True)
    ]), True),
    StructField("performer", StructType([
        StructField("user_text", StringType(), True),
        StructField("user_is_bot", BooleanType(), True)
    ]), True),
    StructField("page_title", StringType(), True)
])

def main():
    spark = SparkSession.builder \
        .appName("WikiStreamFilter") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. Read from Kafka 'input' topic
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-server:9092") \
        .option("subscribe", "input") \
        .option("startingOffsets", "earliest") \
        .load()

    # 2. Parse nested JSON structure
    parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select(
            col("data.performer.user_text").alias("user_id"),
            col("data.meta.domain").alias("domain"),
            col("data.meta.dt").alias("created_at"),
            col("data.page_title").alias("page_title"),
            col("data.performer.user_is_bot").alias("user_is_bot")
        )

    # 3. Filtering logic
    allowed_domains = ["en.wikipedia.org", "www.wikidata.org", "commons.wikimedia.org"]

    filtered_stream = parsed_stream.filter(
        (col("domain").isin(allowed_domains)) &
        (col("user_is_bot") == False)
    )

    # 4. Prepare for 'processed' topic
    kafka_output = filtered_stream.selectExpr("to_json(struct(*)) AS value")

    # 5. Write to Kafka 'processed'
    query = kafka_output.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-server:9092") \
        .option("topic", "processed") \
        .option("checkpointLocation", "/tmp/checkpoint_filter") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()