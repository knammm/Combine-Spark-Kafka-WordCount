from pyspark.sql import SparkSession
from pyspark.sql.functions import window, avg, from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Kafka broker address
kafka_bootstrap_servers = '10.1.7.194:9092'

# Kafka topic
kafka_topic = 'test_2153599'

# Spark session
spark = SparkSession.builder.master("yam").appName("IoTStream").getOrCreate()

# Define schema for IoT data
schema = StructType([
    StructField("date", StringType(), True),
    StructField("humidity", FloatType(), True),
    StructField("temperature", FloatType(), True),
    StructField("time", StringType(), True)
])

# Create DataFrame representing the stream of input lines from Kafka
iot_stream_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap_servers).option("subscribe", kafka_topic).load()
iot_data_df = iot_stream_df.select(from_json(col("value").cast("string"), schema).alias("json")).select("json.*")

# Deserialize JSON data and apply schema
#iot_data_df = iot_stream_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("json")).select("json.*")

# Define a window specification
#window_spec = window("time", "2 minutes")

# Compute average temperature and humidity for each 5 minutes
#average_data_df = iot_data_df \
#    .groupBy(window_spec) \
#    .agg(avg("temperature").alias("avg_temperature"), avg("humidity").alias("avg_humidity"))

# Start the streaming query
query = iot_data_df.writeStream.option("checkpointLocation", "/user/S2153599/IoTLab3Test/").outputMode("complete").format("console").start();

# Await termination of the query
query.awaitTermination()
