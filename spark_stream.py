import logging
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_keyspace(session):
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH REPLICATION = {
            'class' : 'SimpleStrategy',
            'replication_factor' : 1
        }
        """
    )

def create_table(session):
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_streams.posts (
            title TEXT PRIMARY KEY,
            upvotes INT
        )
        """
    )

def insert_data(session, **kwargs):
    title = kwargs.get('title')
    upvotes = kwargs.get('upvotes')
    try:
        session.execute(
            """
            INSERT INTO spark_streams.posts (title, upvotes)
            VALUES (%s, %s)
            """,
            (title, upvotes)
        )
        logging.info("Data inserted successfully")
    except Exception as e:
        logging.error(f"Couldn't insert data due to exception {e}")

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'reddit_post') \
            .option('startingOffsets', 'latest') \
            .load()
        logging.info("Connected to Kafka successfully")
        return spark_df
    except Exception as e:
        logging.error(f"Couldn't connect to Kafka due to exception {e}")
        return None


def create_cassandra_connection():
    try:
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Couldn't create the Cassandra connection due to exception {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("upvotes", IntegerType(), True),
    ])

    selection_df = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')) \
        .select("data.*")

    return selection_df

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    spark_conn = create_spark_connection()

    if spark_conn:
        spark_df = connect_to_kafka(spark_conn)
        if spark_df:
            selection_df = create_selection_df_from_kafka(spark_df)
            session = create_cassandra_connection()

            if session:
                create_keyspace(session)
                create_table(session)

                logging.info("Streaming is being started...")

                streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                                   .option('checkpointLocation', '/tmp/checkpoint')
                                   .option('keyspace', 'spark_streams')
                                   .option('table', 'posts')
                                   .start())

                streaming_query.awaitTermination()
