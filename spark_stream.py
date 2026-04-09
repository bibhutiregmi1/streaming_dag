import logging, os, sys
from datetime import datetime


from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_keyspace(session):
    session.execute(""" 
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        with replication ={'class': 'SimpleStrategy', 'replication_factor': '1' };
        """)
    print("Keyspace created successfully!")


def create_table(session):
    session.execute(""" 
            CREATE TABLE IF NOT EXISTS spark_streams.created_users(
            id UUID primary key,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT);
            """)
    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("Inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name'),
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    post_code = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
                INSERT INTO spark_stream.created_users(id, first_name, last_name,gender, address, post_code, email,
                username,registered_date, phone, picture)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (user_id, first_name, last_name,gender, address, post_code, email,
                username,registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f"Couldn't insert data due to exception :{e} ")



def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages',
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"  # Fixed spelling
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        # s_conn = SparkSession.builder \
        #     .appName('SparkDataStreaming') \
        #     .config('spark.cassandra.connection.host', 'cassandra') \
        #     .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception :{e} ")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn\
            .readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.error(f"Couldn't create data frame due to error: {e}")
    return spark_df


def create_cassandra_connection():
    # connection to the cassandra cluster
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        return session
    except Exception as e:
        logging.error(f"Couldn't create cassandra connection due to exception: {e}")
    return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value as STRING)")\
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel



if __name__=="__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        print(f"Debug: spark_df is {type(spark_df)}")
        selection_df = create_selection_df_from_kafka(spark_df)

        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            # insert_data(session)
            logging.info("Streaming is started....")
            selection_df = selection_df.filter(selection_df.id.isNotNull())
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")\
                .option('checkpointLocation', '/tmp/checkpoint')\
                .option('keyspace', 'spark_streams')\
                .option('table', 'created_users')\
                .start())
            try:
                streaming_query.awaitTermination()
            except Exception as e:
                print(f"Streaming stopped: {e}")
            finally:
                spark_conn.stop()




