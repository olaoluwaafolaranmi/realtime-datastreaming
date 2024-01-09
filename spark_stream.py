import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthenticator
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *


#keyspace in cassandra is similar to a schema
def create_keyspace(session):

    session.execute("""
                    CREATE KEYSPACE IF NOT EXISTS spark_streams
                    WITH replication = {'class': 'SimpleStrategy', 'replication_factor':'1'};
                    """)
    print('keyspace created successfully')

    return



def create_table(session):

    session.execute("""
                    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
                    id UUID PRIMARY KEY,
                    first_name TEXT,
                    last_name TEXT,
                    gender TEXT,
                    address TEXT,
                    post_code TEXT,
                    email TEXT,
                    username TEXT,
                    registered_date TEXT,
                    phone TEXT,
                    picture TEXT
                    );
                    """)
    print('Table created successfully')


    return


def insert_data(session, **kwargs):

    print('inserting data .....')

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('postcode')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:

        session.execute("""
                        INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, postcode,
                        email, username, dob, registered_date, phone, picture)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (user_id, first_name, last_name, gender, address, postcode,
                        email, username, dob, registered_date, phone, picture))
        logging.info(f"data inserted for {first_name} {last_name}")
    
    except Exception as e: 
        logging.error(f'could not insert data due to {e}')

    return

def connect_to_kafka(spark_conn):
    spark_df = None

    try:
        spark_df = spark_conn.readStream \
        .format('kafka')\
        .option('kafka.bootstrap.servers', 'broker:29092')\
        .option('subscribe', 'users_created')\
        .option('startingOffsets', 'earliest')\
        .load()
        logging.info('kafka dataframe created successfully')
        return spark_df
    except Exception as e:
        logging.warning(f'kafka df not created due to {e}')

        return spark_df

# 'org.apache.spark:spark-streaming-kafka-0-10_2.13:3.4.1')\ 'org.apache.kafka:kafka-clients:3.4.1'

def create_spark_connection():
    #spark_conn = None
    try:
        spark_conn = SparkSession.builder\
                    .appName("SparkDataStreaming")\
                    .config('spark.jars.packages','com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,'
                                                    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1')\
                    .config('spark.cassandra.connection.host', 'localhost')\
                    .getOrCreate()
        
        spark_conn.sparkContext.setLogLevel('ERROR')
        logging.info('spark connection created successfully')
        return spark_conn
    except Exception as e:
        logging.error(f'unable to create spark session due to {e}')

        return spark_conn



def create_cassandra_connection():
    #connecting to cassandra cluster
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()

        return session
    except Exception as e:
        logging.error(f'unable to create cassandra session due to {e}')
        
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField('id', StringType(), False),
        StructField('first_name', StringType(), False),
        StructField('last_name', StringType(), False),
        StructField('gender', StringType(), False),
        StructField('address', StringType(), False),
        StructField('postcode', StringType(), False),
        StructField('email', StringType(), False),
        StructField('username', StringType(), False),
        StructField('registered_date', StringType(), False),
        StructField('phone', StringType(), False),
        StructField('picture', StringType(), False)
    ])

    sel = spark_df.selectExpr('CAST(value AS STRING)')\
            .select(from_json(col('value'), schema).alias('data')).select('data.*')
    
    print(sel)

    return sel

if __name__ == "__main__":

    #create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        #connect to kafka with spark connection
        df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(df)
        sess = create_cassandra_connection()

        if sess is not None:
            create_keyspace(sess)
            create_table(sess)
            

            # streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
            #                                         .option('checkpointLocation', '/tmp/checkpoint')
            #                                         .option('keyspace', 'spark_streams')
            #                                         .option('table', 'created_users')
            #                                         .start())
            
            # streaming_query.awaitTermination()

