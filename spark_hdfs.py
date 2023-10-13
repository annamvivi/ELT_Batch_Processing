# spark_hdfs_connector.py
from pyspark.sql import SparkSession
from pyspark import SparkConf
from hdfs import InsecureClient

class SparkHDFSConnector:
    def __init__(self, hdfs_base_url, hdfs_user, spark_app_name, spark_master_url="local[2]"):
        self.hdfs_client = InsecureClient(hdfs_base_url, user=hdfs_user)
        self.spark_conf = SparkConf().setAppName(spark_app_name).setMaster(spark_master_url)
        self.spark_conf.set("spark.hadoop.fs.defaultFS", hdfs_base_url)
        self.spark_conf.set("spark.hadoop.yarn.resourcemanager.hostname", "localhost:8088")
        self.spark_session = SparkSession.builder.config(conf=self.spark_conf).getOrCreate()

    def read_parquet_from_hdfs(self, parquet_file_path):
        df = self.spark_session.read.parquet(parquet_file_path)
        return df
    
    # def read_json_from_hdfs(self, json_file_path):
    #     df = self.spark_session.read.json(json_file_path)
    #     return df

    def stop_spark_session(self):
        self.spark_session.stop()

class SparkHDFSReader:
    def __init__(self, hdfs_url, hdfs_user):
        self.hdfs_url = hdfs_url
        self.hdfs_user = hdfs_user
        #self.spark = None

    def connect_to_hdfs(self):
        # Connect to HDFS
        hdfs_client = InsecureClient(self.hdfs_url, user=self.hdfs_user)
        return hdfs_client

    def create_hdfs_spark_session(self, app_name="HDFSSparkApp", master_url="local[2]"):
        # Create a SparkConf object and set Hadoop configuration properties
        conf = SparkConf() \
            .setAppName(app_name) \
            .setMaster(master_url)

        conf.set("spark.hadoop.fs.defaultFS", self.hdfs_url)
        conf.set("spark.hadoop.yarn.resourcemanager.hostname", "localhost")

        # Create a SparkSession with the configured conf object
        self.spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()

    def read_parquet_file(self, file_path):
        # Read the Parquet file
        df = self.spark.read.parquet(file_path)
        return df
    
    # def read_json_file(self, file_path):
    #     # Read the JSON file
    #     df = self.spark.read.json(file_path)
    #     return df


    def show_dataframe(self, df, num_rows=5):
        # Show the first 'num_rows' rows of the DataFrame
        df.show(num_rows)

    def print_schema(self, df):
        # Print the schema (column names and data types) of the DataFrame
        df.printSchema()

    def close_spark_session(self):
        # Don't forget to stop the SparkSession when done
        self.spark.stop()

class SparkOperations:
    def __init__(self, app_name="MySparkApp", master_url="local[2]"):
        # Create a SparkConf object and set Hadoop configuration properties
        conf = SparkConf() \
            .setAppName(app_name) \
            .setMaster(master_url)
        
        # Set any additional Spark configuration properties here


        # Create a SparkSession with the configured conf object
        self.spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()
        
    # Method to create a Spark DataFrame from a Pandas DataFrame
    def create_spark_dataframe(self, pandas_df):
        return self.spark.createDataFrame(pandas_df)

    def read_parquet_file(self, file_path):
        # Read the Parquet file
        df = self.spark.read.parquet(file_path)
        return df
    
    def read_json_file(self, file_path):
        # Read the JSON file
        df = self.spark.read.json(file_path)
        return df

    def show_dataframe(self, df, num_rows=5):
        # Show the first 'num_rows' rows of the DataFrame
        df.show(num_rows)

    def print_schema(self, df):
        # Print the schema (column names and data types) of the DataFrame
        df.printSchema()

    def create_dataframe_with_schema(self, data, custom_schema):
        # Create a DataFrame with the custom schema
        json_df = self.spark.createDataFrame(data, schema=custom_schema)
        return json_df

    def close_spark_session(self):
        # Don't forget to stop the SparkSession when done
        self.spark.stop()

