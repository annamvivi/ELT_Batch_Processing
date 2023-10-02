from spark_hdfs import SparkHDFSConnector, SparkHDFSReader

if __name__ == "__main__":
    hdfs_url = "http://localhost:9870"
    hdfs_user = "anna"
    spark_app_name = "MySparkApp"
    spark_master_url = "local[2]"  # Use Spark cluster's master URL if not running locally

    # Create an instance of the SparkHDFSConnector class
    connector = SparkHDFSConnector(hdfs_url, hdfs_user, spark_app_name, spark_master_url)

    # Define the Parquet file path
    parquet_file_path = "hdfs://localhost:9000/user/anna/flight_data/Flight_Delay.parquet"

    # Read and show the Parquet file using the connector
    df = connector.read_parquet_from_hdfs(parquet_file_path)

    # Stop the Spark session for the connector
    connector.stop_spark_session()

    # Create an instance of the SparkHDFSReader class
    hdfs_reader = SparkHDFSReader(hdfs_url, hdfs_user)

    # Create a Spark session for other operations
    hdfs_reader.create_spark_session(app_name="MySparkApp2")

    # Read and print the schema for the Parquet file using the reader
    df = hdfs_reader.read_parquet_file(parquet_file_path)
    hdfs_reader.show_dataframe(df)
    hdfs_reader.print_schema(df)

    # Close the Spark session for other operations
    hdfs_reader.close_spark_session()
