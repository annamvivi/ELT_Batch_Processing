#!/bin/bash

# Add debugging statements
echo "Starting Hive truncate query execution from script..."
echo "Current user: $(whoami)"

# # Print environment variables
# echo "Environment variables:"
# env

# # Print the classpath
# echo "Classpath:"
# echo $CLASSPATH

# Truncate Dimension and Fact tables
echo "[INFO] Truncate Dimension and Fact Tables..."
hive -f /mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/DW_script/truncate_table/truncate_dim_and_fact_DW.sql

# Add a message to indicate the end of the script
echo "Hive query execution from script completed."