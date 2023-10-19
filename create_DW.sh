#!/bin/bash

# Add debugging statements
echo "Starting Hive query execution from script..."
echo "Current user: $(whoami)"

# # Print environment variables
# echo "Environment variables:"
# env

# # Print the classpath
# echo "Classpath:"
# echo $CLASSPATH

# Create flight_dw database if not exist
hive -f /mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/DW_script/create_table/CreateDatabase.sql

# Create Dimension and Fact tables
echo "[INFO] Create Dimension and Fact Tables..."
hive -f /mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/DW_script/create_table/arrivaldim.sql
hive -f /mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/DW_script/create_table/datedim.sql
hive -f /mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/DW_script/create_table/DelayTypeDim.sql
hive -f /mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/DW_script/create_table/departuredim.sql
hive -f /mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/DW_script/create_table/destinationdim.sql
hive -f /mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/DW_script/create_table/timedim.sql
hive -f /mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/DW_script/create_table/weatherdim.sql
hive -f /mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/DW_script/create_table/flightfact.sql

# Add a message to indicate the end of the script
echo "Hive query execution from script completed."



