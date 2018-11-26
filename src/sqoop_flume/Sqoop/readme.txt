# Sqoop reads source data from '/user/cloudera/HW_Sqoop' directory and
# imports them to staging table 'staging_weather' and later to 'weather'.

# The following parameters were used for optimization:
# 1. --batch enables JDBC batching that doing prepared statements with multiple sets of values.
# 2. -Dsqoop.export.records.per.statement = 1000 lets to send multiple inserts in one statement and it is supported by MySQL database.
# 1000 is a default maximum bulk insert count in MySQL.
# 3. -Dsqoop.export.records.per.transaction = 100 lets to executes multiple (100) insert statements during the transaction and
# lets to reduce overhead with initiating transactions. 
# 
# 4. --num-mappers 6, chosen experimentally. 
# MySQL supports 150 open connections by default, assumed that export runs during a day and we do not need to increase the load 
# on the database. 

# --staging-table staging_weather parameter is used for exporting data to staging table at first and if only 
# it will be completed successfully the result stores in the 'weather' table.
