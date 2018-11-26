#!/usr/bin/env bash

MODULE_NAME="HW_SQOOP"

PARAM_HADOOP_HOME=/usr/lib/hadoop-mapreduce
PARAM_SOURCE_HDFS_DIR=/user/cloudera/HW_Sqoop

mysql -u root -pcloudera

use mysql;

create table staging_weather (
stationId Varchar(250), 
date Varchar(10), 
tmin int, 
tmax int, 
snow int, 
snwd int, 
prcp int);

create table weather (
stationId Varchar(250), 
date Varchar(10), 
tmin int, 
tmax int, 
snow int, 
snwd int, 
prcp int);

exit;

sqoop export -Dsqoop.export.records.per.statement=1000 -Dsqoop.export.records.per.transaction=100 --num-mappers 6 --batch --connect "jdbc:mysql://localhost/mysql" --hadoop-home $PARAM_HADOOP_HOME --password "cloudera" --username "root"  --export-dir PARAM_SOURCE_HDFS_DIR --table "weather" --staging-table staging_weather --verbose

mysql -u root -pcloudera

use mysql;
SELECT count(*) FROM weather_stage;
SELECT * FROM weather_stage ORDER BY stationid, date LIMIT 10;


exit;