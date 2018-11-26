#!/usr/bin/env bash

# Script unpacks distributive code.
# Registers custom UDF function agentparser.
# Selects most popular device, browser, OS for each city using the agentparser.

MODULE_NAME="HW_HIVE_3"

LOCAL_PATH="/home/cloudera"
LOCAL_APP_PATH=$LOCAL_PATH/$MODULE_NAME
LOCAL_SOURCE_PATH=$LOCAL_APP_PATH/files

CUSTOM_UDF_JAR_NAME="agentparser.jar"
CUSTOM_UDF_NAME="agentparser"
INPUT_DATA_ARCHIEVE_NAME="initial.tar"
EXTENDED_LIBRARY_NAME="UserAgentUtils-1.21.jar"

CITY_FILE_NAME="city.en.txt"

DICTIONARY_CITY_NAME="city"
DICTIONARY_IMP_NAME="imp"

HIVE_QUERY_SCRIPT_NAME="hiveQuery.hql"

echo "Creating directories..."
rmdir -rf $LOCAL_APP_PATH
mkdir $LOCAL_APP_PATH
mkdir $LOCAL_SOURCE_PATH
mkdir $LOCAL_APP_PATH/$DICTIONARY_CITY_NAME
mkdir $LOCAL_APP_PATH/$DICTIONARY_IMP_NAME

echo "Copy distributive to the created directory..."
cp $CUSTOM_UDF_JAR_NAME $LOCAL_APP_PATH
cp $EXTENDED_LIBRARY_NAME $LOCAL_APP_PATH
cp $HIVE_QUERY_SCRIPT_NAME $LOCAL_APP_PATH
cp $INPUT_DATA_ARCHIEVE_NAME $LOCAL_SOURCE_PATH

echo "Unzipping initial source files..."
cd $LOCAL_SOURCE_PATH
tar -xvf $LOCAL_SOURCE_PATH/$INPUT_DATA_ARCHIEVE_NAME -C $LOCAL_SOURCE_PATH --force-local

echo "Copying txt files..."
cp $LOCAL_SOURCE_PATH/$CITY_FILE_NAME $LOCAL_APP_PATH/$DICTIONARY_CITY_NAME
cp $LOCAL_SOURCE_PATH/$DICTIONARY_IMP_NAME*.txt $LOCAL_APP_PATH/$DICTIONARY_IMP_NAME

#rm -R $LOCAL_SOURCE_PATH

echo "Adding to HIVE_AUX_JARS_PATH environment variable path to custom UDF..."
export HIVE_AUX_JARS_PATH="$LOCAL_APP_PATH"

echo "Registering agent parser UDF, creating tables, importing data and runnig select in Hive..."
hive --hiveconf CITY_FILE_NAME=$LOCAL_APP_PATH/$DICTIONARY_CITY_NAME --hiveconf IMP_FILE_NAME=$LOCAL_APP_PATH/$DICTIONARY_IMP_NAME --hiveconf hive.vectorized.execution.enabled = true --hiveconf hive.exec.dynamic.partition = true --hiveconf hive.exec.dynamic.partition.mode = nonstrict --hiveconf hive.exec.max.dynamic.partitions=1000 -f $LOCAL_APP_PATH/$HIVE_QUERY_SCRIPT_NAME;
hive_status=$?

echo "Script has been completed..."
rm -R $LOCAL_SOURCE_PATH
rm -R $LOCAL_APP_PATH
