#!/usr/bin/env bash

MODULE_NAME="HW_HIVE_2"

LOCAL_PATH="/home/cloudera"
LOCAL_APP_PATH=$LOCAL_PATH/$MODULE_NAME
LOCAL_SOURCE_PATH=$LOCAL_APP_PATH/files

INPUT_DATA_ARCHIEVE_NAME="initial.tar"

FLYING_FILE_NAME="2007.csv"
AIRPORTS_FILE_NAME="airports.csv"
CARRIERS_FILE_NAME="carriers.csv"

DICTIONARY_FLYING_NAME="flying"
DICTIONARY_AIRPORTS_NAME="airports"
DICTIONARY_CARRIERS_NAME="carriers"

HIVE_QUERY_SCRIPT_NAME="hiveQuery.hql"

echo "Creating directories..."
rmdir -rf $LOCAL_APP_PATH
mkdir $LOCAL_APP_PATH
mkdir $LOCAL_SOURCE_PATH
mkdir $LOCAL_APP_PATH/$DICTIONARY_FLYING_NAME
mkdir $LOCAL_APP_PATH/$DICTIONARY_AIRPORTS_NAME
mkdir $LOCAL_APP_PATH/$DICTIONARY_CARRIERS_NAME

echo "Copy distributive to the created directory..."
cp $HIVE_QUERY_SCRIPT_NAME $LOCAL_APP_PATH
cp $INPUT_DATA_ARCHIEVE_NAME $LOCAL_SOURCE_PATH

echo "Unzipping initial source files..."
cd $LOCAL_SOURCE_PATH
tar -xvf $LOCAL_SOURCE_PATH/$INPUT_DATA_ARCHIEVE_NAME -C $LOCAL_SOURCE_PATH --force-local

echo "Copying txt files..."
cp $LOCAL_SOURCE_PATH/$FLYING_FILE_NAME $LOCAL_APP_PATH/$DICTIONARY_FLYING_NAME
cp $LOCAL_SOURCE_PATH/$AIRPORTS_FILE_NAME $LOCAL_APP_PATH/$DICTIONARY_AIRPORTS_NAME
cp $LOCAL_SOURCE_PATH/$CARRIERS_FILE_NAME $LOCAL_APP_PATH/$DICTIONARY_CARRIERS_NAME

echo "Creating tables, importing data and runnig select in Hive..."
hive --hiveconf FLYING_FILE_NAME=$LOCAL_APP_PATH/$DICTIONARY_FLYING_NAME --hiveconf AIRPORTS_FILE_NAME=$LOCAL_APP_PATH/$DICTIONARY_AIRPORTS_NAME --hiveconf CARRIERS_FILE_NAME=$LOCAL_APP_PATH/$DICTIONARY_CARRIERS_NAME --hiveconf hive.vectorized.execution.enabled = true --hiveconf hive.exec.dynamic.partition.mode=nonstrict --hiveconf hive.exec.dynamic.partition = true -f $LOCAL_APP_PATH/$HIVE_QUERY_SCRIPT_NAME;
hive_status=$?

echo "Script has been completed..."
rm -R $LOCAL_SOURCE_PATH
rm -R $LOCAL_APP_PATH

