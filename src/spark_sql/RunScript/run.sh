JAR_PATH="/home/cloudera/HW_Spark_SQL/spark-sql-homework.jar"
BIDS_PATH="/user/cloudera/HW_Spark_SQL/common/training/motels.home/bids"
MOTELS_PATH="/user/cloudera/HW_Spark_SQL/common/training/motels.home/motels"
EXCHANGE_RATES_PATH="/user/cloudera/HW_Spark_SQL/common/training/motels.home/exchange_rates"
OUTPUT_PATH="/user/cloudera/HW_Spark_SQL/common/training/motels.home/spark-core-output"

spark-submit --class com.epam.hubd.spark.scala.sql.homework.MotelsHomeRecommendation --master yarn-client $JAR_PATH $BIDS_PATH $MOTELS_PATH $EXCHANGE_RATES_PATH $OUTPUT_PATH
