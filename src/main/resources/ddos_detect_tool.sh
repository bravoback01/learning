#!/bin/sh
############################
# This script is used to invoke DDOS detector tool. A Spark streming application
############################

. ${CONFIG_LOCATION}/ddos.properties

MAIN_JAR=${APP_JAR}
JARS=${JARS}

spark-submit \
  --class com.ddos.processor.Driver2 \
  --master yarn \
  --deploy-mode client \
  --executor-memory 1G \
  --num-executors 1 \
  --executor-cores 3 \
  --conf spark.executor.extraClassPath=${JARS} \
  --conf spark.driver.extraClassPath=${JARS} \
  $MAIN_JAR ddos_cons_group_1 ddos_topic_1
