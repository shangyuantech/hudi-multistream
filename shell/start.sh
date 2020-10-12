#!/usr/bin/env bash

ps -ef | grep multistream-run-base-1.0.0-SNAPSHOT.jar | grep -v grep | awk '{print $2}' | xargs kill -9
if [ `whoami` = "root" ];then
 echo "root用户！切换到hdfs用户"
 su hdfs -c "nohup spark-submit --master yarn --jars $(echo libs/*.jar | tr ' ' ','),../../hoodie/spark-avro_2.11-2.4.3.jar,../../hoodie/hudi-utilities-bundle_2.11-0.6.1-SNAPSHOT.jar,etl-stream-1.2.0-SNAPSHOT.jar --driver-java-options "-Dspring.profiles.active=bigdata" --class org.apache.hudi.multistream.HudiMultistreamApplication --conf spark.driver.userClassPathFirst=true multistream-run-base-1.0.0-SNAPSHOT.jar >multistream.log 2>&1 &"
else
 nohup /home/hdfs/software/spark/bin/spark-submit \
    --master yarn \
    --jars $(echo libs/*.jar | tr ' ' ','),../../hoodie/spark-avro_2.11-2.4.3.jar,../../hoodie/hudi-utilities-bundle_2.11-0.6.1-SNAPSHOT.jar,etl-stream-1.2.0-SNAPSHOT.jar  \
    --driver-java-options "-Dspring.profiles.active=bigdata" \
    --class org.apache.hudi.multistream.HudiMultistreamApplication \
    --conf spark.driver.userClassPathFirst=true \
    multistream-run-base-1.0.0-SNAPSHOT.jar >multistream.log 2>&1 &
fi
 echo "启动完毕！"