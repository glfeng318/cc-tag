# cc-tag

write spark jobs with clojure

## test

java -cp ./target/cc-tag-0.1.0-standalone.jar cc_tag.demo

## run

/usr/hdp/current/spark2-client/bin/spark-submit --master yarn --deploy-mode client --executor-memory=8g --driver-memory=3g --num-executors=5 --executor-cores 4 --class cc_tag.new1567 cc-tag-0.1.0-standalone.jar 2019-12-12 2020-01-08



/usr/hdp/current/spark2-client/bin/spark-submit --driver-class-path /home/yuanfeng/jobs/mysql-connector-java-5.1.29.jar --jars /home/yuanfeng/jobs/mysql-connector-java-5.1.29.jar --master yarn --deploy-mode client --executor-memory=8g --driver-memory=3g --num-executors=5 --executor-cores 4 --class cc_tag.new1567 cc-tag-0.1.0-standalone.jar 2019-12-12 2020-01-08


