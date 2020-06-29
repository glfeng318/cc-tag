(ns cc-tag.manyou
    (:require [cljc.java-time.local-date :as ld])
    (:import [org.apache.log4j Logger Level])
    (:import [org.apache.spark.sql SparkSession]
             [org.apache.spark.sql SaveMode])
    (:gen-class))
  
  ;; set log level
  (defn init-log-level []
    (.setLevel (Logger/getRootLogger) Level/ERROR)
    (.setLevel (Logger/getLogger "org.apache.spark") Level/WARN)
    (.setLevel (Logger/getLogger "org.spark-project") Level/WARN))
  
  
  (defn -main [& args]
    (init-log-level)
    (let [[s e] args
          spark (-> (SparkSession/builder)
                    (.appName "cc-tag$manyou")
                    (.config "spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    (.config "spark.rdd.compress", "true")
                    (.config "spark.speculation.interval", "10000ms")
                    (.config "spark.sql.tungsten.enabled", "true")
                    (.config "spark.sql.shuffle.partitions", "800")
                    (.master "yarn")  ;; yarn client mode
                    (.enableHiveSupport)
                    (.getOrCreate))]
      
      ;; 设置orc解析模式 如果分区下没有文件也能在sql查询不抛错
      (-> spark
        (.sqlContext)
        (.setConf "spark.sql.hive.convertMetastoreOrc", "false"))
      (-> spark
        (.sql "select dt,open_id from test.yf_manyou_20191218")
        (.cache)
        (.createOrReplaceTempView "manyou"))
      (doseq [date (map #(str (ld/plus-days (ld/parse s) %)) (range 0 (inc (- (ld/to-epoch-day (ld/parse e)) (ld/to-epoch-day (ld/parse s))))))]
        (-> spark
          (.sql (format "select max(dt) as dt,count(distinct open_id) as cnt from manyou where dt<='%s'" date))
          (.repartition 30)
          (.write)
          (.mode SaveMode/Append)
          (.option "codec" "org.apache.hadoop.io.compress.SnappyCodec")
          (.insertInto "test.yf_manyou_total_20191231"))
        (prn (format "%s" date)))
      (.stop spark)))
  