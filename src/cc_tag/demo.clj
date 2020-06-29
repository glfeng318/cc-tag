(ns cc-tag.demo
  (:require [cc-tag.util :as util])
  (:require [cljc.java-time.local-date :as ld])
  (:import [org.apache.spark.sql SparkSession]
           [org.apache.spark.sql SaveMode])
  (:gen-class))

(defn test-spark []
  (let [spark (-> (SparkSession/builder)
                  (.appName "cc-tag")
                  (.master "local[*]")
                  (.config "spark.sql.warehouse.dir" "./resources")
                  ;; (.enableHiveSupport)
                  (.getOrCreate))]
  (prn (.version spark))
  (-> spark
    (.sqlContext)
    (.setConf "spark.sql.hive.convertMetastoreOrc", "false"))
  ; (-> spark
  ;   (.read)
  ;   (.textFile "s.json"))
  (.stop spark)
  (prn "stop spark")))

(defn -main [& args]
  (test-spark)
  (prn (util/date-range "2020-01-01" "2020-02-02"))
  (prn "demo"))