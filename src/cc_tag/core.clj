(ns cc-tag.core
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
                  (.appName "cc-tag")
                  (.config "spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                  (.config "spark.rdd.compress", "true")
                  (.config "spark.speculation.interval", "10000ms")
                  (.config "spark.sql.tungsten.enabled", "true")
                  (.config "spark.sql.shuffle.partitions", "800")
                  (.master "yarn")  ;; yarn client mode
                  (.enableHiveSupport)
                  (.getOrCreate))]
    ;; cache orders
    (-> spark
        (.sql "select upper(third_open_id) as open_id,to_date(pay_time) as pay_date
               from ods.v_ods_base_movies_iqiyi_order_all
               where dt = '2019-12-22' 
                 and order_type != 5
                 and source_sign='6'
                 and appcode='1028'
                 and to_date(pay_time) between '2019-03-20' and '2019-12-18'
                 and test_flag = 0 
                 and flag = 0 
                 and product_type not in ('5','P-Movie','7','P-Movie-Ticket')")
        (.cache)
        (.createOrReplaceTempView "ord"))
    ;; cache manyou
    (-> spark
        (.sql "select dt,utype,open_id from test.yf_manyou_20191218")
        (.cache)
        (.createOrReplaceTempView "manyou"))
    (doseq [date (map #(str (ld/plus-days (ld/parse s) %)) (range 0 (inc (- (ld/to-epoch-day (ld/parse e)) (ld/to-epoch-day (ld/parse s))))))]
      (-> spark
        (.sql (format "select distinct dt,a.open_id from
                       (select dt,open_id from manyou where dt='%1$s') a inner join
                       (select distinct open_id from ord where pay_date >= '%1$s') b on a.open_id=b.open_id" date))
        (.repartition 30)
        (.write)
        (.mode SaveMode/Append)
        (.option "codec" "org.apache.hadoop.io.compress.SnappyCodec")
        (.insertInto "test.yf_manyou_pay_20191227"))
      (prn (format "%s" date)))
    (.stop spark)))
