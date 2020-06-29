(ns cc-tag.svip
  (:require [cc-tag.util :as util])
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
                    (.appName "cc-tag$svip")
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
      (doseq [date (util/date-range s e)]
        (prn date))
      (doseq [date (map #(str (ld/plus-days (ld/parse s) %)) (range 0 (inc (- (ld/to-epoch-day (ld/parse e)) (ld/to-epoch-day (ld/parse s)))) 10))]
        (-> spark
          (.sql (format "select 
            t1.dt as dt,
            count(distinct t1.mac) as sdk_uv,
            sum(vv) as sdk_vv,
            sum(dur) as sdk_dur,
            count(distinct case when t3.open_id is not null then t1.mac end) as manyou_uv,
            sum(case when t3.open_id is not null then vv end) as manyou_vv,
            sum(case when t3.open_id is not null then dur end) as manyou_dur
          from
          (
            select dt,mac,count(1) as vv,sum(dur) as dur
            from
            (
              select partition_day as dt,mac,start_time,stop_time,dur
              from edw.edw_player_test
              where partition_day between '%1$s' and '%2$s' and dur between 60 and 86400
                and partner='tencent'
              group by partition_day,mac,start_time,stop_time,dur
            ) as a1 group by dt,mac
          ) t1 left join
          (
            select dt,upper(mac) as mac,min(upper(third_open_id)) as open_id 
            from coocaa_gdl.user_openid_mac_relation 
            where dt between '%1$s' and '%2$s'
            group by dt,upper(mac)
          ) as t2 on t1.mac=t2.mac and t1.dt=t2.dt left join
          (
            select distinct dt,open_id from test.yf_manyou_20191218 where dt between '%1$s' and '%2$s'
          ) t3 on t2.open_id=t3.open_id and t2.dt=t3.dt
          group by t1.dt" date (str (ld/plus-days (ld/parse date) 9)) ))
          (.repartition 30)
          (.write)
          (.mode SaveMode/Append)
          (.option "codec" "org.apache.hadoop.io.compress.SnappyCodec")
          (.insertInto "test.yf_manyou_play_20200102"))
        (prn (format "%s" date)))
      (.stop spark)))
  