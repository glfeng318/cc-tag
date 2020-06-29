(ns cc-tag.new1567
    (:require [cc-tag.util :as util])
    (:require [cljc.java-time.local-date :as ld])
    (:import [org.apache.log4j Logger Level])
    (:import java.util.Properties)
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
              prop (new java.util.Properties)
              spark (-> (SparkSession/builder)
                        (.appName "cc-tag$new1567")
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
            (.sql "select dt,case when qq_openid='' then wx_openid else qq_openid end as openid,to_date(vip_pay_time) as vip_pay_time,order_no,price
                   from ods.ods_esoc_txorder where dt='2020-01-08' and price>0")
            (.cache)
            (.createOrReplaceTempView "tmp"))
        (.setProperty prop "driver" "com.mysql.jdbc.Driver")
        (.setProperty prop "user" "xxxxx")
        (.setProperty prop "password" "xxxxx")
        (doseq [date (util/date-range s e)]
            (-> spark
                (.sql (format "select a.dt as dt, total_user,new_user,buy_user,buy_cnt,buy_amt from (
                        select 
                        '%1$s' as dt,
                        count(distinct openid) as total_user,
                        count(distinct case when to_date(vip_pay_time)='%1$s' then openid end) as buy_user,
                        count(distinct case when to_date(vip_pay_time)='%1$s' then order_no end) as buy_cnt,
                        sum(case when to_date(vip_pay_time)='%1$s' then price end) as buy_amt
                        from tmp where vip_pay_time<='%1$s'
                    ) a left join
                    (
                        select max(dt) as dt,count(distinct openid) as new_user from
                        (
                        select openid,count(distinct to_date(vip_pay_time)) as buy_cnt,max(to_date(vip_pay_time)) as last_pay_date,'%1$s' as dt 
                        from tmp where openid<>'' and vip_pay_time<='%1$s'
                        group by openid
                        having last_pay_date='%1$s' and buy_cnt=1
                        ) t
                    ) b on a.dt=b.dt" date))
                (.write)
                (.mode SaveMode/Append)
                (.jdbc "jdbc:mysql://ip:port/Cboard?characterEncoding=UTF-8" "pay_new1567" prop))
            (prn (format "%s" date)))
        (.stop spark)))
          