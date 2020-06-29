(ns cc-tag.game
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
              url "jdbc:mysql://ip:port/Cboard?characterEncoding=UTF-8"
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
        ;;
        (.setProperty prop "driver" "com.mysql.jdbc.Driver")
        (.setProperty prop "user" "xxxxx")
        (.setProperty prop "password" "xxxxx")
        
        ;; 设置orc解析模式 如果分区下没有文件也能在sql查询不抛错
        (-> spark
            (.sqlContext)
            (.setConf "spark.sql.hive.convertMetastoreOrc", "false"))
        ;; 查询曝光并更新到mysql
        (-> spark
            (.sql (format "select a.app,a.apk,count(1) as show_pv,count(distinct a.mac) as show_uv
                           from
                           (
                           select data['app_name'] as app,upper(data['app_package_name']) as apk,upper(mac) as mac
                           from default.base_clog
                           where day = '20200101'
                               and productid='CC_AppStore'
                               and name='detail_exposure'
                           ) a inner join 
                           (
                           select app_name as app,appname as apk 
                           from coocaa_rds.rds_app_info 
                           where day='2020-01-01' and parent_name='游戏'
                           ) b on a.apk=b.apk
                           group by a.app,a.apk" date))
            (.write)
            (.mode SaveMode/Overwrite)
            (.jdbc url "app_detail_page_show" prop))
        ;; 曝光
        ;; spark.read.jdbc的table参数(dbtable配置项)可以是表名或子查询, 如果为子查询, 必须放在括号中并分配一个别名.
        ;; https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
        (-> spark
            (.read)
            (.jdbc url format("(select dt,app,apk,show_pv,show_uv from app_detail_page_show where dt='2020-01-01') as tbl_alias") prop)
            (.cache)
            (.createOrReplaceTempView "app_detail_page_show"))
        ;; 下载/安装/启动
        (-> spark
            (.read)
            (.jdbc url format("(select start_date,partner,app_id,app_name,download_user_num,download_num,install_user_num,install_num,start_num from application_download_install_start_analysis_day where start_date='2020-01-01') as tbl_alias") prop)
            (.cache)
            (.createOrReplaceTempView "app_download_install_start"))
        ;; 新增+活跃+累计
        (-> spark
            (.read)
            (.jdbc url format("(select start_dt,parent_name,sort_name,app_name,appname,accumulated_user,new_user,active_user,active_rate from third_app_user_day where start_dt='2020-01-01') as tbl_alias") prop)
            (.cache)
            (.createOrReplaceTempView "app_user"))
        ;; 留存
        (-> spark
            (.read)
            (.jdbc url format("(select dt,parent_name,sort_name,app_name,appname,re_2,re_7 from homepage_content_thirdapp_new_user_retention where dt='2020-01-01') as tbl_alias") prop)
            (.cache)
            (.createOrReplaceTempView "app_new_user_retention"))
        ;; 营收
        (-> spark
            (.read)
            (.jdbc url format("(select update_date,app_code,app_name,app_real_name,app_type_code,sum_user,new_user,user_success, order_success, actual_amount_success from pay_analysis_app where app_type_code='游戏' and update_date='2020-01-01') as tbl_alias") prop)
            (.cache)
            (.createOrReplaceTempView "app_pay"))
        (.stop spark)))
    

