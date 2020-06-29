(defproject cc-tag "0.1.0"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [cljc.java-time "0.1.7"]]
  :repl-options {:init-ns cc-tag.core}
  :aot :all
  :main cc-tag.core
  ; scala 2.11.8 + spark 2.0.0
  :profiles {:provided {:dependencies [[org.apache.spark/spark-core_2.11 "2.0.0"]
                                       [org.apache.spark/spark-sql_2.11 "2.0.0"]]}})
