(ns cc-tag.util
  (:use clojure.reflect)
  (:require [cljc.java-time.local-date :as ld]))

(defn date-range
  "return date string with format yyyy-mm-dd that between s and e"
  ([s e] (map #(str (ld/plus-days (ld/parse s) %)) (range 0 (inc (- (ld/to-epoch-day (ld/parse e)) (ld/to-epoch-day (ld/parse s)))) 1)))
  ([s e step] (map #(str (ld/plus-days (ld/parse s) %)) (range 0 (inc (- (ld/to-epoch-day (ld/parse e)) (ld/to-epoch-day (ld/parse s)))) step))))
;; list methods in the obj/class
(defn list-fn [obj]
    (->> obj reflect 
             :members 
             (filter :return-type)  
             (map :name) 
             sort 
             (map #(str "." %) )
             distinct
             println))












