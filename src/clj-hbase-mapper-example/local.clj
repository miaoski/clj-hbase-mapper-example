(ns clj-hbase-mapper-example.local
  (:require [clojure-hbase.core    :as hb]
            [clojure.tools.logging :as log])
  (:use [clojure.java.io :only [reader]]
        [clojure.string  :only [split]])
  (:gen-class :main true))


(defn parseint [x]
  (try (Integer/parseInt x) (catch Exception e 0)))

(defn remove-brakets [s]
  (subs s 1 (- (count s) 1)))

(defn feeder-map
  "Input: rowkey [column-quantifier#value]"
  [htable cf csv]
  (let [[rowkey cx] (split csv #"\t")]
    (if (some nil? [rowkey cx])
      (log/warn "Ignore invalid line:" csv)
      (let [sub   (remove-brakets cx)
            pairs (split sub #",")]
        (doseq [p pairs]
          (let [[country cnt] (split p #"#")]
            (if (some nil? [country cnt])
              (log/warn "Ignore invalid line:" csv)
              (hb/with-table [ht (hb/table htable)]
                             (log/info "Putting:" rowkey cf ":" country "=" cnt)
                             (hb/put ht rowkey :value [(keyword cf) country cnt])))))))))

(defn feeder-value
  "Input: rowkey value"
  [htable column csv]
  (let [[rowkey cx] (split csv #"\t")
        [cf cq]     column]
    (if (some nil? [rowkey cx])
      (log/warn "Ignore invalid line:" csv)
      (hb/with-table [ht (hb/table htable)]
                     (log/info "Putting:" rowkey (str cf ":" cq) "=" cx)
                     (hb/put ht rowkey :value [(keyword cf) cq cx])))))


(defn feeder
  [mapper-fn htable cf]
  (let [rdr (java.io.BufferedReader. *in*)]
    (doall (pmap #(mapper-fn htable cf %) (line-seq rdr)))))


(def usage (str "Usage: clj_hbase_mapper_example.local map   table-name column-family\n"
                "       clj_hbase_mapper_example.local value table-name cf:column\n"
                "Program reads data from STDIN."))

(defn -main
  [& args]
  (let [[type table column-family] args]
    (if (some nil? [type table column-family])
      (println usage)
      (cond
        (= "map" type)
        (feeder feeder-map table column-family)
        (= "value" type)
        (let [[cf cq] (split column-family #":")]
          (if (some nil? [cf cq])
            (log/error "Please specify cf:column")
            (feeder feeder-value table [cf cq])))
        :else
        (println usage)))
    (shutdown-agents)))
