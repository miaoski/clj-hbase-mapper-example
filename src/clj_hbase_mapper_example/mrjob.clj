(ns clj-hbase-mapper-example.mrjob
  (:require [clojure-hbase.core     :as hb]
            [clojure.tools.logging  :as log]
            [clojure-hadoop.wrap    :as wrap]
            [clojure-hadoop.gen     :as gen]
            [clojure-hadoop.job     :as job]
            [clojure-hadoop.imports :as imp])
  (:use [clojure.java.io        :only [reader]]
        [clojure.string         :only [split]]
        [clojure-hadoop.context :only (with-context)])
  (:import (java.util StringTokenizer)
           (org.apache.hadoop.hbase HBaseConfiguration)
           (org.apache.hadoop.hbase.security User)
           (org.apache.hadoop.util Tool)
           (java.io IOException)))

(imp/import-conf)
(imp/import-io)
(imp/import-fs)
(imp/import-mapreduce)
(imp/import-mapreduce-lib-input)
(imp/import-mapreduce-lib-output)

(gen/gen-job-classes)
(gen/gen-main-method)

(defn parseint [x]
  (try (Integer/parseInt x) (catch Exception e 0)))

(defn remove-brakets [s]
  (subs s 1 (- (count s) 1)))

(def TYPE   (atom ""))
(def HTABLE (atom "default.table"))
(def CF     (atom "default:cf"))

(defn feeder-map
  "Input: rowkey [column-quantifier#value]"
  [line]
  (let [[rowkey cx] (split line #"\t")]
    (if (some nil? [rowkey cx])
      (log/warn "Ignore invalid line:" line)
      (let [pairs (split (remove-brakets cx) #",")]
        (doseq [p pairs]
          (let [[s cnt] (split p #"#")]
            (if (some nil? [s cnt])
              (log/warn "Ignore invalid value:" line)
              (hb/with-table [ht (hb/table @HTABLE)]
                             #_(log/info "Putting:" rowkey @CF ":" s "=" cnt)
                             (hb/put ht rowkey :value [(keyword @CF) s cnt])))))))))


(defn feeder-value
  "Input: rowkey value"
  [line]
  (let [[rowkey cx] (split line #"\t")
        [family fq] (split @CF  #":")]
    (if (some nil? [rowkey cx])
      (log/warn "Ignore invalid line:" line)
      (hb/with-table [ht (hb/table @HTABLE)]
                     #_(log/info "Putting:" rowkey (str family ":" fq) "=" cx)
                     (hb/put ht rowkey :value [(keyword family) fq cx])))))


(defn mapper-setup
  [this context]
  (with-context context
                (let [configuration (.getConfiguration context)]
                  (reset! TYPE   (.get configuration "cljhbex.type"))
                  (reset! HTABLE (.get configuration "cljhbex.table"))
                  (reset! CF     (.get configuration "cljhbex.cf")))))

(defn mapper-map
  [this key value ^MapContext context]
    (cond
      (= @TYPE "map")
      (feeder-map (str value))
      (= @TYPE "value")
      (feeder-value (str value)))
    (.write context key (LongWritable. 1)))  ; can be (Text. (str key))


(defn- hbase-set-kerberos [#^Job job]
  (if (User/isSecurityEnabled)
    (try
      (.obtainAuthTokenForJob (User/getCurrent) (.getConfiguration job) job)
      (catch IOException e
        (throw (IOException. "Failed to obtain current user.")))
      (catch InterruptedException e
        (throw (InterruptedException. "Interrupted obtaining user authentication token"))))))


(defn- set-htable-name [#^Job job args]
  (.set (.getConfiguration job) "cljhbex.type"  (first args))
  (.set (.getConfiguration job) "cljhbex.table" (nth args 2))
  (.set (.getConfiguration job) "cljhbex.cf"    (nth args 3)))


(defn tool-run [^Tool this args]
  (doto (Job. (HBaseConfiguration/create))
    (.setJarByClass (.getClass this))
    (.setJobName (str "clj-hbase-mapper-example.mrjob: " (second args)))
    (.setMapperClass (Class/forName "clj_hbase_mapper_example.mrjob_mapper"))
    (.setReducerClass (Class/forName "clj_hbase_mapper_example.mrjob_reducer"))
    (.setNumReduceTasks 0)
    (.setInputFormatClass TextInputFormat)
    (.setOutputFormatClass NullOutputFormat)
    (FileInputFormat/setInputPaths ^String (second args))
    (set-htable-name args)
    (hbase-set-kerberos)
    (.waitForCompletion true))
  0)  
