(defproject clj-hbase-mapper-example "0.0.1"
            :description "A Clojure example to put records into HBase using Hadoop mappers"
            :dependencies [[org.clojure/clojure "1.4.0"]
                           [clojure-hbase  "0.90.5-4"]
                           [clojure-hadoop "1.4.1"]
                           [org.clojure/tools.logging "0.2.6"]]
            :aot [clj-hbase-mapper-example.mrjob
                  clj-hbase-mapper-example.local])
