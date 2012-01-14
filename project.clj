(defproject db_loader "1.0.0"
  :description "Tool to load data from runs into various formats and locations"
  :dev-dependencies [[swank-clojure "1.4.0-SNAPSHOT"]]  
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [org.clojure/clojure-contrib "1.2.0"]
		 [org.clojure/java.jdbc "0.1.1"]
		 [mysql/mysql-connector-java "5.1.6"]
                 [local-file "0.0.4"]]
  :main db_loader)