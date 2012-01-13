;; TODO
;; need to synchronize across multiple users/accesses
;; load config file - to take args from command line?
;; move to clojure 1.3
;; Having issues with locking on cluster ; for now am just running sequentially
;; Extension/unzipping seems to be problematic
(ns db_loader
  ^{
    :author "Emma Tosch"
    :email "etosch@cs.umass.edu"
    :doc "A utility for managing log files.
USAGE: lein run -m db_loader :filename ~/log_file_location/some_output_file.log :problemname problem
       lein run -m db_loader :clean (some-list-of-csv-files | all)
       lein run -m db_loader :filename ~/log_file_location/some_output_file.log :problemname problem :mysql {password P@$$w0rd db-host localhost}

The :problemname argument is optional; it's used to look up and track semantically grouped problems (e.g. all dsoar problems).
Settings are held in the ~/.db_config file. If you do not already have one, one will be created."}
  (:import [java.io File]
	   [java.text SimpleDateFormat FieldPosition]
	   [java.util Date]
	   [java.sql BatchUpdateException])
  (:require [clojure.contrib.string :as s]
	    [clojure.contrib.duck-streams :as ds]
	    [clojure.contrib.seq-utils :as su]
	    [clojure.java.jdbc :as jdbc]
	    [clojure.contrib.sql :as sql]
	    [clojure.java.shell :as shell]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Vars ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def debug (atom false))
(def id (atom nil))
(def user (atom nil))
(def data_dir (atom nil))
(def problem_data (atom nil)) ;; hashmap of problem names to ids
(def experiments [:id :user :rundate :problem_name :problem_id :clojush_version :logfile_location :csv_write_time])
(def experiment [:id :parameter :value])
(def generations [:id :gennum :parameter :value])
(def summary [:id :successp :maxgen])
(def tables '(experiments experiment generations summary))
(def separator (System/getProperty "file.separator"))
(def datetime-format "yyyy-MM-dd kk:mm:ss")
(def atom-skip-nils (atom true))
(def db (atom nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Utility Functions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn from-symbol [symb]
  (deref (ns-resolve 'db_loader symb)))

(defn rcons [thing lst]
  (reverse (cons thing (reverse lst))))

(defn seq-split [re some-seq]
  "Returns a list of lists, split and grouped by re. e.g. (\"---\" \"param1 = val1\" \"param2 = val2\" \"---\" \"param3 = val3\" etc.)
becomes ((\"param1 = val1\" \"param2 = val2\") (\"param3 = val3\")...)"
  (lazy-seq
   (loop [old-seq some-seq
	  new-seq ()
	  unit ()]
     (cond (empty? old-seq) (if (empty? unit) new-seq (rcons unit new-seq))
	   (re-find re (first old-seq)) (recur (rest old-seq) (if (empty? unit) new-seq (rcons unit new-seq)) ())
	   :else (recur (rest old-seq) new-seq (rcons (first old-seq) unit))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Config Management ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn load-config [config-file-name]
  (let [config (if (.exists (File. config-file-name))
		 (read-string (slurp config-file-name))
		 {})]
    (reset! id (:id config 0))
    (reset! user (:user config (System/getProperty "user.name")))
    (reset! data_dir (:data_dir config (str (System/getProperty "user.home") separator "data_dir" separator)))
    (reset! problem_data (:problem_data config {"unspecified" "-1"}))))
	    
(defn save-config [config-file-name problemname]
  (spit config-file-name
	(-> (read-string (slurp config-file-name))
	    (assoc :id (inc @id))
	    (assoc :user @user)
	    (assoc :data_dir @data_dir)
	    (assoc :problem_data (if (get @problem_data problemname)
				   @problem_data
				   (swap! problem_data assoc problemname (str (inc (apply max (map read-string (vals @problem_data)))))))))))


(defmacro write-table [csv body]
  `(try
     (ds/with-out-append-writer
       (str @data_dir ~csv)
       ~body)
     true
     (catch Exception ~(symbol 'e) (do (.printStackTrace ~(symbol 'e)) false))))

(defmacro insert-table [tablename rows]
  `(try
    (sql/insert-rows ~tablename ~rows)
    true
    (catch Exception ~(symbol 'e) (do (.printStackTrace ~(symbol 'e)) false))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Experiments Table ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn experiments-vals [filename problemname experiment-data]
  [ @id
   @user
   (.toString (.format (SimpleDateFormat. datetime-format)
		       (Date. (.lastModified (File. filename)))
		       (StringBuffer.)
		       (FieldPosition. 0)))
   problemname
   (get @problem_data problemname)
   (second (su/find-first #(= (first %) "Clojush version") experiment-data))
    filename
   (.toString (.format (SimpleDateFormat. datetime-format)
		       (Date. (System/currentTimeMillis))
		       (StringBuffer.)
		       (FieldPosition. 0)))
   ])
       
(defn write-experiments [filename problemname experiment-data]
  (try
    (ds/with-out-append-writer
      (str @data_dir "experiments.csv")
      (let [fields (experiments-vals filename problemname experiment-data)]
	(doseq [field (butlast fields)]
	  (print (str field ",")))
	(println (str (last fields)))))
    true
    (catch Exception e (do (when @debug (.printStackTrace e)) false))))

(defn insert-experiments [filename problemname experiment-data]
  (try
    (sql/insert-rows :experiments (experiments-vals filename problemname experiment-data))
    true
    (catch Exception e (do (when @debug (.printStackTrace e)) false))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Experiment Table ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn experiment-vals [experiment-data]
  (for [[param value] (filter #(= 2 (count %)) experiment-data)]
    (if (or (and @atom-skip-nils (not= (s/trim value) "nil"))
	    (not @atom-skip-nils))
      [@id param value])))

(defn write-experiment [filename experiment-data]
  (try
    (ds/with-out-append-writer
      (str @data_dir "experiment.csv")
      (doseq [[id param value] (experiment-vals experiment-data)]
	      (println (str id "," param "," value))))
    true
    (catch Exception e false)))

(defn insert-experiment [filename experiment-data]
  (let [current-row (atom nil)]
    (try
      (doseq [row (experiment-vals experiment-data)]
	(try
	  (when @debug (reset! current-row row))
	  (sql/insert-rows :experiment row)
	  (catch BatchUpdateException b (ds/with-out-append-writer
					  (str @data_dir "db.err")
					  (println (.getMessage b) row)))))
      true
      (catch Exception e (do (when @debug (or (.printStackTrace e) (println :experiment  @current-row))) false)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Generations Table ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn generations-vals [generation-data]
  (let [generations (map (fn [generation]
			   (let [gen-num (re-find #"\d+$" (su/find-first #(re-find #"-\*-" %) generation))]
			     (list gen-num generation)))
			 generation-data)]
    (remove #(empty? %) (mapcat identity (for [[gen-num gen-data] generations]
					   (for [[param value] (filter #(= 2 (count %))(map #(s/split #": " %) gen-data))]
					     (if (or (and @atom-skip-nils (not= (s/trim value) "nil"))
						     (not @atom-skip-nils))
					       [ @id gen-num param (s/trim value) ])))))))
  
(defn write-generations [generation-data]
  (try
    (ds/with-out-append-writer
      (str @data_dir "generations.csv")
      (doseq [[id gen-num param value] (generations-vals generation-data)]
	(println (str id "," gen-num "," param "," value))))
    true
    (catch Exception e (do (when @debug (.printStackTrace e) ) false))))

(defn insert-generations [generation-data]
  (let [current-row (atom nil)]
    (try
      (doseq [row (generations-vals generation-data)]
	(try 
	  (when @debug (reset! current-row row))
	  (sql/insert-rows :generations row)
	  (catch BatchUpdateException b (ds/with-out-append-writer
					  (str @data_dir "db.err")
					  (println (.getMessage b) row)))))
      true
      (catch Exception e (do (when @debug (or (.printStackTrace e) (println :generations @current-row))) false)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Summary Table ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn summary-vals [summary-data]
  (let [final-gen (su/find-first #(re-find #"[SUCCESS|FAILURE] at generation \d+" %) summary-data)]
    [@id
     (boolean (re-find #"SUCCESS" final-gen))
     (re-find #"\d+$" final-gen) ;; not sure if the log with FAILURE has generation in the same line
     ]))

(defn write-summary [summary-data]
  (try
    (ds/with-out-append-writer
      (str @data_dir "summary.csv")
      (doseq [[id successp final-gen] (summary-vals summary-data)]
	(println (str id "," successp "," final-gen))))
    true
    (catch Exception e (do (when @debug (.printStackTrace e)) false))))

(defn insert-summary [summary-data]
    (try
      (sql/insert-rows :summary (summary-vals summary-data))
      true
      (catch Exception e (do (when @debug (.printStackTrace e)) false))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; MAIN ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn parse-log [filename problemname output]
  (let [partition-data (seq-split #";+$" (ds/read-lines (s/replace-str "\\ " " " filename)))
	experiment-data (map #(s/split #" = " %) (filter #(re-find #" = " %) (first partition-data)))
	generation-data (filter (fn [g] (su/find-first #(re-find #"-\*-" %) g))
				(rest (butlast partition-data)))
	]
    (cond (= output :csv) (when (write-experiments filename problemname experiment-data)
			    (do (write-experiment filename experiment-data)
				(write-generations generation-data)
				(write-summary (last partition-data))))
	  (= output :mysql) (when (insert-experiments filename problemname experiment-data)
			      (do (insert-experiment filename experiment-data)
				  (insert-generations generation-data)
				  (insert-summary (last partition-data))))
	  :else false)
    ))

(defn clean-csvs [argmap problemname]
  (let [clean (cond (seq? (:clean argmap)) (seq (:clean argmap))
		    (= (:clean argmap) "all") (map str tables)
		    :else (list (:clean argmap)))]
    (doseq [csv clean]
      (try (.delete (File. (str @data_dir csv ".csv")))
	   (catch Exception e)))
    (when (= (:clean argmap) "all")
      (.delete (File. (System/getProperty "user.home") ".db_config")))))

(defn load-into-mysql [argmap problemname]
  (sql/with-connection
    @db
    (sql/transaction (parse-log (:filename argmap) problemname :mysql))))

(defn write-to-csv [argmap problemname]
  (doseq [csv '(experiments experiment generations summary)]
    (let [file (File. (str @data_dir csv ".csv"))]
      (when (not (.exists file))
	(ds/make-parents file)
	(ds/spit file (reduce str (concat (butlast (interleave (map name (from-symbol csv)) (repeat ",")))
					  (list "\n")))))))
  (parse-log (:filename argmap) problemname :csv))

(defn load-other-params-into-mysql [filename index]
  (sql/with-connection
    @db
    (sql/transaction
     (let [uuid (read-string (first (s/split #"\." (last (s/split #"_" filename)))))
	   index (read-string (slurp index))
	   entries (first (filter #(= uuid (:uuid %)) (:command-maps index)))]
       (doseq [[param val] (:argmap entries)]
	 (try
	   (sql/insert-rows :experiment [ @id (name param) val ])
	   true
	   (catch BatchUpdateException b (do (when @debug (or (.printStackTrace b) (println :experiment [ @id (name param) val ]))) false))))))))

(defn add-other-params-to-csv [filename index]
  (let [uuid (read-string (first (s/split #"\." (last (s/split #"_" filename)))))
	index (read-string (slurp index))
	entries (first (filter #(= uuid (:uuid %)) (:command-maps index)))]
    (try
      (ds/with-out-append-writer
	(str @data_dir "experiment.csv")
	(doseq [[param val] (:argmap entries)]
	  (println (str @id ","(name param) ","val))))
      true
      (catch Exception e (do (when @debug (.printStackTrace e)) false)))))

  
(defn load-other-params [filename index target]
  (cond (= target :mysql) (load-other-params-into-mysql filename index)
	(= target :csv) (add-other-params-to-csv filename index)
	:else (throw (Exception. "Unrecognized target."))))

(defn -main [& args]
  (try
    (let [strargs (drop-while #(not= (first %) \:) (map str args))
	  argmap (zipmap (map read-string (filter #(= (first %) \:) strargs))
			 (map #(if (= 1 (count %)) (first %) %)
			      (seq-split #":.*" strargs)))
	  problemname (:problemname argmap "unspecified")
	  mysql-map (when (:mysql argmap) (read-string (reduce #(str %1 " " %2 " ") (:mysql argmap))))
	  {db-name :db-name, user :user, password :password, db-host :db-host, db-port :db-port, skip-nils :skip-nils
	   :as db-data
	   :or {db-name "etoschdb", user "etosch", db-host "fly.hampshire.edu", db-port 3306, skip-nils true}}
	  (when mysql-map
	    (zipmap (map keyword (keys mysql-map))
		    (vals mysql-map)))]
      (when @debug (println "argmap: " argmap "\tdb-data: " db-data))
      (when mysql-map
	(reset! db {:classname "com.mysql.jdbc.Driver"
		    :subprotocol "mysql"
		    :subname (str "//" db-host ":" db-port "/" db-name)
		    :user user
		    :password password})
	(when @debug (println db)))
      (reset! atom-skip-nils skip-nils)
      (when (and (:debug argmap) (read-string (:debug argmap)))
	(reset! debug true))
      (let [config-file-name (str (System/getProperty "user.home") separator ".db_config")]
	(load-config config-file-name)
	(save-config config-file-name problemname))
      (when (empty? argmap)
	(println (:doc (meta *ns*)))
	(System/exit 1))
      ;; Main processing section
      (let [filename (atom (:filename argmap))
	    index (when @filename
		    (let [index-file (str (.getParent (File. @filename)) "/index.clj")]
		      (when (.exists (File. index-file))
			index-file)))
	    extension  (when @filename (last (s/split #"\." @filename)))]
	(when extension
	  (cond (= "log" extension) :uncompressed-log
		(= "gz" extension)  (do (shell/sh "gunzip" @filename)
					(reset! filename (s/join "." (butlast (s/split #"\." @filename)))))
		(= "tgz" extension) (do (shell/sh "tar" "czf" @filename "-C" (.getParent (File. @filename)))
					(reset! filename (s/join "." (butlast (s/split #"\." @filename)))))
		:else (throw (Exception. "Unrecognized file extension"))))
	(cond (:clean argmap) (clean-csvs argmap problemname)
	      (:mysql argmap) (load-into-mysql argmap problemname)
	      :else (write-to-csv argmap problemname))
	(when index (load-other-params @filename index (if (:mysql argmap) :mysql :csv)))
	(when extension
	  (cond (= "gz" extension) (shell/sh "gzip" @filename)
		(= "tgz" extension) (shell/sh "tar" "czf" @filename "-C" (:filename argmap)))))
      (System/exit 0))
    (catch Exception e
      (.printStackTrace e))))


	  ;;(zipmap (map read-string (take-nth 2 strargs))
      ;;		 (take-nth 2 (rest strargs)))]
      ;; (loop [success false]
      ;; 	(when-not success
      ;; 	  (recur (try
	;; 			 channel (.getChannel (RandomAccessFile. (File. config-file-name) "rw"))
	;; 			 lock (.tryLock channel)]
      ;; 		     (.release lock)
      ;; 		     (.close channel))
      ;; 		   true
      ;; 		   (catch Exception e (do (.printStackTrace e) false))))))
      ;; (println "Succeeded in acquiring lock")
