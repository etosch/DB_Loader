(ns parse-logs-ecj
  (:require [clojure.string :as s]
	    [clojure.java.jdbc :as jdbc]
	    )
  (:import [java.io BufferedReader FileReader File]
	   [java.util Date]
	   [java.text SimpleDateFormat]
	   [org.postgresql.Driver]))

;; Mar 28 09:53 binomial
;; Apr  1 13:35 binomial-500
;;  Mar 26 12:35 output
;; drwxr-sr-x 5 etosch lspector     4096 Mar 26 05:58 output0
;; drwxr-sr-x 5 etosch lspector     4096 Mar 26 05:58 output1
;; drwxr-sr-x 5 etosch lspector     4096 Mar 26 06:00 output12
;; drwxr-sr-x 5 etosch lspector     4096 Mar 26 05:59 output8

;; load data from /mnt/work1/lspector/etosch/binomial
;; insert into experiments
;; (userid, probid, locid, batchdate)
;; select u.userid, p.probid, l.locid, timestamp '2012-03-28 09:53'
;; from users u, problems p, locations l
;; where u.username='etosch'
;; and p.probname='binomial-3'
;; and l.location='swarm.cs.umass.edu'

(def output-folder (atom "/mnt/work1/lspector/etosch/binomial"))
(def parameter-folder (atom "/home2/etosch/ecj/ec/app/regression"))
(def db {:subprotocol "postgresql"
	 :classname "org.postgresql.Driver"
	 :subname "//bbb.hampshire.edu:5432/etosch"
	 :user "etosch"
	 :password "ohRuek1A"})
(def location "swarm.cs.umass.edu")

;; batch=set of experiments/trials
(defn new-batch
  ^{:doc "Creates a new entry in the EXPERIMENTS table for this folder and returns the batchid. If a batchid already exists for this experiment, it returns that id."}
  [folder user]
  (jdbc/with-connection db
    (let [userid (jdbc/with-query-results res ["select userid from users where username='?'" user]
		   ((first res) :userid))
	  probid (jdbc/with-query-results res [(str "select probid from problems where probname like '%"
						    (last (s/split folder #"/"))
						    "%'")]
		   ((first res) :probid))
	  locid (jdbc/with-query-results res ["select locid from locations where location='?'" location]
		  ((first res) :locid))
	  batchdate (.format (SimpleDateFormat. "yyyy-MM-dd HH:mm")
			     (Date. (.lastModified (File. folder))))
	  query (str "select batchid from experiments where userid=" userid
		     " and probid=" probid " and locid=" locid " and batchdate=" batchdate)
	  batchid (jdbc/with-connection db
		    (jdbc/with-query-results res [query]
		      ((first res) :batchid)))
	  ]
      ;; first check to see if this experiment has already been added
      (or batchid (do (jdbc/insert-values :experiments [:userid :probid :locid :batchdate] [userid probid locid batchdate])
		      (jdbc/with-query-results res [query]
			((first res) :batchid)))))))

(defn exp-param-vals
  ^{:doc "Extracts key-value pairs from the experiment parameter file"}
  [filename]
  (s/split-lines (slurp filename)
  
(defn new-exp
  ^{:doc "Creates a new entry in the EXPERIMENT table for this parameter file and batchid and returns the expid. If an entry already exists, it throws an error stating the number of rows in the EXPERIMENT and GENERATIONS table having this id."}
    [filename batchid]
    (jdbc/with-connection db
      (let [[expid, ct] (jdbc/with-query-results res ["select expid from experiment where batchid=? and logname='?'" batchid filename]
			  [((first res) :expid) (count res)])
	    gens (jdbc/with-query-results res ["select count(distinct genid) as gens from generations where expid=?" expid]
		   ((first res) :gens))
	    ]
	(if expid
	  (throw (Exception. (str "Trial " filename " already has id " expid " with " ct " entries in EXPERIMENT  and " gens " entries in GENERATIONS")))
	  (let [expid (jdbc/with-query-results res ["select nextval('exp_seq')"]
			((first res) :nextval))]
	    (doseq [[param value] (exp-param-vals filename)]
	      (jdbc/insert-values :experiment [:expid :logname :batchid :param :value] [expid filename batchid param value]))
	    expid)))))

(defn load-exp
  [filename batchid]
  


(defn read-gen
  ^{:doc "Reads in a file and returns a list of lists. The number of items in the list is the number of generations. Each generation is a list of the individual lines comprising that generation."}
  [filename]
  (with-open [reader (BufferedReader. (FileReader. filename))]
    (loop [line (.readLine reader)
	   gens '()
	   build '()]
      (cond (not line)
	      (rest (reverse (map reverse (cons build gens))))
	    (or (re-find #"Generation:" line)
		(re-find #"of Run:" line))
	      (recur (.readLine reader)
		     (cons build gens)
		     (list line))
	    :else (recur (.readLine reader)
			 gens
			 (cons line build))))))

(defn group-gen
  ^{:doc "Loops through a given generation and collects values that may have overflowed onto new lines"}
  [single-gen]
  (loop [gs single-gen
	 this '()]
    (cond (empty? gs) (reverse this)
	  (re-find #":" (first gs)) (recur (rest gs)
					   (cons (first gs) this))
	  :else (recur (rest gs)
		       (cons (str (first this) " " (s/trim (first gs))) (rest this))))))

(defn clean-data
  ^{:doc "Returns a vector of hashmaps of parameters and values per generation."}
  [single-gen]
  (map #(let [[k v] (s/split % #":")]
	  (vector (s/replace k #"\s+" "") (when v (s/trim v))))
       single-gen))

