(ns us.chouser.mdm.sqlite-stats
  (:require [clojure.string :as str])
  (:import (java.sql Connection DriverManager ResultSet)
           (java.util.concurrent ExecutorService Executors TimeUnit)))

(set! *warn-on-reflection* true)

(defonce ^:private ^:dynamic *global-collector* nil)

(defonce ^:private *metric-info (atom {}))

(defn register-metrics [m]
  (swap! *metric-info merge m))

(defn record
  ([metric tagmap value]
   (record *global-collector* metric tagmap value))
  ([collector metric tagmap value]
   (let [{:keys [bucket-maxs tag-types]} (get @*metric-info metric)]
     (if-not tag-types
       (println "ERROR: Not recording unregistered metric" (pr-str metric))
       (swap! collector update-in [:values (assoc tagmap :metric metric)]
              (fn [v]
                (if v
                  {:vmin (min value (:vmin v))
                   :vmax (max value (:vmax v))
                   :vsum (+ value (:vsum v))
                   :vcount (inc (:vcount v))}
                  {:vmin value
                   :vmax value
                   :vsum value
                   :vcount 1})))))))

(defn- columns [tag-types]
  (-> [[:time :int]]
      (into tag-types)
      (into (map vector [:vmin :vmax :vsum :vcount] (repeat :int)))))

(defn- metric-table-name [metric]
  (pr-str (str (namespace metric) "/" (name metric))))

(defn- metric-create-table
  [metric-info metric]
  (->> (get metric-info metric) :tag-types columns
       (map (fn [[k t]]
              (str (pr-str (name k)) " " (name t))))
       (str/join ", ")
       (format "create table if not exists %s (%s)"
               (metric-table-name metric))))

(defn- metric-insert-values [now metric-info values]
  (for [[metric pairs] (group-by #(-> % key :metric) values)
        :let [cols (-> metric-info (get metric) :tag-types columns
                       (->> (map first)))]
        [tagmap nums] pairs]
    (format "insert into %s values(%s)"
            (metric-table-name metric)
            (->> cols
                 (map (merge tagmap nums {:time now}))
                 (map #(cond
                         (number? %) (str %)
                         (nil? %) "NULL"
                         :else (pr-str (str %))))
                 (str/join ", ")))))

(defn- write [collector]
  (try
    (let [[state _] (swap-vals! collector assoc :values {})
          {:keys [conn created-tables values period-secs]} state
          new-metrics (->> values
                           (map #(:metric (key %)))
                           (remove created-tables)
                           set)
          now (-> (System/currentTimeMillis)
                  (quot 1000)
                  (quot period-secs) (* period-secs))
          statement (.createStatement ^Connection conn)] ;; TODO Use prepared statements
      (->> new-metrics
           (map #(metric-create-table @*metric-info %))
           (run! #(.executeUpdate statement %)))
      (->> (metric-insert-values now @*metric-info values)
           (run! #(.executeUpdate statement %)))
      (swap! collector update :created-tables into new-metrics))
    (catch Exception ex
      (.printStackTrace ex))))

(defn start [{:keys [filename period-secs]}]
  (assert filename)
  (assert period-secs)
  (let [conn (DriverManager/getConnection (str "jdbc:sqlite:" filename))
        statement (.createStatement conn)
        pool (Executors/newScheduledThreadPool 0)
        collector (atom {:conn conn
                         :pool pool
                         :period-secs period-secs
                         :created-tables #{}})]
    (swap! collector assoc :future
           (.scheduleAtFixedRate pool
                                 #(write collector)
                                 (- period-secs
                                    (rem (quot (System/currentTimeMillis) 1000)
                                         period-secs))
                                 period-secs
                                 TimeUnit/SECONDS))
    collector))

(defn stop [collector]
  (let [pool ^ExecutorService (:pool @collector)]
    (.shutdown pool)
    (.awaitTermination pool 5 TimeUnit/SECONDS)
    (write collector)
    (.close ^Connection (:conn @collector))))

(defn start-global [config]
  (alter-var-root
   #'*global-collector*
   (fn [old]
     (if-not old
       (start config)
       (do
         (println "ERROR: global collector already exists; refusing to start another")
         old)))))

(defn stop-global []
  (alter-var-root
   #'*global-collector*
   (fn [old]
     (if old
       (stop old)
       (println "WARNING: no global collector to stop"))
     nil)))

;; handy for tests:
(defn with-collector* [config f]
  (binding [*global-collector* (start config)]
    (try
      (f)
      (finally
        (stop *global-collector*)))))

(defmacro with-collector [config & body]
  `(with-collector* ~config
     (fn with-collector# [] ~@body)))

(defn result-maps [^ResultSet result-set]
  (let [md (.getMetaData result-set)
        cc (.getColumnCount md)
        names (mapv #(keyword (.getColumnName md (inc %))) (range cc))]
    (->> (repeatedly (fn []
                       (when (.next result-set)
                         (->> (range cc)
                              (mapv #(.getObject result-set (int (inc %))))
                              (zipmap names)))))
         (take-while some?))))

(defn query
  ([q] (query *global-collector* q))
  ([collector q]
   (let [{:keys [conn]} @collector
         s (.createStatement ^Connection conn)]
     (result-maps (.executeQuery s q)))))
