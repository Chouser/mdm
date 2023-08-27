(ns us.chouser.mdm.sqlite-stats
  (:require [clojure.string :as str])
  (:import (java.sql Connection DriverManager ResultSet)
           (java.util.concurrent ExecutorService Executors TimeUnit)))

(set! *warn-on-reflection* true)

(defn record [collector metric tagmap value]
  (swap! collector
         (fn [c]
           (let [{:keys [bucket-maxs tagtypes]} (get-in c [:meta metric])]
             (update-in c [:vals (assoc tagmap :metric metric)]
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

(defn columns [tagtypes]
  (-> [[:time :int]]
      (into tagtypes)
      (into (map vector [:vmin :vmax :vsum :vcount] (repeat :int)))))

(defn metric-create-table
  [[metric {:keys [tagtypes]}]]
  (->> (columns tagtypes)
       (map (fn [[k t]]
              (str (pr-str (name k)) " " (name t))))
       (str/join ", ")
       (format "create table if not exists %s (%s)"
               (pr-str (str metric)))))

(defn metric-insert-values [now meta values]
  (for [[metric pairs] (group-by #(-> % key :metric) values)
        :let [cols (-> meta (get metric) :tagtypes columns
                       (->> (map first)))]
        [tagmap nums] pairs]
    (format "insert into %s values(%s)"
            (pr-str (str metric))
            (->> cols
                 (map (merge tagmap nums {:time now}))
                 (map #(cond
                         (number? %) (str %)
                         (nil? %) "NULL"
                         :else (pr-str (str %))))
                 (str/join ", ")))))

(defn write [collector]
  (let [[{:keys [conn meta vals]} _] (swap-vals! collector assoc :vals {})
        now (quot (System/currentTimeMillis) 1000)
        statement (.createStatement ^Connection conn)] ;; TODO Use prepared statements
    (->> (metric-insert-values now meta vals)
         (run! #(.executeUpdate statement %)))))

(def period (* 5 60))

(defn start [{:keys [filename meta]}]
  (let [conn (DriverManager/getConnection (str "jdbc:sqlite:" filename))
        statement (.createStatement conn)
        pool (Executors/newScheduledThreadPool 0)
        collector (atom {:meta meta
                         :conn conn
                         :pool pool})]
    (->> meta
         (run! #(->> (metric-create-table %)
                     (.executeUpdate statement))))
    (.scheduleAtFixedRate pool
                          #(write collector)
                          (- period (rem (System/currentTimeMillis) period))
                          period
                          TimeUnit/SECONDS)
    collector))

(defn stop [collector]
  (let [pool ^ExecutorService (:pool @collector)]
    (.shutdown pool)
    (.awaitTermination pool 5 TimeUnit/SECONDS)
    (.close ^Connection (:conn @collector))))

#_
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
