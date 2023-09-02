(ns us.chouser.mdm
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [us.chouser.mdm.jab :as jab]
            [us.chouser.mdm.mqtt :as mqtt]
            [us.chouser.mdm.sqlite-stats :as s.stats]))

(set! *warn-on-reflection* true)

(def version "1.4")

(def get-secret
  (let [[filename] (->> ["secrets.edn" "secrets-test.edn"]
                        (filter #(.exists (io/file %))))]
    (println "Using secrets:" (pr-str filename))
    (partial get (edn/read-string (slurp filename)))))

(def pressure-alert-secs 660)
(def weight-alert-secs 660)
(def alert-reminder-secs 900)
(def fudge-secs 5)

(defn time-str [seconds]
  (cond
    (< seconds   100) (str seconds " seconds")
    (< seconds   597) (format "%.1f minutes" (/ seconds 60.0))
    (< seconds  5970) (format "%.0f minutes" (/ seconds 60.0))
    (< seconds 35820) (format "%.1f hours"   (/ seconds 3600.0))
    :else             (format "%.0f hours"   (/ seconds 3600.0))))

(defn check-state
  "Pure function to decide what alert message to send if any. Return updated
  state."
  [state now]
  (let [sfx (get-secret :alert-suffix)
        p-since (quot (- now (:pressure-ts state now)) 1000)
        w-since (quot (- now (:weight-ts state now)) 1000)
        m-since (quot (- now (:msg-ts state now)) 1000)
        branch [(if (:pressure-alerted? state) 1 0)
                (if (:weight-alerted? state) 1 0)
                (if (< pressure-alert-secs p-since) 1 0)
                (if (< weight-alert-secs w-since) 1 0)]
        msg
        , (case branch
            [0 0 0 0] nil
            [0 0 0 1] (str "Alert! No Weight signal received for " (time-str w-since) "." sfx)
            [0 0 1 0] (str "Alert! No Pressure signal received for " (time-str p-since) "." sfx)
            [0 0 1 1] (str "Alert! No Pressure signal received for " (time-str p-since)
                           ", and no Weight signal for " (time-str w-since) "." sfx)
            [0 1 0 0] "Cleared all alerts: Weight signal received."
            [0 1 0 1] nil
            [0 1 1 0] (str "Alert! No Pressure signal received for " (time-str p-since)
                           ". But weirdly a Weight signal was just received, "
                           "so that alert is cleared." sfx)
            [0 1 1 1] (str "Another alert! No Pressure signal received for " (time-str p-since)
                           ", in addition to the Weight alert." sfx)
            [1 0 0 0] "Cleared all alerts: Pressure signal received."
            [1 0 0 1] (str "Alert! No Weight signal received for " (time-str w-since)
                           ". But weirdly a Pressure signal was just received, "
                           "so that alert is cleared." sfx)
            [1 0 1 0] nil
            [1 0 1 1] (str "Another alert! No Weight signal received for " (time-str w-since)
                           ", in addition to the Pressure alert." sfx)
            [1 1 0 0] "Cleared all alerts: Weight and Pressure signals received."
            [1 1 0 1] "Cleared one alert: Pressure signal received; still waiting for a Weight signal."
            [1 1 1 0] "Cleared one alert: Weight signal received; still waiting for a Pressure signal."
            [1 1 1 1] nil)
        msg (or msg
                (when (and (< alert-reminder-secs m-since)
                           (or (:pressure-alerted? state)
                               (:weight-alerted? state)))
                  (str "Still alerted"
                       (when (:pressure-alerted? state)
                         (str ", " (time-str p-since) " since last Pressure signal"))
                       (when (:weight-alerted? state)
                         (str ", " (time-str w-since) " since last Weight signal"))
                       "." sfx)))]
    (assoc state
           :pressure-alerted? (< pressure-alert-secs p-since)
           :weight-alerted? (< weight-alert-secs w-since)
           :msg-ts (if msg now (:msg-ts state now))
           :alert-msg msg)))

(defn send-group-text [muc-key text]
  (let [muc (-> (get-secret :mucs) (get muc-key))]
    (when-not muc
      (throw (ex-info (str "Bad muc key: " (pr-str muc-key))
                      {:muc-key muc-key :mucs (keys (get-secret :mucs))})))
    (if-let [pw (get-secret :xmpp-password)]
      (let [conn (jab/connect {:host "xabber.org"
                               :username "ottowarburg"
                               :password pw})]
        (println muc-key "TEXT:" text)
        (try
          (-> (jab/join-muc conn
                            {:address (:address muc)
                             :password (:password muc)
                             :nickname (get-secret :bot-name)})
              (jab/send-muc text))
          (finally (jab/disconnect conn))))
      (println muc-key "TEST TEXT:" text))))

(defn alert-as-needed [sys]
  (let [{:keys [state]}
        , (swap! sys update :state
                 check-state
                 (System/currentTimeMillis))
        msg (:alert-msg state)]
    (when msg
      (send-group-text (if (re-find #"Still" msg) :general :tech)
                       msg))))

(defn reschedule! [sys f k secs]
  (some-> @sys ^java.util.concurrent.Future (get-in [:futures k]) (.cancel false))
  (swap! sys assoc-in [:futures k]
         (.schedule ^java.util.concurrent.ScheduledExecutorService (:scheduler @sys)
                    ^Runnable (partial f sys)
                    (long secs)
                    java.util.concurrent.TimeUnit/SECONDS)))

(defn alert-reminder [sys]
  (alert-as-needed sys)
  (reschedule! sys #'alert-reminder :alert-reminder (+ fudge-secs alert-reminder-secs)))

(defn seconds-til-daily-report [now-millis]
  (let [target-time (java.time.LocalTime/of 15 0) ;; 3pm local
        instant (java.time.Instant/ofEpochMilli now-millis)
        zone (java.time.ZoneId/systemDefault)
        today-3pm (.with (java.time.LocalDateTime/ofInstant instant zone) target-time)
        today-seconds (-> today-3pm
                          (.atZone zone)
                          .toInstant
                          (->> (java.time.Duration/between instant))
                          .getSeconds)]
    (if (< today-seconds 100)
      (+ today-seconds (* 24 60 60))
      today-seconds)))

(defn daily-report [sys]
  (let [now (System/currentTimeMillis)
        [old _] (swap-vals! sys #(-> %
                                     (assoc :stat-start now)
                                     (dissoc :metric)))]
    (send-group-text
     :tech
     (if-not (:stat-start old)
       (str "Started and connected version " version)
       (format "Over the last %s, I've seen %s. This is version %s"
               (time-str (/ (- now (:stat-start old)) 1000.0))
               (->> old :metric
                    (map (fn [[topic n]]
                           (format "%d %s signals" n topic)))
                    (str/join ", "))
               version)))
    (reschedule! sys #'daily-report :daily (seconds-til-daily-report now))))

(defn on-mqtt-msg [sys {:keys [topic msg]}]
  (when-not (:stop? @sys)
    (try
      (let [now (System/currentTimeMillis)
            prev-state (:state @sys)
            collector (:collector @sys)]
        (swap! sys update-in [:metric topic] (fnil inc 0))
        (case topic
          "Pressure" (do
                       (swap! sys assoc-in [:state :pressure-ts] now)
                       (when (re-matches #"[-.\d]+" msg)
                         (s.stats/record collector `value
                                         {:topic topic} (Double/parseDouble msg)))
                       (when-let [prev (:pressure-ts prev-state)]
                         (s.stats/record collector `interval {:topic topic} (- now prev)))
                       (reschedule! sys #'alert-as-needed :pressure (+ fudge-secs pressure-alert-secs)))
          "Weight" (do
                     (swap! sys assoc-in [:state :weight-ts] now)
                     (when (re-matches #"[-.\d]+" msg)
                       (s.stats/record collector `value
                                       {:topic topic} (Double/parseDouble msg)))
                     (when-let [prev (:weight-ts prev-state)]
                       (s.stats/record collector `interval {:topic topic} (- now prev)))
                     (reschedule! sys #'alert-as-needed :weight (+ fudge-secs weight-alert-secs)))
          :ignore)
        (future (alert-as-needed sys)))
      (catch Exception ex
        (prn :on-mqtt-msg ex)))))

(defn start []
  (doto (atom {})
    ((fn [sys]
       (swap! sys assoc
              :collector (s.stats/start
                          {:filename (get-secret :metrics-filename)
                           :meta {`value    {:tagtypes {:topic :int}}
                                  `interval {:tagtypes {:topic :int}}}})
              :mqtt-client (mqtt/start {:address (get-secret :mqtt-broker)
                                        :client-id (get-secret :bot-name)
                                        :subs [{:topic "#"
                                                :msg-fn #(on-mqtt-msg sys %)}]})
              :scheduler (java.util.concurrent.Executors/newScheduledThreadPool 0))))
    (alert-reminder)
    (daily-report)))

(defn stop [sys]
  (swap! sys assoc :stop? true)
  (->> @sys :futures vals
       (run! #(.cancel ^java.util.concurrent.Future % false)))
  (mqtt/stop (:mqtt-client @sys))
  (when-let [s ^java.util.concurrent.ExecutorService (:scheduler sys)]
    (.shutdown s)
    (.awaitTermination s 5 java.util.concurrent.TimeUnit/SECONDS)))

(defn -main []
  (def sys
    (start)))

(comment

  (def sys (start))
  (stop sys)

  )
