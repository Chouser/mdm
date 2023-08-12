(ns us.chouser.mdm
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [us.chouser.mdm.jab :as jab]
            [us.chouser.mdm.mqtt :as mqtt]))

(set! *warn-on-reflection* true)

(def bot-name "Otto")
(def version "1.0")

(def get-secret
  (partial get (edn/read-string (slurp "secrets.edn"))))

(def pressure-alert-secs 300)
(def weight-alert-secs 300)
(def fudge-secs 5)

(defn time-str [seconds]
  (str seconds " seconds"))

(defn check-state
  "Pure function to decide what alert message to send if any. Return updated
  state."
  [state now]
  (let [p-since (quot (- now (:pressure-ts state now)) 1000)
        w-since (quot (- now (:weight-ts state now)) 1000)
        branch [(if (:pressure-alerted? state) 1 0)
                (if (:weight-alerted? state) 1 0)
                (if (< pressure-alert-secs p-since) 1 0)
                (if (< weight-alert-secs w-since) 1 0)]]
    (assoc
     state
     :pressure-alerted? (< pressure-alert-secs p-since)
     :weight-alerted? (< weight-alert-secs w-since)
     :alert-msg
     , (case branch
         [0 0 0 0] nil
         [0 0 0 1] (str "Alert! No Weight signal received for " (time-str w-since) ".")
         [0 0 1 0] (str "Alert! No Pressure signal received for " (time-str p-since) ".")
         [0 0 1 1] (str "Alert! No Pressure signal received for " (time-str p-since)
                        ", and no Weight signal for " (time-str w-since) ".")
         [0 1 0 0] "Cleared all alerts: Weight signal received."
         [0 1 0 1] nil
         [0 1 1 0] (str "Alert! No Pressure signal received for " (time-str p-since)
                        ". But weirdly a Weight signal was just received, "
                        "so that alert is cleared.")
         [0 1 1 1] (str "Another alert! No Pressure signal received for " (time-str p-since)
                        ", in addition to the Weight alert.")
         [1 0 0 0] "Cleared all alerts: Pressure signal received."
         [1 0 0 1] (str "Alert! No Weight signal received for " (time-str w-since)
                        ". But weirdly a Pressure signal was just received, "
                        "so that alert is cleared.")
         [1 0 1 0] nil
         [1 0 1 1] (str "Another alert! No Weight signal received for " (time-str w-since)
                        ", in addition to the Pressure alert.")
         [1 1 0 0] "Cleared all alerts: Weight and Pressure signals received."
         [1 1 0 1] "Cleared one alert: Pressure signal received; still waiting for a Weight signal."
         [1 1 1 0] "Cleared one alert: Weight signal received; still waiting for a Pressure signal."
         [1 1 1 1] nil))))

(defn send-group-text [text]
  (let [conn (jab/connect {:host "xabber.org"
                           :username "chouser"
                           :password (get-secret :xmpp-password)})]
    (try
      (-> (jab/join-muc conn
                        {:address (get-secret :muc-address)
                         :nickname bot-name
                         :password (get-secret :muc-password)})
          (jab/send-muc text))
      (finally (jab/disconnect conn)))))

(defn send-group-text [text]
  (println "TEXT:" text))

(defn alert-as-needed [sys]
  (let [{:keys [state]}
        , (swap! sys update :state
                 check-state
                 (System/currentTimeMillis))
        msg (:alert-msg state)]
    (some-> msg send-group-text)))

(defn reschedule! [sys f k secs]
  (some-> @sys ^java.util.concurrent.Future (get-in [:futures k]) (.cancel false))
  (swap! sys assoc-in [:futures k]
         (.schedule ^java.util.concurrent.ScheduledExecutorService (:scheduler @sys)
                    ^Runnable (partial f sys)
                    (long secs)
                    java.util.concurrent.TimeUnit/SECONDS)))

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
     (if-not (:stat-start old)
       (str "Started and connected version " version)
       (format "Over the last %.1f hours, I've seen %s. This is version %s"
               (-> now (- (:stat-start old)) (quot 1000) (quot 60) (/ 60.0))
               (->> old :metric
                    (map (fn [[topic n]]
                           (format "%d %s signals" n topic)))
                    (str/join ", "))
               version)))
    (reschedule! sys #'daily-report :daily (seconds-til-daily-report now))))

(defn on-mqtt-msg [sys {:keys [topic msg]}]
  (when-not (:stop? @sys)
    (try
      (let [now (System/currentTimeMillis)]
        (swap! sys update-in [:metric topic] (fnil inc 0))
        (case topic
          "Pressure" (do
                       (swap! sys assoc-in [:state :pressure-ts] now)
                       (reschedule! sys alert-as-needed :pressure (+ fudge-secs pressure-alert-secs)))
          "Weight" (do
                     (swap! sys assoc-in [:state :weight-ts] now)
                     (reschedule! sys alert-as-needed :weight (+ fudge-secs weight-alert-secs)))
          :ignore)
        (alert-as-needed sys))
      (catch Exception ex
        (prn ex)))))

(defn start []
  (doto (atom {})
    (swap! assoc
           :mqtt-client (mqtt/connect {:address (get-secret :mqtt-broker)
                                       :client-id "Otto"})
           :scheduler (java.util.concurrent.Executors/newScheduledThreadPool 0))
    (as-> sys
        (swap! sys assoc :mqtt-sub
               (mqtt/subscribe (:mqtt-client @sys) {:topic "#" :msg-fn (partial #'on-mqtt-msg sys)})))
    (daily-report)))

(defn stop [sys]
  (swap! sys assoc :stop? true)
  (->> @sys :futures vals
       (run! #(.cancel ^java.util.concurrent.Future % false)))
  (mqtt/disconnect (:mqtt-client @sys)))

(defn -main []
  (def sys
    (start)))

(comment

  (def sys (start))
  (stop sys)

  (def xmpp-conn
    (jab/connect {:host "xabber.org"
                  :username "chouser"
                  :password (get-secret :xmpp-password)}))

  (def muc
    (jab/join-muc xmpp-conn
                  {:address (get-secret :muc-address)
                   :nickname "Christopher"
                   :password (get-secret :muc-password)}))

  (jab/send-muc muc "Thanks. No big rush, though...")

  (jab/disconnect xmpp-conn)

  )
