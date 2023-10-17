(ns us.chouser.mdm
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [us.chouser.mdm.jab :as jab]
            [us.chouser.mdm.mqtt :as mqtt]
            [us.chouser.mdm.sqlite-stats :as s.stats]))

(set! *warn-on-reflection* true)

(def version "1.7")

(def get-secret
  (let [[filename] (->> ["secrets.edn" "secrets-test.edn"]
                        (filter #(.exists (io/file %))))]
    (println "Using secrets:" (pr-str filename))
    (partial get (edn/read-string (slurp filename)))))

(def signal-alert-ms (* 660 1000))
(def alert-reminder-ms (* 900 1000))
(def fudge-ms (* 5 1000))

(def alert-topics #{"Weight" "Pressure"})

(defn ms-str [ms]
  (let [s (/ ms 1000.0)]
    (cond
      (< s   100) (format "%d seconds" s)
      (< s   597) (format "%.1f minutes" (/ s 60.0))
      (< s  5970) (format "%.0f minutes" (/ s 60.0))
      (< s 35820) (format "%.1f hours"   (/ s 3600.0))
      :else       (format "%.0f hours"   (/ s 3600.0)))))

(defn check-state
  "Pure function to decide what alert msg to send if any. Return updated
  state."
  [{:keys [alerted?] :as state} now-ts]
  (let [[[_ oldest-signal-ts] :as topic-tss]
        , (->> alert-topics
               (map (partial find (:topic-ts state)))
               (sort-by val))
        alerted-topics
        , (->> topic-tss
               (take-while #(< signal-alert-ms
                               (- now-ts (val %))))
               (map #(format "%s (%s ago)"
                             (key %)
                             (ms-str (- now-ts (val %)))))
               (str/join ", "))
        last-msg-ts (:last-msg-ts state now-ts)
        over-age-threshold? (<= signal-alert-ms
                                (- now-ts oldest-signal-ts))
        msg (if over-age-threshold?
              (if-not alerted?
                (str "Alert! " alerted-topics)
                (when (<= alert-reminder-ms (- now-ts last-msg-ts))
                  (str "Still alerted. " alerted-topics)))
              (when alerted?
                "Cleared alert."))]
    (merge state
           {:alerted? over-age-threshold?
            :alert-msg msg
            :check-state-ts (+ fudge-ms
                               (if over-age-threshold?
                                 (+ last-msg-ts alert-reminder-ms)
                                 (+ oldest-signal-ts signal-alert-ms)))}
           (when msg
             {:last-msg-ts now-ts}))))

(defn send-contact-text [conn contact text]
  (let [{:keys [user-address muc-address password]}
        , (-> (get-secret :contacts) (get contact))]
    (cond
      muc-address
      , (-> (jab/join-muc conn
                          {:address muc-address
                           :password password
                           :nickname (get-secret :bot-name)})
            (jab/send-muc text))
      user-address
      , (jab/send-chat conn user-address text)
      :else (println "ERROR: Bad contact:" (pr-str contact)))))

(defn send-group-text [target text]
  (let [cts (-> (get-secret :text-targets) (get target))]
    (if-not cts
      (println "ERROR: Bad text target:" (pr-str target))
      (if-let [pw (get-secret :xmpp-password)]
        (let [conn (jab/connect {:host "xabber.org"
                                 :username "ottowarburg"
                                 :password pw})]
          (println target "TEXT:" text)
          (try
            (->> cts
                 (run! #(send-contact-text conn % text)))
            (finally (jab/disconnect conn))))
        (println target "TEST TEXT:" text)))))

(defn send-m []
  (if-let [pw (get-secret :xmpp-password)]
    (let [conn (jab/connect {:host "xabber.org"
                             :username "ottowarburg"
                             :password pw})]
      (prn :conn conn)
      (try
        (jab/send-chat conn "chouser@xabber.org" "ok")
        (finally (jab/disconnect conn))))))

(defn reschedule! [sys f k secs]
  (some-> @sys ^java.util.concurrent.Future (get-in [:futures k]) (.cancel false))
  (swap! sys assoc-in [:futures k]
         (.schedule ^java.util.concurrent.ScheduledExecutorService (:scheduler @sys)
                    ^Runnable (partial f sys)
                    (long secs)
                    java.util.concurrent.TimeUnit/SECONDS)))

(defn alert-as-needed [sys]
  (let [[old-sys new-sys]
        , (swap-vals! sys update :state
                      check-state
                      (System/currentTimeMillis))]
    (when (not= (-> old-sys :state :check-state-ts)
                (-> new-sys :state :check-state-ts))
      (reschedule! sys #'alert-as-needed :alert-as-needed
                   (quot (- (-> new-sys :state :check-state-ts)
                            (System/currentTimeMillis))
                         1000)))
    (when-let [msg (-> new-sys :state :alert-msg)]
      (send-group-text (if (re-find #"Still" msg) :reminder :alert)
                       msg))))

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
     :status
     (if-not (:stat-start old)
       (str "Started and connected version " version)
       (format "Over the last %s, I've seen %s. This is version %s"
               (ms-str (- now (:stat-start old)))
               (->> old :metric
                    (map (fn [[topic n]]
                           (format "%d %s signals" n topic)))
                    (str/join ", "))
               version)))
    (reschedule! sys #'daily-report :daily (seconds-til-daily-report now))))

(s.stats/register-metrics
 {::value    {:tag-types {:topic :string}}
  ::interval {:tag-types {:topic :string}}
  ::reboot   {:tag-types {}}})

(defn on-mqtt-msg [sys {:keys [topic msg]}]
  (when-not (:stop? @sys)
    (try
      (let [now (System/currentTimeMillis)
            [prev-sys _]
            , (swap-vals! sys
                          #(-> %
                               (update-in [:metric topic] (fnil inc 0))
                               (assoc-in [:state :topic-ts topic] now)))]
        (when-let [prev (-> prev-sys :state :topic-ts (get topic))]
          (s.stats/record ::interval {:topic topic} (- now prev)))
        (when (re-matches #"[-.\d]+" msg)
          (s.stats/record ::value {:topic topic} (Double/parseDouble msg)))
        (when (= "BoinkLog" topic)
          (when-let [bootings (re-seq #"Booting" msg)]
            (s.stats/record ::reboot {} (count bootings))))
        (future (alert-as-needed sys)))
      (catch Exception ex
        (prn :on-mqtt-msg ex)))))

(defn start []
  (s.stats/start-global {:filename (get-secret :metrics-filename)
                         :period-secs (get-secret :metrics-period-secs (* 5 60))})
  (doto (atom {:state {:topic-ts (->> (map vector
                                           alert-topics
                                           (repeat (System/currentTimeMillis)))
                                      (into {}))}})
    ((fn [sys]
       (swap! sys assoc
              :mqtt-client (mqtt/start {:address (get-secret :mqtt-broker)
                                        :client-id (get-secret :bot-name)
                                        :subs [{:topic "#"
                                                :msg-fn #(on-mqtt-msg sys %)}]})
              :scheduler (java.util.concurrent.Executors/newScheduledThreadPool 0))))
    (alert-as-needed)
    (daily-report)))

(defn stop [sys]
  (swap! sys assoc :stop? true)
  (->> @sys :futures vals
       (run! #(.cancel ^java.util.concurrent.Future % false)))
  (mqtt/stop (:mqtt-client @sys))
  (when-let [s ^java.util.concurrent.ExecutorService (:scheduler sys)]
    (.shutdown s)
    (.awaitTermination s 5 java.util.concurrent.TimeUnit/SECONDS))
  (s.stats/stop-global))

(defn -main []
  (def sys
    (start)))

(comment

  (def sys (start))
  (stop sys)

  )
