(ns us.chouser.mdm
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.stacktrace :refer [print-cause-trace]]
            [us.chouser.mdm.chat :as chat]
            [us.chouser.mdm.jab :as jab]
            [us.chouser.mdm.mqtt :as mqtt]
            [us.chouser.mdm.sqlite-stats :as s.stats]))

(set! *warn-on-reflection* true)

(def version "1.11")

(def get-secret
  (let [[filename] (->> ["secrets-test.edn" "secrets.edn"]
                        (filter #(.exists (io/file %))))]
    (println "Using secrets:" (pr-str filename))
    (partial get (edn/read-string (slurp filename)))))

(def signal-alert-ms (* 660 1000))
(def alert-reminder-ms (* 900 1000))
(def fudge-ms (* 5 1000))

(def alert-topics #{"Weight" "Pressure" "BoinkT"})

(def bot-name-ptn
  (re-pattern (str "(?i)\\b\\Q" (get-secret :bot-name) "\\E\\b")))

(defn ms-str [ms]
  (let [s (/ ms 1000.0)]
    (cond
      (< s   100) (format "%.1f seconds" s)
      (< s   597) (format "%.1f minutes" (/ s 60.0))
      (< s  5970) (format "%.0f minutes" (/ s 60.0))
      (< s 35820) (format "%.1f hours"   (/ s 3600.0))
      :else       (format "%.0f hours"   (/ s 3600.0)))))

(defn calc-suppression-end-ts [suppressions now-ts]
  (let [now-str (chat/format-ts now-ts)]
    (->> suppressions
         (some (fn [[start end]]
                 (when (and (<= (compare start now-str) 0)
                            (< (compare now-str end) 0))
                   (chat/parse-to-ts end)))))))

(defn check-state
  "Pure function to decide what alert msg to send if any. Return updated
  state."
  [{:keys [alerted?] :as state} now-ts suppressions]
  (let [[{oldest-signal-ts :ts} :as topic-tss]
        , (->> alert-topics
               (map (fn [topic]
                      {:topic topic
                       :ts (get (:topic-ts state) topic 0)}))
               (sort-by :ts))
        alerted-topics
        , (->> topic-tss
               (take-while #(< signal-alert-ms
                               (- now-ts (:ts %))))
               (map #(format "%s (%s ago)"
                             (:topic %)
                             (ms-str (- now-ts (:ts %)))))
               (str/join ", "))
        last-msg-ts (:last-msg-ts state now-ts)
        over-age-threshold? (<= signal-alert-ms
                                (- now-ts oldest-signal-ts))
        suppression-end-ts (calc-suppression-end-ts suppressions now-ts)
        ;;_ (prn :age (- now-ts oldest-signal-ts) topic-tss alerted-topics)
        msg (when-not suppression-end-ts
              (if over-age-threshold?
                (if-not alerted?
                  (str "Alert! " alerted-topics)
                  (when (<= alert-reminder-ms (- now-ts last-msg-ts))
                    (str "Still alerted. " alerted-topics)))
                (when alerted?
                  "Cleared alert.")))
        check-state-ts (min (+ now-ts 60000)
                            (+ fudge-ms
                               (or suppression-end-ts
                                   (if over-age-threshold?
                                     (+ now-ts alert-reminder-ms)
                                     (+ oldest-signal-ts signal-alert-ms)))))]
    (when (< (- check-state-ts now-ts) 4000)
      (println "WARNING: check-state ts very soon:" (- check-state-ts now-ts)))
    (merge state
           {:alerted? over-age-threshold?
            :alert-msg msg
            :check-state-ts check-state-ts}
           (when msg
             {:last-msg-ts now-ts}))))

(defn send-group-text [jab target text]
  (let [cts (-> (get-secret :text-targets) (get target))]
    (if-not cts
      (println "ERROR: Bad text target:" (pr-str target))
      (->> cts
           (run! #(let [{:keys [user-address muc-address]}
                        , (-> (get-secret :contacts) (get %))]
                    (cond
                      muc-address
                      , (let [muc (get (:mucs @jab) muc-address)]
                          (jab/send-muc muc text))
                      user-address
                      , (jab/send-chat (:conn @jab) user-address text)
                      :else (println "ERROR: Bad contact:" (pr-str %)))))))))

(defn reschedule! [sys f k secs]
  (some-> @sys ^java.util.concurrent.Future (get-in [:futures k]) (.cancel false))
  (swap! sys assoc-in [:futures k]
         (.schedule ^java.util.concurrent.ScheduledExecutorService (:scheduler @sys)
                    ^Runnable (partial f sys)
                    (long secs)
                    java.util.concurrent.TimeUnit/SECONDS)))

(defn alert-as-needed [sys]
  (try
    (let [[old-sys new-sys]
          , (swap-vals! sys update :state
                        check-state
                        (System/currentTimeMillis)
                        (-> @sys :jab deref :chat-state deref :suppressions))]
      (when (not= (-> old-sys :state :check-state-ts)
                  (-> new-sys :state :check-state-ts))
        (let [check-in-secs (quot (- (-> new-sys :state :check-state-ts)
                                     (System/currentTimeMillis))
                                  1000)]
          (reschedule! sys #'alert-as-needed :alert-as-needed check-in-secs)))
      (when-let [msg (-> new-sys :state :alert-msg)]
        (send-group-text (:jab new-sys)
                         (if (re-find #"Still" msg) :reminder :alert)
                         msg)))
    (catch Throwable ex
      (print-cause-trace ex)
      (throw ex))))

(defn seconds-til-daily-report [now-millis]
  ;; Note this doesn't do the right thing across DST changes:
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
     (:jab @sys)
     :status
     (if-not (:stat-start old)
       (str "Started and connected version " version)
       (format "Over the last %s, I've seen %s. This is version %s"
               (ms-str (- now (:stat-start old)))
               (->> old :metric
                    (sort-by #(- (val %)))
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
      (when-not (= msg "unstable")
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
          (future (alert-as-needed sys))))
      (catch Exception ex
        (prn :on-mqtt-msg ex)))))

(defn on-jabber-msg [{:keys [chat-log] :as chat-state}
                     jab
                     {:keys [id body from] :as msg}]
  (if (or (nil? body)
          (not (#{:chat :groupchat} (:type msg))) ;; maybe an :error msg?
          (some #(= (:id %) id) chat-log) ;; already processed this chat-id
          (and (= :groupchat (:type msg)) ;; groupchat without mentioning Otto
               (not (re-find bot-name-ptn body))))
    (do (prn :not-for-me msg)
        chat-state)
    (let [[_ muc-addr from-nick] (re-matches #"([^/]*)/(.*)" from)
          {:keys [send-chat] :as new-chat-state}
          , (chat/apply-chat-str! chat-state (chat/format-time (chat/now)) body id)
          new-chat-state (dissoc new-chat-state :send-chat)]
      (prn :sending send-chat)
      (if (= :groupchat (:type msg))
        (let [muc (get (:mucs @jab) muc-addr)]
          (jab/send-muc muc send-chat))
        (jab/send-chat (:conn @jab) from send-chat))
      (spit (get-secret :chat-state-file)
            (prn-str new-chat-state))
      new-chat-state)))

(defn jabber-start []
  (let [jab (atom {:conn nil
                   :chat-state (agent (try
                                        (read-string (slurp (get-secret :chat-state-file)))
                                        (catch Exception ex
                                          chat/init-state))
                                      :error-handler #(println "chat agent error" (pr-str %&)))})
        conn (-> {:host "xabber.org"
                  :username "ottowarburg"
                  :password (get-secret :xmpp-password)
                  :on-message #(send-off (:chat-state @jab) on-jabber-msg jab %)
                  :on-connect (fn [conn]
                                (->> (get-secret :contacts)
                                     vals
                                     (filter :muc-address)
                                     (map (fn [{:keys [muc-address password]}]
                                            (prn :join-muc muc-address)
                                            [muc-address
                                             (->> {:address muc-address
                                                   :password password
                                                   :nickname (get-secret :bot-name)}
                                                  (jab/join-muc conn))]))
                                     (into {})
                                     (swap! jab assoc :mucs)))}
                 jab/connect)]
    (swap! jab assoc :conn conn)
    jab))

(defn jabber-stop [jab]
  (jab/disconnect (:conn @jab)))

(defn start []
  (s.stats/start-global {:filename (get-secret :metrics-filename)
                         :period-secs (get-secret :metrics-period-secs (* 5 60))})
  (doto (atom {:state {:topic-ts (->> (map vector
                                           alert-topics
                                           (repeat (System/currentTimeMillis)))
                                      (into {}))}})
    (swap! assoc :jab (jabber-start))
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
  (jabber-stop (:jab @sys))
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

  (defn on-msg [x]
    (prn :msg x))

  (def conn
    (-> {:host "xabber.org"
         :username "ottowarburg"
         :password (get-secret :xmpp-password)
          :on-message #'on-msg}
        jab/connect))

  (def muc
    (->> {:address "gptest@conference.xabber.org"
          :password "oinkoink"
          :nickname "mybot"}
         (jab/join-muc conn)))

  (jab/send-muc muc "hi 00")

  (jab/disconnect conn)

  )
