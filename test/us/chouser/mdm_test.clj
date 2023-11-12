(ns us.chouser.mdm-test
  (:require [clojure.string :as str]
            [clojure.test :as t :refer [deftest is testing]]
            [us.chouser.mdm :as mdm]
            [us.chouser.mdm.sqlite-stats :as s.stats]))

(deftest test-check-state
  (is (= {:alerted? false
          :alert-msg "Cleared alert."
          :last-msg-ts 100
          :check-state-ts 665004
          :topic-ts {"Pressure" 5
                     "Weight" 4
                     "Other" 3}}
         (mdm/check-state {:alerted? true
                           :topic-ts {"Pressure" 5
                                      "Weight" 4
                                      "Other" 3}}
                          100)))

  (is (= [nil
          nil
          "Alert! Weight (12 minutes ago), Pressure (12 minutes ago)"
          "Still alerted. Pressure (28 minutes ago)"
          nil
          "Cleared alert."
          nil])
      (->>
       [[ 400000 []]
        [ 700000 []]
        [1700000 ["Weight"]]
        [1800000 ["Weight"]]
        [1900000 ["Pressure"]]
        [2000000 ["Weight"]]]
       (reductions
        (fn [state [now-ts m]]
          (mdm/check-state (reduce #(assoc-in %1 [:topic-ts %2] now-ts)
                                   state m)
                           now-ts))
        {:topic-ts {"Pressure" 5
                    "Weight" 4
                    "Other" 3}})
       (map :alert-msg))))

(defn count-reboots []
  (-> "select * from \"us.chouser.mdm/reboot\""
      s.stats/query
      count))

(deftest test-booting-metrics
  (with-redefs [mdm/alert-as-needed (constantly nil)]
    (s.stats/with-collector {:filename "metrics-test.db"
                             :period-secs 30}
      (let [sys (atom {})
            msg "2023-09-09T03:18:27	Weights: 727.3628, 727.28, 727.3148, 727.3192, 727.3464, Result=727.3246
2023-09-09T03:18:27	Connecting to ... pressure sensor.
2023-09-09T03:18:27	Booting b'Boink-v1.1
2023-09-09T03:18:28	Pressure: 97.4151, 96.65471, 97.24437, 95.52776, 97.19907, Result=96.8082"
            prev-reboots (count-reboots)]
        (s.stats/with-collector {:filename "metrics-test.db"
                                 :period-secs 30}
          (mdm/on-mqtt-msg sys {:topic "BoinkLog" :msg msg}))
        (is (= (inc prev-reboots)
               (count-reboots)))))))

(deftest daily-report
  (let [msgs (atom [])]
    (with-redefs [mdm/send-group-text #(swap! msgs conj %2)
                  mdm/reschedule! (constantly :ignore)]
      (doto (atom {})
        mdm/daily-report
        (swap! assoc
               :stat-start (- (System/currentTimeMillis)
                              200000)
               :metric {"Bar" 200
                        "Foo" 100})
        mdm/daily-report)
      (is (str/starts-with?
           (first @msgs)
           "Started and connected version"))
      (is (str/starts-with?
           (second @msgs)
           "Over the last 3.3 minutes, I've seen 200 Bar signals, 100 Foo signals. This is version")))))
