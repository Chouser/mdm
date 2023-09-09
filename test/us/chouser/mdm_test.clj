(ns us.chouser.mdm-test
  (:require [us.chouser.mdm :as mdm]
            [us.chouser.mdm.sqlite-stats :as s.stats]
            [clojure.test :as t :refer [deftest is testing]]))

(deftest test-check-state
  (is (= {:pressure-alerted? false
          :weight-alerted? false
          :pressure-ts 99
          :weight-ts 99
          :msg-ts 100
          :alert-msg "Cleared all alerts: Weight and Pressure signals received."}
         (mdm/check-state {:pressure-alerted? true
                           :weight-alerted? true
                           :pressure-ts 99
                           :weight-ts 99}
                          100)))

  (doseq [p0 [0 1], w0 [0 1], p1 [0 1], w1 [0 1]]
    (let [pts (if (zero? p1) 9999999 0)
          wts (if (zero? w1) 9999999 0)
          s (mdm/check-state {:pressure-alerted? (pos? p0)
                              :weight-alerted? (pos? w0)
                              :pressure-ts pts
                              :weight-ts wts}
                             10000000)]
      (is (= (or (and (zero? p0) (pos? p1))
                 (and (zero? w0) (pos? w1)))
             (boolean (re-find #"(?i)alert!" (or (:alert-msg s) "")))))
      (is (= (boolean (re-find #"(?i)alert!" (or (:alert-msg s) "")))
             (boolean (re-find #"2[.]8 hours" (or (:alert-msg s) ""))))))))

(defn count-reboots []
  (-> "select * from \"us.chouser.mdm/reboot\""
      s.stats/query
      count))

(deftest test-booting-metrics
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
             (count-reboots))))))
