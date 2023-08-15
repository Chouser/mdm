(ns us.chouser.mdm-test
  (:require [us.chouser.mdm :as mdm]
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
