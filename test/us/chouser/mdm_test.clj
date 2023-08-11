(ns us.chouser.mdm-test
  (:require [us.chouser.mdm :as mdm]
            [clojure.test :as t :refer [deftest is testing]]))

(deftest test-check-state
  (is (= {:pressure-alerted? false
          :weight-alerted? false
          :pressure-ts 99
          :weight-ts 99
          :alert-msg "Cleared all alerts: Weight and Pressure signals received."}
         (mdm/check-state {:pressure-alerted? true
                           :weight-alerted? true
                           :pressure-ts 99
                           :weight-ts 99}
                          100))))
