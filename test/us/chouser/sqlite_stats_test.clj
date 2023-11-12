(ns us.chouser.sqlite-stats-test
  (:require [clojure.test :as t :refer [deftest is testing]]
            [us.chouser.mdm.sqlite-stats :as ss]))

(def example-info
  {::ss/value {:tag-types {:topic :int}},
   ::ss/interval {:tag-types {:topic :int}}})

(def example-values
  {{:topic "Weight", :metric :us.chouser.mdm/value}
   , {:vmin 2033.654, :vmax 2035.223, :vsum 437359.633, :vcount 215},
   {:topic "Weight", :metric :us.chouser.mdm/interval}
   , {:vmin 0, :vmax 595799, :vsum 1032678, :vcount 216},
   {:topic "Pressure", :metric :us.chouser.mdm/value}
   , {:vmin 98.9997, :vmax 100.0087, :vsum 21492.37893, :vcount 216},
   {:topic "Pressure", :metric :us.chouser.mdm/interval}
   , {:vmin 0, :vmax 597926, :vsum 1034299, :vcount 216}})

(deftest test-metric-create-table
  (is (= (str "create table if not exists \"us.chouser.mdm.sqlite-stats/value\""
              " (\"time\" int, \"topic\" int, \"vmin\" int, \"vmax\" int, \"vsum\" int, \"vcount\" int)")
         (#'ss/metric-create-table example-info ::ss/value))))

(deftest test-metric-insert-values
  (is (= ["insert into \"us.chouser.mdm/interval\" values(1234, 0, 595799, 1032678, 216)"
          "insert into \"us.chouser.mdm/interval\" values(1234, 0, 597926, 1034299, 216)"
          "insert into \"us.chouser.mdm/value\" values(1234, 2033.654, 2035.223, 437359.633, 215)"
          "insert into \"us.chouser.mdm/value\" values(1234, 98.9997, 100.0087, 21492.37893, 216)"]
         (sort
          (#'ss/metric-insert-values 1234 example-info example-values)))))
