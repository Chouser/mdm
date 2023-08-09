(ns us.chouser.mdm
  (:require [clojure.edn :as edn]
            [us.chouser.mdm.jab :as jab]
            [us.chouser.mdm.mqtt :as mqtt]))

(def get-secret
  (partial get (edn/read-string (slurp "secrets.edn"))))

(comment

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


  (def mqtt-client
    (mqtt/connect {:address (get-secret :mqtt-broker)
                   :client-id "Otto"}))

  (def subs-info
    (mqtt/subscribe mqtt-client
                    {:topic "#"
                     :msg-fn prn}))

  (mqtt/disconnect mqtt-client)

  )
