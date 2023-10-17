(ns us.chouser.mdm.mqtt
  (:require [us.chouser.mdm.sqlite-stats :as ss])
  (:import (org.eclipse.paho.mqttv5.client
            IMqttMessageListener MqttCallback MqttClient MqttConnectionOptions)
           (org.eclipse.paho.mqttv5.client.persist MemoryPersistence)
           (org.eclipse.paho.mqttv5.common MqttSubscription)))

(set! *warn-on-reflection* true)

(ss/register-metrics
 {::session    {:tag-types {:event :string}}})

(defn array-of [sym]
  (.getName (class (into-array (resolve sym) []))))

(defn subscribe [^MqttClient client {:keys [topic qos msg-fn]}]
  (println "mqtt subscribing" topic)
  (ss/record ::session {:event :attempt-subscribe} 1)
  (let [sub (MqttSubscription. topic (or qos 1))]
    {:subscription sub
     :token (.subscribe client
                        ^#=(array-of MqttSubscription) (into-array [sub])
                        ^#=(array-of IMqttMessageListener)
                        (into-array [(reify IMqttMessageListener
                                       (messageArrived [_ topic msg]
                                         (msg-fn {:topic topic
                                                  :msg (str msg)})))]))}))

(defn connect [a]
  (let [{:keys [address client-id subs]} (:config @a)
        client (MqttClient. address client-id (MemoryPersistence.))
        callback
        , (reify MqttCallback
            (disconnected [_ resp]
              (println "mqtt disconnected:" resp)
              (ss/record ::session {:event :disconnect} 1)
              (loop []
                (Thread/sleep 5000)
                (when (and (not (:stop? @a))
                           (try (connect a)
                                false
                                (catch Exception ex
                                  (println "mqtt connect failed:" (.getMessage ex))
                                  true)))
                  (recur))))
            (mqttErrorOccurred [_ ex]
              (ss/record ::session {:event :error} 1)
              (println "mqtt error occurred:" ex))
            (messageArrived [_ topic message]
              (ss/record ::session {:event :message} 1))
            (deliveryComplete [_ token]
              (ss/record ::session {:event :delivered} 1)
              (println "mqtt delivery complete:" token))
            (connectComplete [_ reconnect? uri]
              (ss/record ::session {:event :connected} 1)
              (println "mqtt connect complete:"
                       (if reconnect?
                         "paho-auto-reconnct"
                         "pano-no-auto-reconnect")
                       uri))
            (authPacketArrived [_ reasonCode properties]
              (ss/record ::session {:event :auth-packet} 1)
              (println "mqtt auth packet arrived:" reasonCode properties)))]
    (swap! a assoc :client client)
    (.setCallback client callback)
    (ss/record ::session {:event :attempt-connect} 1)
    (.connect client (doto (MqttConnectionOptions.)
                       (.setConnectionTimeout 5)
                       ;; We're not trying to receive every message, but instead
                       ;; monitor that fresh messages are being received. Use
                       ;; CleanStart to disable message persistence:
                       (.setCleanStart true)))
    (->> subs (run! #(subscribe client %)))
    a))

(defn start [config]
  (connect (atom {:config config})))

(defn stop [a]
  (swap! a assoc :stop? true)
  (.disconnect ^MqttClient (:client @a)))
