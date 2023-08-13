(ns us.chouser.mdm.mqtt
  (:import (org.eclipse.paho.mqttv5.client
            IMqttMessageListener MqttClient MqttConnectionOptions)
           (org.eclipse.paho.mqttv5.client.persist MemoryPersistence)
           (org.eclipse.paho.mqttv5.common MqttSubscription)))

(set! *warn-on-reflection* true)

(defn array-of [sym]
  (.getName (class (into-array (resolve sym) []))))

(defn connect [{:keys [address client-id]}]
  (doto (MqttClient. address client-id (MemoryPersistence.))
    (.connect (doto (MqttConnectionOptions.)
                (.setAutomaticReconnect true)
                ;; We're not trying to receive every message, but instead
                ;; monitor that fresh messages are being received. Use
                ;; CleanStart to disable message persistence:
                (.setCleanStart true)))))

(defn subscribe [^MqttClient client {:keys [topic qos msg-fn]}]
  (let [sub (MqttSubscription. topic (or qos 1))]
    {:subscription sub
     :token (.subscribe client
                        ^#=(array-of MqttSubscription) (into-array [sub])
                        ^#=(array-of IMqttMessageListener)
                        (into-array [(reify IMqttMessageListener
                                       (messageArrived [_ topic msg]
                                         (msg-fn {:topic topic
                                                  :msg (str msg)})))]))}))

(defn disconnect [^MqttClient client]
  (.disconnect client))
