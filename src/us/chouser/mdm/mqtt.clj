(ns us.chouser.mdm.mqtt
  (:import (org.eclipse.paho.mqttv5.client
            IMqttMessageListener MqttCallback MqttClient MqttConnectionOptions)
           (org.eclipse.paho.mqttv5.client.persist MemoryPersistence)
           (org.eclipse.paho.mqttv5.common MqttSubscription)))

(set! *warn-on-reflection* true)

(defn array-of [sym]
  (.getName (class (into-array (resolve sym) []))))

(defn connect [{:keys [address client-id]}]
  (doto (MqttClient. address client-id (MemoryPersistence.))
    (.setCallback
     (reify MqttCallback
       (disconnected [_ resp]
         (println "mqtt disconnected:" resp))
       (mqttErrorOccurred [_ ex]
         (println "mqtt error occurred:" ex))
       (messageArrived [_ topic message]
         :ignore)
       (deliveryComplete [_ token]
         (println "mqtt delivery complete:" token))
       (connectComplete [_ reconnect uri]
         (println "mqtt connect complete:" reconnect uri))
       (authPacketArrived [_ reasonCode properties]
         (println "mqtt auth packet arrived:" reasonCode properties))))
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
