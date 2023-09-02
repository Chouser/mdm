(ns us.chouser.mdm.mqtt
  (:import (org.eclipse.paho.mqttv5.client
            IMqttMessageListener MqttCallback MqttClient MqttConnectionOptions)
           (org.eclipse.paho.mqttv5.client.persist MemoryPersistence)
           (org.eclipse.paho.mqttv5.common MqttSubscription)))

(set! *warn-on-reflection* true)

(defn array-of [sym]
  (.getName (class (into-array (resolve sym) []))))

(defn subscribe [^MqttClient client {:keys [topic qos msg-fn]}]
  (println "mqtt subscribing" topic)
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
        client (MqttClient. address client-id (MemoryPersistence.))]
    (swap! a assoc :client
           (doto client
             (.setCallback
              (reify MqttCallback
                (disconnected [_ resp]
                  (println "mqtt disconnected:" resp)
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
                         (.setConnectionTimeout 5)
                         ;; We're not trying to receive every message, but instead
                         ;; monitor that fresh messages are being received. Use
                         ;; CleanStart to disable message persistence:
                         (.setCleanStart true)))))
    (->> subs (run! #(subscribe client %)))
    a))

(defn start [config]
  (connect (atom {:config config})))

(defn stop [a]
  (swap! a assoc :stop? true)
  (.disconnect ^MqttClient (:client @a)))
