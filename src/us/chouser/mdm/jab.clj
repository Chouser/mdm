(ns us.chouser.mdm.jab
  (:import (org.jivesoftware.smack ReconnectionManager
                                   ReconnectionManager$ReconnectionPolicy)
           (org.jivesoftware.smack.packet Message Message$Type Presence Presence$Type)
           (org.jivesoftware.smack.tcp XMPPTCPConnection
                                       XMPPTCPConnectionConfiguration)
           (org.jivesoftware.smackx.muc MultiUserChat MultiUserChatManager)
           (org.jivesoftware.smack.chat2 ChatManager)
           (org.jxmpp.jid.impl JidCreate)
           (org.jxmpp.jid.parts Resourcepart)))

(set! *warn-on-reflection* true)

(org.jivesoftware.smack.SmackConfiguration/setDefaultReplyTimeout 30000)

(defn connect
  "on-message will be called with a map containing :id, :from, and :body. It
  must return promptly without making any Jabber callsor other blocking IO"
  [{:keys [^String host, username, password, on-message, on-connect]}]
  (let [conn (-> (XMPPTCPConnectionConfiguration/builder)
                 (.setXmppDomain host)
                 (.setUsernameAndPassword username password)
                 (.setCompressionEnabled false)
                 (.build)
                 (XMPPTCPConnection.))]
    (doto (ReconnectionManager/getInstanceFor conn)
      .enableAutomaticReconnection
      (.setFixedDelay 10) ;; seconds
      (.setReconnectionPolicy
       ReconnectionManager$ReconnectionPolicy/RANDOM_INCREASING_DELAY))
    (when on-connect
      (.addConnectionListener
       conn (reify org.jivesoftware.smack.ConnectionListener
              (authenticated [this conn resumed?]
                (prn :jab-authenticated :resumed? resumed?)
                (on-connect conn)))))
    (when on-message
      (.addSyncStanzaListener
       conn
       (reify org.jivesoftware.smack.StanzaListener
         (processStanza [_ stanza]
           (when (instance? Message stanza)
             (let [msg ^org.jivesoftware.smack.packet.Message stanza]
               (println :jab (str (.toXML msg)))
               (when (.getStanzaId msg)
                 (on-message
                  {:id (.getStanzaId msg)
                   :from (-> msg .getFrom str)
                   :type (some-> msg .getType str keyword) ;; chat vs groupchat
                   :body (.getBody msg)}))))))
       (reify org.jivesoftware.smack.filter.StanzaFilter
         (accept [_ _] true))))
    (doto conn .connect .login)))

(defn join-muc [conn {:keys [^String address, nickname, password]}]
  (let [muc (-> (MultiUserChatManager/getInstanceFor conn)
                (.getMultiUserChat (JidCreate/entityBareFrom address)))]
    (.join muc
           (-> (.getEnterConfigurationBuilder muc (Resourcepart/from nickname))
               (.withPassword password)
               (.requestHistorySince 120) ;; seconds
               (.timeoutAfter 45000) ;; milliseconds; default was 5000
               .build))
    muc))

(defn send-muc [^MultiUserChat muc, ^String body]
  (->> (doto (.createMessage muc)
         (.setType Message$Type/groupchat)
         (.setBody body))
       (.sendMessage muc)))

(defn disconnect [^XMPPTCPConnection conn]
  (.disconnect conn))

(defn send-chat [^XMPPTCPConnection conn ^String address ^String msg]
  (let [jid (JidCreate/entityBareFrom address)]
    ;; Request private message authorization:
    (.sendStanza conn (Presence. jid Presence$Type/subscribe))
    ;; Send chat message:
    (.send (.chatWith (ChatManager/getInstanceFor conn) jid)
           (doto (Message. jid Message$Type/chat)
             (.setBody msg)))))
