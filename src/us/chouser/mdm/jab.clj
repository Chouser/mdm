(ns us.chouser.mdm.jab
  (:import (org.jivesoftware.smack.packet Message$Type)
           (org.jivesoftware.smack.tcp XMPPTCPConnection
                                       XMPPTCPConnectionConfiguration)
           (org.jivesoftware.smackx.muc MultiUserChat MultiUserChatManager)
           (org.jxmpp.jid.impl JidCreate)
           (org.jxmpp.jid.parts Resourcepart)))

(set! *warn-on-reflection* true)

(defn connect [{:keys [^String host, username, password]}]
  (-> (XMPPTCPConnectionConfiguration/builder)
      (.setXmppDomain host)
      (.setHost host)
      (.setUsernameAndPassword username password)
      (.setCompressionEnabled false)
      (.build)
      (XMPPTCPConnection.)
      (doto .connect .login)))

(defn join-muc [conn {:keys [^String address, nickname, password]}]
  (-> (MultiUserChatManager/getInstanceFor conn)
      (.getMultiUserChat (JidCreate/entityBareFrom address))
      (doto (.join (Resourcepart/from nickname) password))))

(defn send-muc [^MultiUserChat muc, ^String body]
  (->> (doto (.createMessage muc)
         (.setType Message$Type/groupchat)
         (.setBody body))
       (.sendMessage muc)))

(defn disconnect [^XMPPTCPConnection conn]
  (.disconnect conn))
