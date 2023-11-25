(ns us.chouser.mdm.chat
  (:require [clojure.string :as str]
            [us.chouser.gryphonport.util :as util])
  (:import (java.io PushbackReader)
           (java.time Instant LocalDateTime ZonedDateTime ZoneOffset ZoneId)
           (java.time.temporal ChronoUnit)
           (java.time.format DateTimeFormatter)))

(def nope
  ["I'm a bit lost with your instructions. Can you break them down differently?"
   "I'm a little confused by your instructions. Could you simplify or clarify them?"
   "I'm finding it challenging to comprehend your instructions. Could you present them differently?"
   "I'm finding it hard to follow your instructions. Could you try presenting them differently?"
   "I'm having a hard time understanding your instructions. Could you phrase them differently?"
   "I'm having a tough time making sense of your instructions. Can you try explaining differently?"
   "I'm having difficulty following your instructions. Can you provide a different explanation?"
   "I'm having difficulty following your instructions. Could you simplify or reword them?"
   "I'm having trouble grasping your instructions. Could you rephrase them for me?"
   "I'm having trouble interpreting your instructions. Could you try another way of explaining?"
   "I'm having trouble making sense of your instructions. Could you provide an alternative explanation?"
   "I'm struggling to understand your instructions. Could you provide an alternative explanation?"
   "Your instructions are a bit difficult for me to follow. Could you phrase them differently or simplify?"
   "Your instructions are a bit puzzling to me. Could you try rewording them?"
   "Your instructions are a bit unclear to me. Can you try breaking them down differently?"
   "Your instructions are a bit unclear to me. Could you explain in a different way?"
   "Your instructions are causing confusion for me. Could you try explaining differently?"
   "Your instructions are causing some confusion. Could you try expressing them differently?"
   "Your instructions are not quite registering with me. Could you rephrase them?"
   "Your instructions aren't quite clear to me. Can you try explaining in a different manner?"])

(def tz (ZoneId/of "America/Chicago"))

;; TODO include day of week when posting current date
(let [fmt (-> (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm")
              (.withZone tz))]
  (defn format-time [d]
    (.format fmt d))
  (defn parse-time [s]
    (LocalDateTime/parse s fmt)))

(defn format-ts [ts]
  (-> ts Instant/ofEpochMilli (.atZone tz) format-time))

(defn parse-to-ts [s]
  (-> s parse-time (.atZone tz) .toInstant .toEpochMilli))

(let [fmt (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm, 'a' EEEE")]
  (defn format-current-time [d]
    (.format fmt d)))

(defn now []
  (ZonedDateTime/now tz))

(def setup-chat-log
  [{:role :system
    :raw "Your name is Otto."}
   {:role :user
    :status {:current-time (format-current-time (parse-time "2010-01-01T11:00"))
             :suppressions {"2010-01-02T05:00" "2010-01-02T07:00"}}
    :chat-room "Joe said: Otto, will alerts be sent tomorrow morning?"}
   {:role :assistant
    :thoughts "Since suppressions are what prevent alerts, I should tell Joe about the suppression."
    :send-chat "Alerts may be sent at any time tomorrow except during the suppression from 5am until 7am."}
   {:role :user
    :chat-room "Joe said: Suppress alerts after 5 tomorrow."}
   {:role :assistant
    :thoughts "I don't know exactly what time Joe means by '5'. I'll ask for more detail."
    :send-chat "Did you mean in the morning or the evening?"}
   {:role :user
    :chat-room "Joe said: Oh I meant in the afternoon"}
   {:role :assistant
    :thoughts "Now I know Joe meant PM, and 4 hours is probably a good duration for a suppression."
    :commands [[:add-suppression "2010-01-02T17:00" "2010-01-02T21:00"]]
    :send-chat "Ok, Joe, I will not send alerts between 5 PM and 9 PM tomorrow."}])

(defonce *state
  (atom {:chat-log []
         :suppressions {}}))

(defn lines [strs]
  (map list strs (repeat "\n")))

(defn chat-log-entry->msg [{:keys [role raw] :as entry}]
  [role
   raw
   (when (= :user role)
     [(when-let [{:keys [current-time suppressions]} (:status entry)]
        [(when current-time ["The current time is " current-time " (all times are local to Chicago)\n"])
         (if (empty? suppressions)
           "There are no suppressions scheduled\n"
           ["Currently scheduled suppressions:\n"
            (for [[from to] (->> suppressions sort)]
              ["- from " from " until " to "\n"])])])
      (when-let [x (:chat-room entry)] (lines ["CHAT ROOM" x]))])
   (when (= :assistant role)
     [(let [x (:commands  entry)]
        (lines (cons "COMMANDS"
                     (if (empty? x)
                       ["None"]
                       (->> x
                            (mapv (fn [[cmd a b]]
                                    (case cmd
                                      :add-suppression ["add suppression: " a " until " b]
                                      :cancel-suppression ["cancel suppression: " a]))))))))
      (when-let [x (:send-chat entry)] (lines ["SEND CHAT" x]))])])

(defn parse [s]
  (->> (re-seq #".+" s)
       (partition-by #(re-matches #"[A-Z ]+" %))
       (drop-while #(not (re-matches #"[A-Z ]+" (first %))))
       (partition 2)
       (map (fn [[[section] lines]]
              (case section
                "COMMANDS" [:commands
                            (if (and (= 1 (count lines))
                                     (re-matches #"(?i)none" (first lines)))
                              nil
                              (->> lines
                                   (mapv
                                    (fn [line]
                                      (condp re-matches line
                                        #"add suppression: (.+) until (.+)"
                                        :>> (fn [[_ start end]]
                                              (run! parse-time [start end])
                                              [:add-suppression start end])
                                        #"cancel suppression: (.*)"
                                        :>> (fn [[_ start]] [:cancel-suppression start])
                                        (throw (ex-info "Bad command" {:cmd line})))))))]
                "SEND CHAT" [:send-chat (str/join "\n" lines)]
                (do
                  (println "WARNING: Bad section " (pr-str section))
                  nil))))
       (into {:role :assistant})
       (#(if (:send-chat %)
           %
           (throw (ex-info "No SEND CHAT reponse from GPT" {:str s}))))))

(defn prompt [{:keys [chat-log suppressions]}
              new-user-entry]
  (let [context (->> chat-log
                     ;; even number, not to big to keep the bill small
                     (drop (- (count chat-log) 10))
                     (drop-while #(not= :user (:role %))))]
    (conj (->>
           (concat setup-chat-log
                   [(first context)]
                   (->> context rest (map #(dissoc % :status))))
           (remove empty?)
           (mapv chat-log-entry->msg))
          (concat (chat-log-entry->msg new-user-entry)
                  (lines ["\nINSTRUCTION"
                          "Alerts may be sent at any time except during a suppression."
                          "Your response must include a SEND CHAT section."
                          "Each line in COMMANDS absolutely must match one of these patterns:"
                          "- add suppression: <start> until <end>"
                          "- cancel suppression: <start>"
                          "- none"])))))

(defn handle-response [state new-user-entry resp-str]
  (try
    (let [parsed (parse resp-str)]
      (-> state
          ;; Keep plenty of history to avoid re-processing
          (update :chat-log #(-> (take-last 150 %)
                                 vec
                                 (conj new-user-entry parsed)))
          (update :suppressions
                  (fn [ss]
                    (reduce (fn [ss [cmd a b]]
                              (case cmd
                                :add-suppression
                                , (if (get ss a)
                                    (throw (ex-info "Suppression collision"
                                                    {:start a
                                                     :prompt (str "Cannot add a suppression starting at "
                                                                  a " because one already exists.")}))
                                    (assoc ss a b))
                                :cancel-suppression
                                , (if-not (get ss a)
                                    (throw (ex-info "Suppression collision"
                                                    {:start a
                                                     :prompt (str "There is no suppression starting at "
                                                                  a " to be cancelled.")}))
                                    (dissoc ss a))))
                            ss
                            (:commands parsed))))
          (assoc :send-chat (:send-chat parsed))))
    (catch Exception ex
      (println (str "ERROR in handle-response, resp-str:\n" resp-str))
      (.printStackTrace ex)
      (assoc state :send-chat (rand-nth nope)))))

(defn remove-old-suppressions [now-str state]
  (update state :suppressions
          #(reduce-kv (fn [s start end]
                        (if (<= (compare end now-str) 0)
                          (dissoc s start)
                          s))
                      %
                      %)))

(defn apply-chat-str! [state now-str chat-str & [id]]
  (let [new-user-entry {:role :user
                        :status {:current-time now-str
                                 :suppressions (:suppressions state)}
                        :chat-room chat-str
                        :id id}]
    (->> new-user-entry
         (prompt state)
         util/chatm
         util/content
         (handle-response state new-user-entry)
         (remove-old-suppressions now-str))))

(def init-state
  {:chat-log []
   :suppressions {}})

(defn cpeek [state chat-str]
  (let [new-user-entry {:role :user
                        :status {:current-time
                                 , (format-current-time (parse-time "2013-12-20T15:23"))
                                 :suppressions (:suppressions state)}
                        :chat-room chat-str}]
    (->> new-user-entry
         (prompt state)
         util/pprint-msgs)))

(defn go [chat-str]
  (let [new-state (apply-chat-str! @*state
                                   (format-current-time (now))
                                   chat-str)]
    (println (str "SEND CHAT\n" (:send-chat new-state)))
    (reset! *state new-state)))

(defn go-tests []
  (let [t (fn [state chat-str]
            (prn :user chat-str)
            (let [new-state (apply-chat-str!
                             state
                             (format-current-time (parse-time "2013-12-20T15:23"))
                             chat-str)]
              (prn :asst (:send-chat new-state))
              new-state))]
    (-> {:chat-log []
         :suppressions {}}
        (t "Chris said: Otto, no alerts tomorrow morning at 5")
        (t "Chris said: Otto, that suppression should only last 2 hours")
        (t "Chris said: Otto, please silence alerts Wednesday from 4 to 8")
        (t "Chris said: In case I wasn't clear, Otto, the times I gave for Wednesday's suppression are in the evening")
        (doto (-> :suppressions
                  (= {"2013-12-21T05:00" "2013-12-21T07:00",
                      "2013-12-25T16:00" "2013-12-25T20:00"})
                  assert))
        (t "Chris said: Otto, cancel tomorrow's suppression")
        (t "Chris said: Otto, what time is it in New York?")
        (t "Chris said: Otto, silence alerts for the next 3 hours")
        (doto (-> :suppressions
                  (= {"2013-12-20T15:23" "2013-12-20T18:23",
                      "2013-12-25T16:00" "2013-12-25T20:00"})
                  assert))
        (t "Chris said: When are the upcoming suppressions?")
        :suppressions)))
