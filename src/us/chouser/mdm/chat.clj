(ns us.chouser.mdm.chat
  (:require [clojure.string :as str]
            [us.chouser.gryphonport.util :as util])
  (:import (java.io PushbackReader)
           (java.time ZonedDateTime ZoneOffset ZoneId)
           (java.time.temporal ChronoUnit)
           (java.time.format DateTimeFormatter)))

(def setup-chat-log
  [{:role :system
    :raw "Your name is Otto."}
   {:role :user
    :status {:current-time "2010-01-01T11:00"
             :suppressions {"2010-01-02T05:00" "2010-01-02T07:00"}}
    :chat-room "Joe: Otto, will alerts be sent tomorrow morning?"}
   {:role :assistant
    :thoughts "Since suppressions are what prevent alerts, I should tell Joe about the suppression."
    :send-chat "Alerts may be sent at any time tomorrow except during the suppression from 5am until 7am."}
   {:role :user
    :chat-room "Joe: Otto, please suppress alerts after 5 tomorrow."}
   {:role :assistant
    :thoughts "Joe didn't specify enough detail (no am or pm, no end time) so I must not issue a command. I'll ask for more detail."
    :send-chat "Did you mean in the morning or the evening?"}
   {:role :user
    :chat-room "Joe: Oh I meant in the afternoon, Otto"}
   {:role :assistant
    :thoughts "Now I know Joe meant PM, but I still need an end time, so I must not issue a command yet. I'll suggest a more precise instruction."
    :send-chat "I can suppress alerts starting at 5pm tomorrow. Should I allow alerts after 9pm?"}
   {:role :user
    :chat-room "Joe: That would be perfect, Otto"}
   {:role :assistant
    :thoughts "Now I have everything I need to issue a precise command."
    :commands [[:add-suppression "2010-01-02T17:00" "2010-01-02T22:00"]]
    :send-chat "Ok, Joe, I will not send alerts between 5 pm and 10 pm tomorrow."}
   {:role :user
    :status {:current-time "2010-01-01T11:00"
             :suppressions {"2010-01-02T05:00" "2010-01-02T07:00"
                            "2010-01-02T17:00" "2010-01-02T22:00"}}
    :chat-room "Joe: Cancel next week's suppression, Otto"}
   {:role :assistant
    :thoughts "I don't know what suppression he's talking about, so I must not issue a command. Instead, I'll explain the situation."
    :send-chat "There's no suppression scheduled for next week, but there are two tomorrow."}
   {:role :user
    :chat-room "Joe: Yeah, Otto, allow alerts tomorrow morning"}
   {:role :assistant
    :thoughts "In order to allow alerts in the morning, I need to cancel the suppression at that time."
    :commands [[:cancel-suppression "2010-01-02T05:00"]]
    :send-chat "Cancelled. Now there's only one suppression left."}

   {:role :user
    :chat-room "Joe: Otto could you postpone the start time on that one by half an hour?"}
   {:role :assistant
    :thoughts "I can accomplish this by cancelling the old suppression and adding a new one."
    :commands [[:cancel-suppression "2010-01-02T17:00"]
               [:add-suppression "2010-01-02T17:30" "2010-01-02T22:00"]]
    :send-chat "Tomorrow evening's suppression now starts at 5:30pm"}

   {:role :user
    :status {:suppressions {"2010-01-02T17:30" "2010-01-02T22:00"}}
    :chat-room "Joe: Otto, would you cancel that one too?"}
   {:role :assistant
    :thoughts "There's only one suppression left, so this instruction is clear and I can issue a command."
    :commands [[:cancel-suppression "2010-01-02T17:30"]]
    :send-chat "All suppressions have been cancelled."}])

(def nope
  ["Could you please clarify your instructions? I'm having trouble understanding."
   "I'm sorry, but I didn't quite catch that. Can you rephrase your instructions?"
   "Your instructions are a bit unclear. Could you explain them differently?"
   "It seems I'm not following your instructions. Can you restate them?"
   "I'm having difficulty comprehending your directions. Could you simplify them?"
   "Your instructions are a bit confusing. Can you provide a different explanation?"
   "I'm having trouble making sense of your instructions. Could you reword them?"
   "Sorry, I couldn't grasp your instructions. Can you present them differently?"
   "I'm struggling to understand your instructions. Can you try explaining in another way?"
   "Your instructions are a bit too complex for me. Can you simplify them?"
   "I'm having trouble getting what you mean. Can you give it another shot?"
   "It's not clear to me. Could you rephrase your instructions, please?"
   "I'm not quite getting your instructions. Can you make them clearer?"
   "Your instructions are a bit vague. Could you provide more specific guidance?"
   "Sorry, I didn't catch that. Could you express your instructions differently?"
   "I'm having difficulty following your instructions. Can you simplify them?"
   "It's not quite registering with me. Can you explain it in another way?"
   "Your instructions are a bit muddled. Can you clarify or rephrase them?"
   "I'm struggling to comprehend your instructions. Can you give it another go?"
   "I'm having trouble deciphering your instructions. Can you try a different approach?"])

(let [fmt (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm (z)")]
  (defn format-time [d]
    (.format fmt d)))

(defn now []
  (ZonedDateTime/now (ZoneId/of "America/Chicago")))

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
        [(when current-time ["The current time is " current-time "\n"])
         (if (empty? suppressions)
           "There are no suppressions scheduled\n"
           ["Currently scheduled suppressions:\n"
            (for [[from to] (->> suppressions sort)]
              ["- from " from " until " to "\n"])])])
      (when-let [x (:chat-room entry)] (lines ["CHAT ROOM" x]))])
   (when (= :assistant role)
     [(when-let [x (:thoughts  entry)] (lines ["THOUGHTS" x]))
      (let [x (:commands  entry)]
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
       (partition 2)
       (map (fn [[[section] lines]]
              (case section
                "THOUGHTS" [:thoughts (str/join "\n" lines)]
                "COMMANDS" [:commands
                            (if (and (= 1 (count lines))
                                     (re-matches #"(?i)none" (first lines)))
                              nil
                              (->> lines
                                   (mapv
                                    (fn [line]
                                      (condp re-matches line
                                        #"add suppression: (.+) until (.+)"
                                        :>> (fn [[_ start end]] [:add-suppression start end])
                                        #"cancel suppression: (.*)"
                                        :>> (fn [[_ start]] [:cancel-suppression start])
                                        (throw (ex-info "Bad command" {:cmd line})))))))]
                "SEND CHAT" [:send-chat (str/join "\n" lines)]
                "STATUS" nil
                "CHAT ROOM" nil
                (throw (ex-info "Bad section" {:section section})))))
       (into {:role :assistant})))

(defn prompt [{:keys [chat-log suppressions]}
              new-user-entry]
  (let [context (subvec chat-log (max 0 (- (count chat-log) 10)))]
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
          (update :chat-log #(-> (take-last 7 %)
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
         (handle-response state new-user-entry))))

(def init-state
  {:chat-log []
   :suppressions {}})

(defn cpeek [state chat-str]
  (let [new-user-entry {:role :user
                        :status {:current-time "2013-12-20T15:23"
                                 :suppressions (:suppressions state)}
                        :chat-room chat-str}]
    (->> new-user-entry
         (prompt state)
         util/pprint-msgs)))

(defn go [chat-str]
  (let [new-state (apply-chat-str! @*state (format-time (now)) chat-str)]
    (println (str "SEND CHAT\n" (:send-chat new-state)))
    (reset! *state new-state)))

(defn go-tests []
  (let [t (fn [state chat-str]
            (prn :user chat-str)
            (let [new-state (apply-chat-str! state "2013-12-20T15:23 (CDT)" chat-str)]
              (prn :asst (:send-chat new-state))
              new-state))]
    (-> {:chat-log []
         :suppressions {}}
        (t "Chris: Otto, no alerts tomorrow morning at 5")
        (t "Chris: Otto, that suppression should only last 2 hours")
        (t "Chris: Otto, please silence alerts Wednesday from 4 to 8")
        (t "Chris: In case I wasn't clear, Otto, the times I gave for Wednesday's suppression are in the evening")
        (doto (-> :suppressions
                  (= {"2013-12-21T05:00" "2013-12-21T07:00",
                      "2013-12-25T16:00" "2013-12-25T20:00"})
                  assert))
        (t "Chris: Otto, allow alerts tomorrow but keep other suppressions")
        (t "Chris: Otto, what time is it in New York?")
        (t "Chris: Otto, silence alerts for the next 3 hours")
        (doto (-> :suppressions
                  (= {"2013-12-20T15:23" "2013-12-20T18:23",
                      "2013-12-25T16:00" "2013-12-25T20:00"})
                  assert))
        (t "Chris: When are the upcoming suppressions?")
        :suppressions)))
