(ns raphanus.sync
  (:require [raphanus.commands :as commands]
            [raphanus.conn :as conn]
            [raphanus.utils :as utils]
            [raphanus.extra :as extra]
            [clojure.core.async :as a])
  (:refer-clojure :exclude [time sort sync set keys eval get type]))

(defn sync!
  [ch]
  (let [v (a/<!! ch)]
    (if (utils/throwable? v)
      (throw v)
      v)))

(defn sync-send
  [driver data]
  (sync! (conn/send driver data)))

(defn enqueue
  [driver data return-f]
  (return-f (sync-send driver data)))

(defn connect
  [host port & [options]]
  (sync! (conn/mk host port options) options))

(defn persistent
  [host port & [options]]
  (sync! (conn/persistent host port options)))

(defn cluster
  [hosts & [options]]
  (sync! (conn/cluster hosts options)))

(commands/defcommands enqueue)

(defmacro with-lock
  [driver key options & body]
  `(sync! (extra/with-lock ~driver ~key ~options ~@body)))

(comment
  (def c (clojure.core.async/<!! (raphanus.conn/persistent "127.0.0.1" 6379)))

  (def c (clojure.core.async/<!! (raphanus.conn/cluster [{:host "127.0.0.1" :port 30001}])))
  )
