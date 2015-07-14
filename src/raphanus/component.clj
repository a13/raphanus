(ns raphanus.component
  (:require [com.stuartsierra.component :as component]
            [raphanus.conn :as conn]
            [clojure.core.async :as a]))

(defrecord Component []
  component/Lifecycle
  (start [this]
    (merge this (case (:type this)
                  :cluster (conn/cluster (:hosts this) (:options this))
                  (conn/persistent (:host this) (:port this) (:options this)))))
  (stop [this]
    (a/close! (:requests this))))

(defn mk [host port & [options]]
  (map->Component {:type :persistent :host host :port port :options options}))

(defn cluster-mk [hosts & [options]]
  (map->Component {:type :cluster :hosts hosts :options options}))

