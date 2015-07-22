(ns raphanus.component
  (:require [com.stuartsierra.component :as component]
            [raphanus.sync :as redis]
            [clojure.core.async :as a]))

(defrecord Component []
  component/Lifecycle
  (start [this]
    (let [redis (case (:type this)
                  :cluster (redis/cluster (:hosts this) (:options this))
                  (redis/persistent (:host this) (:port this) (:options this)))]
      (when-not redis
        (throw (Exception. (format "Can't connect to redis: %s" (pr-str this)))))
      (merge this redis)))
  (stop [this]
    (a/close! (:requests this))))

(defn mk [host port & [options]]
  (map->Component {:type :persistent :host host :port port :options options}))

(defn cluster-mk [hosts & [options]]
  (map->Component {:type :cluster :hosts hosts :options options}))

