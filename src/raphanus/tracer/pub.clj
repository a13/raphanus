(ns raphanus.tracer.pub
  (:require [raphanus.tracer :as tracer]
            [com.stuartsierra.component :as component]))

(defrecord PubTracer [tracers]
  component/Lifecycle
  (start [this] (assoc this :tracers (filter (partial satisfies? tracer/Tracer) (vals this))))
  (stop [this] this)
  tracer/Tracer
  (request-completed [this cmd-name args res duration]
    (doseq [t tracers]
      (tracer/request-completed t cmd-name args res duration))))
