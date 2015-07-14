(ns raphanus.tracer.prom
  "Prometheus tracer"
  (:require [raphanus.tracer :as tracer]
            [com.stuartsierra.component :as component])
  (:import [io.prometheus.client Summary]))


(defrecord PromTracer [requests-summary]
  component/Lifecycle
  (start [this] (assoc this :request-counter
                       (-> (Summary/build)
                           (.name "raphanus_request_durations_milliseconds")
                           (.labelNames ["command"])
                           (.help "raphanus requests summary")
                           (.register))))
  (stop [this] this)
  tracer/Tracer
  (request-completed [this cmd-name args res duration]
    (-> requests-summary
        (.labels (into-array [cmd-name]))
        (.observe (/ duration 1000000)))))

