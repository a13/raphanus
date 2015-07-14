(ns raphanus.tracer.log
  (:require [raphanus.tracer :as tracer]
            [clojure.tools.logging :as log]))

(defrecord LogTracer []
  tracer/Tracer
  (request-completed [this cmd-name args res duration]
    (log/debugf "Redis request %s %s completed in %.02f msecs" cmd-name args (float (/ duration 1000000)))))

(defn mk [] (map->LogTracer {}))

