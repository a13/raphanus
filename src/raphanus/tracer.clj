(ns raphanus.tracer)

(defprotocol Tracer
  (request-completed [this cmd-name args res duration]))

