(ns raphanus.utils)

(defn throwable?
  [v]
  (instance? Throwable v))
