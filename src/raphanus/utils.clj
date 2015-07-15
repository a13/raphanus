(ns raphanus.utils
  (:require [clojure.core.async :as a]
            [clojure.tools.logging :as log]))

(defn throwable?
  [v]
  (instance? Throwable v))

(defn safe-res
  [res]
  (if (throwable? res)
    (throw res)
    res))

(defmacro <? [ch]
  `(when-let [sexp-res# ~ch]
     (safe-res (a/<! sexp-res#))))

(defmacro safe
  [& body]
  `(try
     ~@body
     (catch Throwable e#
       (log/error e#)
       e#)))

(defmacro safe-go
  [& body]
  `(a/go (safe ~@body)))
