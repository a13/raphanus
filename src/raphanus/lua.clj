(ns raphanus.lua
  (:require [clojure.core.async :as a]
            [raphanus.core :as core]
            [raphanus.utils :as utils])
  (:import [java.security MessageDigest]))

(defn bytes->hex
  [bytes]
  (let [sb (StringBuilder.)]
    (doseq [b (seq bytes)]
      (.append sb (String/format "%02x" (into-array [b]))))
    (.toString sb)))

(defn sha1
  [s]
  (let [md (MessageDigest/getInstance "SHA-1")]
    (.update md (.getBytes s))
    (bytes->hex (.digest md))))

(defn lua
  [driver script sha keys args]
  (utils/safe-go
   (let [res (a/<! (apply core/evalsha driver sha (count keys) (concat keys args)))
         err? (ex-data res)]
      (if (= "NOSCRIPT" (:type err?))
        (do (utils/<? (core/script-load (:conn err? driver) script))
            (utils/<? (lua driver script sha keys args)))
        res))))

(defmacro defscript [script-name script-body]
  (let [script-name-str (name script-name)
        sync-script-name (symbol (str script-name-str "!"))]
    `(let [hash# (sha1 ~script-body)]
       (defn ~script-name
         ([driver#]
          (~script-name driver# [] []))
         ([driver# keys#]
          (~script-name driver# keys# []))
         ([driver# keys# args#]
          (lua driver# ~script-body hash# keys# args#)))
       (defn ~sync-script-name
         ([driver#] (~sync-script-name driver# [] []))
         ([driver# keys#] (~sync-script-name driver# keys# []))
         ([driver# keys# args#] (utils/<? (~sync-script-name driver# keys# args#)))))))

(defn from
  "Convenience function to load a script into a String so it can be defined with
  defscript"
  [path-to-script]
  (when-let [script-url (clojure.java.io/resource path-to-script)]
    (slurp script-url)))
