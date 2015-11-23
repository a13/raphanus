(ns raphanus.lua
  (:require [raphanus.sync :as sync]
            [raphanus.core :as core])
  (:import [java.security MessageDigest]
           [com.lambdaworks.redis ScriptOutputType RedisCommandExecutionException]
           [com.lambdaworks.redis.cluster SlotHash]))

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

(defprotocol Lua
  (lua
    [client script return-value keys args]
    [client script return-value keys args sha]))

(def RETURN-VALUES {:boolean ScriptOutputType/BOOLEAN
                    :integer ScriptOutputType/INTEGER
                    :status ScriptOutputType/STATUS
                    :value ScriptOutputType/VALUE
                    :multi ScriptOutputType/MULTI})

(defn noscript?
<<<<<<< HEAD
  [e]
  (and (instance? Exception e) (re-find #"^NOSCRIPT\s" (-> e .getCause .getMessage))))

=======
  [return-value res]
  (let [v (case return-value
            :multi (first res)
            res)]
    (and (instance? Exception v) (re-find #"^NOSCRIPT\s" (.getMessage v)))))
>>>>>>> lua noscript handling

(defn selection-first-value
  [execution]
  (-> execution (.iterator) (iterator-seq) first))

(defn prepare-args
  [args]
  (into-array Object (map str args)))

(extend-protocol Lua
  raphanus.core.client-record
  (lua
    ([client script return-value keys args] (lua client script return-value keys args (sha1 script)))
    ([client script return-value keys args sha]
     (let [res (try
                 (sync/evalsha client sha (RETURN-VALUES return-value) (prepare-args keys) (prepare-args args))
                 (catch RedisCommandExecutionException e e))]
       (if (noscript? return-value res)
         (do (sync/scriptLoad client script)
             (sync/evalsha client sha (RETURN-VALUES return-value)
                           (prepare-args keys) (prepare-args args)))
         res))))
  raphanus.core.cluster-record
  (lua
    ([client script return-value keys args] (lua client script return-value keys args (sha1 script)))
    ([client script return-value keys args sha]
     (let [slot (SlotHash/getSlot (or (first keys) sha))
           node (sync/cluster-nodes client (core/->predicate #(.hasSlot % slot)))]
       (let [res (-> (sync/node-evalsha node sha (RETURN-VALUES return-value)
                                        (prepare-args keys) (prepare-args args))
                     (selection-first-value))]
         (if (noscript? return-value res)
           (do
             (sync/node-scriptLoad node script)
             (-> (sync/node-evalsha node sha (RETURN-VALUES return-value)
                                    (prepare-args keys) (prepare-args args))
                 (selection-first-value)))
           res))))))

(defmacro defscript [script-name script-body return-value]
  (let [script-name-str (name script-name)]
    `(let [hash# (sha1 ~script-body)]
       (defn ~script-name
         ([driver#]
          (~script-name driver# [] []))
         ([driver# keys#]
          (~script-name driver# keys# []))
         ([driver# keys# args#]
          (lua driver# ~script-body ~return-value keys# args# hash#))))))

(defn from
  "Convenience function to load a script into a String so it can be defined with
  defscript"
  [path-to-script]
  (when-let [script-url (clojure.java.io/resource path-to-script)]
    (slurp script-url)))
