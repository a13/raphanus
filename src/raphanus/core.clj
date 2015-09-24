(ns raphanus.core
  (:require [defcomponent :refer [defcomponent]])
  (:import [com.lambdaworks.redis RedisClient RedisURI]
           [com.lambdaworks.redis.cluster RedisClusterClient]
           [java.util.function Predicate]))


(defn ->predicate
  [pred]
  (reify Predicate
    (test [this o]
      (pred o))))

(defn redis-uri
  [params]
  (let [uri (RedisURI.)]
    (when-let [v (:host params)] (.setHost uri v))
    (when-let [v (:port params)] (.setPort uri v))
    uri))

(defcomponent cluster []
  [uris]
  (start [this]
         (let [client (RedisClusterClient. (map redis-uri uris))]
           (assoc this :client client :conn (.connect client))))
  (stop [this] (.shutdown (:client this))))

(defcomponent client []
  [uri]
  (start [this]
         (let [client (RedisClient. (redis-uri uri))]
           (assoc this :client client :conn (.connect client))))
  (stop [this] (.shutdown (:client this))))
