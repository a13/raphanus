(ns raphanus.core-test
  (:require [clojure.test :refer :all]
            [raphanus.core :as core]
            [raphanus.sync :as sync]
            [clojure.core.async :as a]))

(def ^:dynamic *redis* nil)

(defn with-redis
  [f]
  (binding [*redis* (sync/persistent "127.0.0.1" 6379)]
    (when-not *redis*
      (throw (ex-info "Can't connect" {})))
    (try
      (f)
      (finally
        (a/close! (:requests *redis*))))))

(defn with-flush
  [f]
  (sync/flushdb *redis*)
  (f))

(use-fixtures :each with-redis with-flush)

(deftest basic
  (is (nil? (a/<!! (core/get *redis* "foo"))))
  (is (= "OK" (a/<!! (core/set *redis* "foo" "bar"))))
  (is (= "bar" (a/<!! (core/get *redis* "foo")))))
