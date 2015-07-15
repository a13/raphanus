(ns raphanus.sync-test
  (:require [raphanus.sync :as sync]
            [raphanus.conn :as conn]
            [clojure.core.async :as a]
            [clojure.test :refer :all]))

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

;; (deftest basic
;;   (is (= "OK" (sync/set *redis* "foo" "bar")))
;;   (is (= "bar" (sync/get *redis* "foo"))))

(deftest locks
  (sync/with-lock *redis* "test-lock" {:expire 5000}
    (assert true)))
