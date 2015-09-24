(ns raphanus.commands
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [clojure.reflect :as r])
  (:import [com.lambdaworks.redis SetArgs]))

(defn arguments-list
  [method]
  (for [i (range (count (:parameter-types method)))]
    (symbol (str "arg" (inc i)))))

(defn set-args
  [m]
  (-> (SetArgs.)
      (cond->
          (:ex m) (.ex (:ex m))
          (:nx m) (.nx)
          (:px m) (.px (:px m))
          (:xx m) (.xx))))

(def custom-appliers
  {'com.lambdaworks.redis.SetArgs `set-args})

(defn apply-arguments-list
  [method]
  (for [[i t] (map-indexed vector (:parameter-types method))]
    (let [s (symbol (str "arg" (inc i)))]
      (if-let [custom (custom-appliers t)]
        (list custom s)
        s))))

(defn defcommand
  [n methods {:keys [get-api result-f] :or {result-f 'identity}}]
  `(defn ~n
     ~@(for [method methods]
         `([client# ~@(arguments-list method)]
           (~result-f (. (~get-api client#) ~(:name method) ~@(apply-arguments-list method)))))))

(defn defcommands-body
  [interfaces params]
  (let [members (for [i interfaces
                      :let [[prefix i'] (if (vector? i) i [nil i])
                            reflection (r/type-reflect i')
                            members (:members reflection)]
                      m members
                      :when (and ((:flags m) :public) ((:flags m) :abstract))]
                  (assoc m ::name (symbol (str prefix (:name m)))))
        methods (group-by ::name members)]
    (map (fn [[k v]]
           (let [methods' (group-by #(count (:parameter-types %)) v)]
             ;; only one method from each arity
             (defcommand k (map first (vals methods')) params))) methods)))

(defmacro defcommands*
  [interfaces]
  `(do ~@(defcommands-body interfaces)))

(defn defcommands
  [interfaces params]
  (let [body (defcommands-body interfaces params)]
    (clojure.core/eval (cons `do body))))
