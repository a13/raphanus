(ns raphanus.commands
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [raphanus.codec :as codec]
            [raphanus.tracer :as tracer]))

(def default-codec-keys
  {'key :key
   'value :value
   :more :multiple-key
   :default :default
   :return :return})

(def codec-keys
  {"GET" {:return :value}
   "LPUSH" {:more :multiple-value}})

(defn cmd-codec-keys
  [cmd-name]
  (merge default-codec-keys (codec-keys cmd-name)))

(defn- args-transformer
  [codecs cmd-name fn-args]
  (let [cmd-parts (vec (str/split cmd-name #" ")) ; ["CONFIG" "SET"]
        codec-keys' (cmd-codec-keys cmd-name)
        [args-main [_ & args-more]] (split-with #(not= '& %) fn-args)
        args (if args-more `(concat ~(vec args-main) ~'args) (vec args-main))]
    `(let ~(-> (for [a args-main
                     :let [codec-key (codec-keys' a (codec-keys' :default))]] 
                 [a `(codec/encode (~codec-key ~codecs) ~a)])
               (cond->
                   args-more (conj ['args `(codec/encode (~(codec-keys' :more) ~codecs) ~'args)]))
               (->>
                (apply concat)
                (vec)))
       (into (mapv #(.getBytes ^String %) ~cmd-parts) ~args))))

(defn- args->params-vec
  "Parses refspec argument map into simple defn-style parameter vector:
  '[key value & more], etc."
  [args]
  (let [num-non-optional (count (take-while #(not (:optional %)) args))
        num-non-multiple (count (take-while #(not (:multiple %)) args))

        ;; Stop explicit naming on the 1st optional arg (exclusive) or 1st
        ;; multiple arg (inclusive)
        num-fixed        (min num-non-optional (inc num-non-multiple))

        fixed-args       (->> args (take num-fixed)
                              (map :name) flatten (map symbol) vec)
        has-more? (seq (filter #(or (:optional %) (:multiple %)) args))]
    (if has-more? (conj fixed-args '& 'args) fixed-args)))

(defn- args->params-docstring
  "Parses refspec argument map into Redis reference-doc-style explanatory
  string: \"BRPOP key [key ...] timeout\", etc."
  [args]
  (let [parse
        #(let [{:keys [command type name enum multiple optional]} %
               name (if (and (coll? name) (not (next name))) (first name) name)
               s (cond command (str command " "
                                    (cond enum         (str/join "|" enum)
                                          (coll? name) (str/join " " name)
                                          :else name))
                       enum (str/join "|" enum)
                       :else name)
               s (if multiple (str s " [" s " ...]") s)
               s (if optional (str "[" s "]") s)]
           s)]
    (str/join " " (map parse args))))

(defn result-f
  [driver cmd-name return-key args]
  (let [start (System/nanoTime)
        return-codec ((:codecs driver) return-key)]
    (fn [res]
      (when-let [tracer (:tracer driver)]
        (tracer/request-completed tracer cmd-name args res (- (System/nanoTime) start)))
      (when-not (= :raphanus/null res)
        (codec/decode return-codec res)))))

(defmacro defcommand [enqueue-f {cmd-name :name args :arguments :as refspec}]
  (let [fn-name      (-> cmd-name (str/replace #" " "-") str/lower-case)
        fn-docstring (str cmd-name " "
                          (args->params-docstring args)
                          "\n\n" (:summary refspec) ".\n\n"
                          "Available since: " (:since refspec) ".\n\n"
                          "Time complexity: " (:complexity refspec))

        fn-args (args->params-vec args)  ; ['key 'value '& 'more]
        all-args (-> (concat ['driver] fn-args)
                     vec)
        codecs-sym (gensym "codecs")
        req-args (args-transformer codecs-sym cmd-name fn-args)]

    `(defn ~(symbol fn-name)
       {:doc ~fn-docstring
        :redis-api (or (:since ~refspec) true)}
       ~all-args
       (let [~codecs-sym (get ~'driver :codecs)
             request# ~req-args]
         (~enqueue-f ~'driver request# (result-f ~'driver ~cmd-name ~(:return (cmd-codec-keys cmd-name))
                                                 ~(vec (filter #(not= '& %) fn-args))))))))


(defn command-specs
  []
  (-> (io/resource "raphanus/commands.edn")
      slurp
      edn/read-string))

(defmacro defcommands [enqueue-f]
  (let [refspec (command-specs)]
    `(do ~@(map (fn [v] `(defcommand ~enqueue-f ~v))
                refspec))))

(comment
  (require 'cheshire.core)
  (-> (slurp "resources/raphanus/commands.json")
      (cheshire.core/parse-string true)
      (->> (mapv (fn [[k v]] (assoc v :name (name k))))
           (spit "resources/raphanus/commands.edn")))

  (defcommand identity {:name "APPEND", :summary "Append a value to a key", :complexity "O(1). The amortized time complexity is O(1) assuming the appended value is small and the already present value is of any size, since the dynamic string library used by Redis will double the free space available on every reallocation.", :arguments [{:name "key", :type "key"} {:name "value", :type "string"}], :since "2.0.0", :group "string"})
  (defcommand identity {:name "SET", :summary "Set the string value of a key", :complexity "O(1)", :arguments [{:name "key", :type "key"} {:name "value", :type "string"} {:command "EX", :name "seconds", :type "integer", :optional true} {:command "PX", :name "milliseconds", :type "integer", :optional true} {:name "condition", :type "enum", :enum ["NX" "XX"], :optional true}], :since "1.0.0", :group "string"})
  )
