(ns raphanus.core
  (:require [raphanus.commands :as commands]
            [raphanus.conn :as conn]
            [raphanus.codec :as codec]
            [raphanus.utils :as utils]
            [clojure.core.async :as a])
  (:refer-clojure :exclude [time sort sync set keys eval get type]))

(defn enqueue
  [driver data return-key]
  (a/go
    (let [v (a/<! (conn/send driver data))]
      (if (utils/throwable? v)
        v
        (codec/decode ((:codecs driver) return-key) v)))))

(commands/defcommands enqueue)

(comment

  (def r (send c (map #(.getBytes %) ["SET" "foooz" "barz"])))

  (def c (a/<!! (connection "127.0.0.1" 6379)))


  (def c (clojure.core.async/<!! (conn/persistent "127.0.0.1" 6379)))

  (defn ->bytes
    [coll]
    (map #(.getBytes %) coll))

  (defn bencha
    [c]
    (dotimes [i 1000]
      (let [res (a/<!! (send c (->bytes ["SET" "foooz" (str "hui" ;; (rand-int 1000)
                                                            )])))]
        (when (not= "OK" res)
          (prn "---RES" (String. res)))
        (assert (= "OK" res)))
      (assert (not-empty (a/<!! (send c (->bytes ["GET" "foooz"])))))))

  (defn full-bencha
    [c]
    (->> (range 10)
         (map (fn [_] (doto (Thread. #(bencha c)) (.start))))
         (map #(.join %))
         doall))

  )
