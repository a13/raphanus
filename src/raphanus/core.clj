(ns raphanus.core
  (:require [raphanus.commands :as commands]
            [raphanus.conn :as conn]
            [raphanus.codec :as codec]
            [raphanus.utils :as utils]
            [clojure.core.async :as a])
  (:refer-clojure :exclude [time sort sync set keys eval get type]))

(defn enqueue
  [driver data return-f]
  (a/go
    (let [v (a/<! (conn/send driver data))]
      (if (utils/throwable? v)
        v
        (return-f v)))))

(commands/defcommands enqueue)

(comment


  (def r (send c (map #(.getBytes %) ["SET" "foooz" "barz"])))

  (def c (a/<!! (connection "127.0.0.1" 6379)))


  (require '[raphanus.sync :as sync])

  (def c (sync/cluster [{:host "10.19.0.172" :port 6379}
                        {:host "10.19.0.135" :port 6379}
                        {:host "10.19.0.78" :port 6379}
                        {:host "10.19.0.184" :port 6379}
                        {:host "10.19.0.138" :port 6379}
                        {:host "10.19.0.157" :port 6379}]))

  (defn bencha
    [c]
    (dotimes [i 10]
      (let [res (sync/set c (str  "test:foo" (rand-int 10)) (str "hui" (rand-int 1000)))]
        (when (not= "OK" res)
          (prn "---RES" (String. res)))
        (assert (= "OK" res)))
      (when (nil? (sync/get c (str "test:foo" (rand-int 10))))
        (prn "---NIL"))))

  (defn full-bencha
    [c]
    (->> (range 10)
         (map (fn [_] (doto (Thread. #(bencha c)) (.start))))
         (map #(.join %))
         doall))

  )
