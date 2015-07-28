(ns raphanus.conn
  (:require [clojure.core.async :as a]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [raphanus.codec :as codec]
            [raphanus.crc16 :as crc16]
            [raphanus.decoder :as decoder]
            [raphanus.encoder :as encoder]
            [raphanus.socket :as socket]
            [raphanus.utils :as utils])
  (:refer-clojure :exclude [send]))

(defn attach-codec
  [conn]
  {:in (decoder/with-decoder (:in conn))
   :out (encoder/with-encoder (:out conn))})

(def DRIVER-BUFFER 40)

;; a/map waits all channels even if any of them is already closed. and drops channel values.
(defn pairs
  [ch1 ch2 & [buf-or-n]]
  (let [out (a/chan buf-or-n)]
    (a/go-loop [ch1-v nil ch2-v nil]
      (cond
        (and ch1-v ch2-v)
        (do (a/>! out (vector ch1-v ch2-v))
            (recur nil nil))
        ch1-v
        (if-let [v (a/<! ch2)]
          (recur ch1-v v)
          (a/close! out))
        ch2-v
        (if-let [v (a/<! ch1)]
          (recur v ch2-v)
          (a/close! out))
        :else
        (a/alt!
          ch1 ([v] (if v
                     (recur v nil)
                     (a/close! out)))
          ch2 ([v] (if v
                     (recur nil v)
                     (a/close! out))))))
    out))

(defn driver
  [conn options]
  (let [promise-queue (a/chan DRIVER-BUFFER)
        requests (a/chan DRIVER-BUFFER)
        replies (pairs (:in conn) promise-queue DRIVER-BUFFER)]
    (a/go-loop []
      (if-let [[reply promise] (a/<! replies)]
        (do (a/put! promise reply)
            (a/close! promise)
            (recur))
        (do (a/close! promise-queue)
            (a/close! requests)
            (loop []
              (when-let [v (a/<! promise-queue)]
                (a/put! v (ex-info "Connection closed" {:type ::connection-closed}))
                (a/close! v)
                (recur))))))
    (a/go-loop []
      (if-let [v (a/<! requests)]
        (do (a/>! promise-queue (:promise v))
            (a/>! (:out conn) (:data v))
            (recur))
        (do (a/close! (:out conn))
            (a/close! promise-queue))))
    {:requests requests
     :desc (format "TCP connect to %s:%s" (:host options) (:port options))
     :codecs (merge codec/default-codecs-dict (:codecs options))
     :timeout (:timeout options 1000)}))

(defmacro <!-with-timeout
  [ch timeout]
  `(let [timeout# ~timeout]
     (if timeout#
       (a/alt!
         ~ch ([v#] v#)
         (a/timeout timeout#) (ex-info "Read timeout" {:type ::timeout}))
       (a/<! ~ch))))

(defmacro >!-with-timeout
  [ch v timeout]
  `(let [timeout# ~timeout]
     (if timeout#
       (a/alt!
         [[~ch ~v]] true
         (a/timeout timeout#) (ex-info "Write timeout" {:type ::timeout}))
       (a/>! ~ch ~v))))

(defn mk
  [host port & [options]]
  (let [conn (socket/connect host port)]
    (a/go
      (let [res (<!-with-timeout conn (:connect-timeout options))]
        (cond
          (utils/throwable? res) res
          (nil? res) nil
          :else (-> res attach-codec (driver (assoc options :host host :port port))))))))

(defn send
  [driver data & [options]]
  (let [promise (a/chan)]
    (a/go
      (let [res (>!-with-timeout (:requests driver) {:promise promise :data data} (:timeout driver))]
        (cond
          (utils/throwable? res) res
          (not (nil? res)) (<!-with-timeout promise (:timeout driver))
          :else (ex-info "Connection closed" {:type ::connection-closed}))))))

(defn persistent
  [host port & [options]]
  (a/go
    (let [connect (fn [] (mk host port (assoc options :connect-timeout 10000)))
          infinity-connect (fn []
                             (a/go-loop []
                               (let [conn (a/<! (connect))]
                                 (if (or (nil? conn) (utils/throwable? conn))
                                   (do (log/warn "Cant't connect, reconnecting" host port)
                                       (a/<! (a/timeout 1000))
                                       (recur))
                                   conn))))
          conn (a/<! (connect))]
      (if (or (nil? conn) (utils/throwable? conn))
        conn
        (let [requests (a/chan DRIVER-BUFFER)]
          (a/go-loop [conn conn prev-req nil]
            (if-let [v (or prev-req (a/<! requests))]
              (let [res (a/<! (send conn (:data v)))]
                (if (= ::connection-closed (:type (ex-data res)))
                  (recur (a/<! (infinity-connect)) v)
                  (do (a/put! (:promise v) res)
                      (recur conn nil))))
              (a/close! (:requests conn))))
          {:requests requests
           :desc (format "Persistent TCP connection to %s:%s" host port)
           :codecs (:codecs conn)})))))

(def HASH-SLOTS 16384)

(defn command->key
  [command]
  ;; too far from ellegance
  (String. (case (String. (first command))
             ;; calculate slot based only on first key
             ("eval" "evalsha") (nth command 3)
             (if (< 1 (count command))
               (nth command 1)
               (first command)))))

(defn key->slot
  [key]
  (let [s (.indexOf key "{")
        key' (if (not= -1 s)
               (let [e (.indexOf key "}" (inc s))]
                 (if (and (not= -1 e) (not= (inc s) e))
                   (.substring key (inc s) (dec e))
                   key))
               key)
        hash (crc16/crc16 key')]
    (mod hash HASH-SLOTS)))

(defn get-conn
  [connections slots command]
  (let [key (command->key command)
        slot (key->slot key)
        host (slots slot)]
    (if host
      (connections host)
      (rand-nth (vals connections)))))

(defn cluster
  [hosts & [options]]
  (let [init-connections (fn []
                           (a/go-loop [res {} [spec & hosts] hosts]
                             (if-not spec
                               res
                               (if-let [c (a/<! (persistent (:host spec) (:port spec) options))]
                                 (recur (assoc res (format "%s:%s" (:host spec) (:port spec)) c) hosts)
                                 (recur res hosts)))))

        requests (a/chan 20)]
    (a/go
      (let [connections (a/<! (init-connections))]
        (if (empty? connections)
          nil
          (do (a/go-loop [slots {} connections connections asking nil prev-req nil]
                (if-let [v (or prev-req (a/<! requests))]
                  (let [conn (or (connections asking) (get-conn connections slots (:data v)))
                        _ (when asking (send conn [(.getBytes "ASKING")]))
                        res (a/<! (send conn (:data v)))
                        err? (ex-data res)]
                    (cond
                      (or (= "MOVED" (:type err?)) (= "ASK" (:type err?)))
                      (let [[slot-str host-and-port] (str/split (:message err?) #" ")
                            [host port-str] (str/split host-and-port #":")
                            port (Integer. port-str)
                            slots' (if (= "MOVED" (:type err?))
                                     (assoc slots (Integer. slot-str) host-and-port)
                                     slots)
                            asking' (when (= "ASK" (:type err?))
                                      host-and-port)]
                        (if (connections host-and-port)
                          (recur slots' connections asking' v)
                          (if-let [conn (a/<! (persistent host port))]
                            (recur slots' (assoc connections host-and-port conn) asking' v)
                            (do (a/put! (:promise v)
                                        (ex-info "Can't connect" {:type ::cant-connect :host host :port port}))
                                (recur slots connections nil nil)))))
                      err?
                      (do (a/put! (:promise v) (ex-info "Error in cluster" (assoc err? :conn conn) res))
                          (recur slots connections nil nil))

                      :else
                      (do (a/put! (:promise v) res)
                          (recur slots connections nil nil))))
                  (doseq [c (vals connections)]
                    (a/close! (:requests c)))))
              {:requests requests
               :desc (format "Cluster connection to %s" (pr-str hosts))
               :codecs (merge codec/default-codecs-dict (:codecs options))}))))))
