(ns raphanus.encoder
  (:require [clojure.core.async :as a]))

(defn encode
  [seq-of-bytes]
  (io.netty.buffer.Unpooled/wrappedBuffer
   (into-array (into [(.getBytes (str "*" (count seq-of-bytes) "\r\n"))]
                     (interleave (map #(.getBytes (str "$" (count %) "\r\n")) seq-of-bytes)
                                 seq-of-bytes
                                 (repeat (.getBytes "\r\n")))))))

(defn with-encoder
  [sink]
  (let [sink' (a/chan 20)]
    (a/go-loop []
      (if-let [v (a/<! sink')]
        (do (a/>! sink (encode v))
            (recur))
        (a/close! sink)))
    sink'))
