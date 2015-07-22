(ns raphanus.decoder
  (:require [clojure.string :as str]
            [clojure.core.async :as a]
            [raphanus.socket :as socket]
            [clojure.tools.logging :as log])
  (:import [io.netty.buffer ByteBuf Unpooled]))

(declare type-reader)

(defn bb-as-string
  ([clb] (bb-as-string clb (StringBuilder.)))
  ([clb ^StringBuilder s]
   (fn [^ByteBuf bb]
     (let [c (char (.readByte bb))]
       (case c
         \return [(bb-as-string clb s) ::none]
         \newline (clb (str s))
         [(bb-as-string clb (.append s c)) ::none])))))

(defn bb-as-int
  [clb]
  (bb-as-string (fn [^String v] (clb (Integer. v)))))

(defn int-reader
  [next]
  (bb-as-int (fn [v] [next v])))

(defn string-reader
  [next]
  (bb-as-string (fn [s] [next s])))

(defn bulk-reader
  [next]
  (let [buf (Unpooled/buffer 1024)]
    (bb-as-int
     (fn [size]
       (letfn [(reader [^ByteBuf bb]
                 (.writeBytes buf bb (min (- (+ size 2) (.readableBytes buf)) (.readableBytes bb)))
                 (if (= (+ size 2) (.readableBytes buf))
                   (let [res (byte-array size)]
                     (.readBytes buf res)
                     (assert (= \return (char (.readByte buf))))
                     (assert (= \newline (char (.readByte buf))))
                     (socket/release buf)
                     [next res])
                   [reader ::none]))]
         (if (= -1 size)
           [next :raphanus/null]
           [reader ::none]))))))

(defn error-reader
  [next]
  (bb-as-string
   (fn [s] (let [[type message] (str/split s #" " 2)]
            [next (ex-info "Redis error" {:type type :message message})]))))

(defn array-reader
  [next]
  (bb-as-int
   (fn [size]
     (if (= -1 size)
       [next :raphanus/null]
       (letfn [(reader [acc current-reader]
                 (fn [bb]
                   (if (= size (count acc))
                     [next acc]
                     (let [[current-reader' message] (current-reader bb)]
                       (case message
                         (::none ::more-please) [(reader acc current-reader') ::none]
                         (let [acc' (conj acc message)]
                           (if (= size (count acc'))
                             [next acc']
                             [(reader acc' current-reader') ::none])))))))]
         [(reader [] type-reader) ::none])))))

(defn type-reader
  [^ByteBuf bb]
  (let [c (char (.readByte bb))
        f (case c
            \+ string-reader
            \- error-reader
            \: int-reader
            \$ bulk-reader
            \* array-reader
            (throw (Exception. (str "Parser error " (pr-str c)))))]
    [(f type-reader) ::none]))

(defn decode
  [reader ^ByteBuf bb]
  (loop [reader' reader acc []]
    (let [[reader'' message] (reader' bb)]
      (case message
        ::more-please [reader'' acc]
        ::none (if (.isReadable bb)
                 (recur reader'' acc)
                 (do (socket/release bb) [reader'' acc]))
        (if (.isReadable bb)
          (recur reader'' (conj acc message))
          (do (socket/release bb)
              [reader'' (conj acc message)]))))))

(comment
  (decode type-reader (io.netty.buffer.Unpooled/copiedBuffer (.getBytes "+OK\r\n"))))

(defn with-decoder
  [src]
  (let [dst (a/chan 20)]
    (a/go-loop [reader type-reader]
      (if-let [bb (a/<! src)]
        (let [[reader' msgs] (try
                               (decode reader bb)
                               (catch Exception e
                                 (log/error e "Error while decoding")
                                 (a/close! dst)))]
          (doseq [m msgs]
            (a/>! dst m))
          (recur reader'))
        (a/close! dst)))
    dst))
