(ns raphanus.decode-test
  (:require [clojure.test :refer :all]
             [raphanus.decoder :as decoder]))

(defn buf
  [data]
  (-> (io.netty.buffer.Unpooled/buffer 2)
      (.writeBytes (.getBytes data))))

(defn decode
  [data]
  (-> (decoder/decode decoder/type-reader (buf data))
      (second)))

(deftest types
  (testing "int"
    (is (= [100500] (decode ":100500\r\n"))))
  (testing "string"
    (is (= ["hello"] (decode "+hello\r\n")))
    (is (= [""] (decode "+\r\n"))))
  (testing "error"
    (is (= {:type "ERR" :message "unknown command"}
           (-> (decode "-ERR unknown command\r\n") first (ex-data)))))
  (testing "bulk"
    (is (= [nil] (decode "$-1\r\n")))
    (is (= (seq (.getBytes "Hello world!\nAnd happy new year!"))
           (-> (decode "$32\r\nHello world!\nAnd happy new year!\r\n") first seq))))
  (testing "array"
    (is (= [nil] (decode "*-1\r\n")))
    (let [res (first (decode "*5\r\n:100\r\n:2\r\n:301\r\n:4\r\n$6\r\nfoobar\r\n"))]
      (is (= [100 2 301 4] (take 4 res)))
      (is (= ["foobar"] (map #(String. %) (drop 4 res))))))
  (testing "multiple"
    (is (= ["hello" 100500] (decode "+hello\r\n:100500\r\n"))))
  )

;; (deftest cont
;;   (let [bb2 (buf "00\r\n$6\r\nfoo")
;;         [decoder msgs] (decoder/decode decoder/type-reader (buf ":1"))
;;         [decoder msgs2] (decoder/decode decoder bb2)
;;         [decoder msgs3] (decoder/decode decoder (add-data bb2 "bar\r\n"))]
;;     (is (= [] msgs))
;;     (is (= [100] msgs2))
;;     (is (= "foobar" (String. (first msgs3))))))
