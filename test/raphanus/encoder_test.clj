(ns raphanus.encoder-test
  (:require [raphanus.encoder :as encoder]
            [raphanus.decoder :as decoder]
            [clojure.test :refer :all])
  (:import [java.nio.charset Charset]))

(deftest encode
  (is (= "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$11\r\nHello world\r\n"
         (.toString (encoder/encode (map #(.getBytes %) ["SET" "foo" "Hello world"])) (Charset/forName "UTF-8"))))

  (testing "bidirectional"
    (is (= ["SET" "foo" "foobar"]
           (-> (map #(.getBytes %) ["SET" "foo" "foobar"])
               encoder/encode
               (->>
                (decoder/decode decoder/type-reader)
                second first
                (map #(String. %))))))))
