(ns raphanus.codec.json
  (:require [cheshire.core :as json]
            [raphanus.codec :as codec]))

(defn mk
  [keyword-fn]
  (reify codec/Codec
   (encode [this v] (.getBytes (json/generate-string v)))
   (decode [this v] (json/parse-string (String. v) keyword-fn))))
