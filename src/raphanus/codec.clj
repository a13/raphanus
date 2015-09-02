(ns raphanus.codec)

(defprotocol Codec
  (encode [this v])
  (decode [this v]))

(def id
  (reify Codec
    (encode [this v] v)
    (decode [this v] v)))

(def string
  (reify Codec
    (encode [this v] (.getBytes (str v)))
    (decode [this v] (String. ^bytes v))))

(defn multiple
  [codec]
  (reify Codec
    (encode [this v] (map (partial encode codec) v))
    (decode [this v] (map (partial decode codec) v))))

(def multiple-string (multiple string))

(def default-codecs-dict
  {:key string
   :value string
   :default string
   :multiple-key multiple-string
   :multiple-value multiple-string
   :return id})
