(ns raphanus.extra
  (:require [raphanus.core :as core]
            [raphanus.lua :as lua]))

;; (defn acquire-lock
;;   [driver key {:keys [expire]}]
;;   (a/go
;;     (let [token (rand-int 2000)]
;;       (a/<! (core/set (assoc driver :timeout (+ expire 1000)) key token "NX" "PX" expire))
;;       token)))

;; (lua/defscript release-lock (lua/from "raphanus/release_lock.lua"))

;; (defmacro with-lock
;;   [driver key options & body]
;;   `(utils/safe-go
;;     (let [token# (utils/<? (acquire-lock ~driver ~key ~options))]
;;       (try
;;         ~@body
;;         (finally
;;           (utils/<? (release-lock ~driver [~key] [token#])))))))
