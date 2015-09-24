(ns raphanus.sync
  (:require [raphanus.commands :as commands]
            [raphanus.core :as core])
  (:import [com.lambdaworks.redis.api StatefulRedisConnection]
           [com.lambdaworks.redis.api.sync RedisStringCommands RedisListCommands RedisScriptingCommands
            RedisServerCommands]
           [com.lambdaworks.redis.cluster.api.sync RedisAdvancedClusterCommands
            NodeSelectionStringCommands NodeSelectionScriptingCommands NodeSelectionServerCommands])
  (:refer-clojure :exclude [time sort sync set keys eval get type]))

(defn sync*
  [component]
  (let [conn (:conn component)]
    (.sync conn)))

(commands/defcommands [RedisListCommands RedisStringCommands RedisScriptingCommands
                       RedisServerCommands
                       ['cluster- RedisAdvancedClusterCommands]]
  {:get-api `sync*})

(commands/defcommands [['node- NodeSelectionStringCommands]
                       ['node- NodeSelectionScriptingCommands]]
  {:get-api `identity})

;; (defmacro with-lock
;;   [driver key options & body]
;;   `(sync! (extra/with-lock ~driver ~key ~options ~@body)))

(comment
  (require 'defcomponent)
  (def s (defcomponent/system [raphanus.core/client] {:start true :params [{:host "localhost" :port 6379}]}))
  (def c (clojure.core/get s raphanus.core/client))
  (def s2 (defcomponent/system [raphanus.core/cluster] {:start true :params [[{:host "localhost" :port 30001}]]}))
  (def c2 (clojure.core/get s2 raphanus.core/cluster))

  (raphanus.lua/lua c "return 1" :integer [] [])

  (def sel (cluster-nodes c2 (core/->predicate (constantly true)))))


