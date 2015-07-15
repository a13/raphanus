(ns raphanus.socket
  (:require [clojure.core.async :as a]
            [clojure.tools.logging :as log])
  (:import [io.netty.channel Channel ChannelHandler ChannelInboundHandler ChannelOutboundHandler ChannelOption
            ChannelHandlerContext]
           [io.netty.channel.epoll Epoll EpollEventLoopGroup EpollSocketChannel]
           [io.netty.channel.nio NioEventLoopGroup]
           [io.netty.bootstrap Bootstrap]
           [io.netty.channel.socket.nio NioSocketChannel]
           [io.netty.util.concurrent DefaultThreadFactory]
           [io.netty.util.internal SystemPropertyUtil]))

(definline release [x]
  `(io.netty.util.ReferenceCountUtil/release ~x))

(defn handler
  [connect-ch]
  (let [in (a/chan 20) out (a/chan 20)
        write-notifier (a/chan)
        close! (fn []
                 (a/close! write-notifier)
                 (a/close! in))
        write-loop (fn [^Channel ch]
                     (a/go
                       (loop []
                         (if (.isWritable ^Channel ch)
                           (let [res (a/alt!
                                       out ([val _] (if val
                                                      val
                                                      :closed))
                                       :default :empty)]
                             (case res
                               :closed (do (.flush ^Channel ch)
                                           (.close ^Channel ch)
                                           (close!))
                               :empty (do (.flush ^Channel ch)
                                          (if-let [v (a/<! out)]
                                            (do (.write ^Channel ch v)
                                                (recur))
                                            (do (.close ^Channel ch)
                                                (close!))))
                               (do (.write ^Channel ch res)
                                   (recur))))
                           (do
                             (.flush ^Channel ch)
                             (a/<! write-notifier)
                             (recur))))))]
    (reify
      ChannelHandler
      (exceptionCaught [this ctx e]
        (log/error e "Error in connection")
        (close!))
      (handlerAdded [this ctx])
      (handlerRemoved [this ctx])
      ChannelInboundHandler
      (channelActive [this ctx]
        (write-loop (.channel ctx))
        (a/put! connect-ch {:in in :out out}))
      (channelInactive [this ctx]
        (close!))
      (channelRead [this ctx msg]
        (let [res (a/alt!!
                    [[in msg]] ([val _] (if val :sent :closed))
                    :default :cant-sent)]
          (when (= :cant-sent res)
            (-> ctx .channel .config (.setAutoRead false))
            (a/go
              (a/>! in msg)
              (-> ^ChannelHandlerContext ctx .channel .config (.setAutoRead true))))))
      (channelReadComplete [this ctx])
      (channelRegistered [this ctx])
      (channelUnregistered [this ctx])
      (channelWritabilityChanged [this ctx]
        (a/put! write-notifier :hey!))
      (userEventTriggered [this ctx e])
      ChannelOutboundHandler
      (bind [this ctx local-address promise]
        (.bind ctx local-address promise))
      (close [this ctx promise]
        (.close ctx promise)
        ;; (log/error (Exception. "Can't connect") "Connection error")
        (a/close! connect-ch))
      (connect [this ctx remote-address local-address promise]
        (.connect ctx remote-address local-address promise))
      (deregister [this ctx promise]
        (.deregister ctx promise))
      (disconnect [this ctx promise]
        (.disconnect ctx promise))
      (flush [this ctx] 
        (.flush ctx))
      (read [this ctx]
        (.read ctx))
      (write [this ctx msg promise]
        (.write ctx msg promise)))))

(defn epoll? []
  (Epoll/isAvailable))
 
(defn get-default-event-loop-threads
  "Determines the default number of threads to use for a Netty EventLoopGroup.
   This mimics the default used by Netty as of version 4.1."
  []
  (let [cpu-count (->> (Runtime/getRuntime) (.availableProcessors))]
    (max 1 (SystemPropertyUtil/getInt "io.netty.eventLoopThreads" (* cpu-count 2)))))

(def ^String client-event-thread-pool-name "raphanus-netty-client-event-pool")

(defonce client-group
  (let [thread-count (get-default-event-loop-threads)
        thread-factory (DefaultThreadFactory. client-event-thread-pool-name true)]
    (if (epoll?)
      (EpollEventLoopGroup. (long thread-count) thread-factory)
      (NioEventLoopGroup. (long thread-count) thread-factory))))

(defn connect
  [^String host ^Integer port]
  (let [connect-ch (a/chan)
        h (handler connect-ch)
        channel (if (epoll?)
                  EpollSocketChannel
                  NioSocketChannel)
        b (doto (Bootstrap.)
            (.option ChannelOption/SO_REUSEADDR true)
            (.option ChannelOption/MAX_MESSAGES_PER_READ Integer/MAX_VALUE)
            (.group client-group)
            (.channel channel)
            (.handler h))]
    (.connect b host port)
    connect-ch))
