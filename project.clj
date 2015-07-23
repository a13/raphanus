(defproject raphanus "0.1.5-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [io.netty/netty-all "4.0.29.Final"]]
  :profiles {:dev {:dependencies [[cheshire "5.5.0"]
                                  [org.slf4j/slf4j-api "1.7.7"]
                                  [ch.qos.logback/logback-classic "1.1.2"]
                                  [org.slf4j/log4j-over-slf4j "1.7.7"]
                                  [org.slf4j/jul-to-slf4j "1.7.7"]
                                  [org.slf4j/jcl-over-slf4j "1.7.7"]
                                  [com.stuartsierra/component "0.2.3"]
                                  [io.prometheus/simpleclient "0.0.10"]
                                  [criterium "0.4.3"]]}}
  :deploy-repositories [["releases" :clojars]])

