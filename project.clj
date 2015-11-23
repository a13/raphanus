(defproject raphanus "0.1.21"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[defcomponent "0.1.4"]
                 [org.clojure/tools.logging "0.3.1"]
                 [biz.paluch.redis/lettuce "4.0.Final"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.7.0"]
                                  [org.slf4j/slf4j-api "1.7.7"]
                                  [ch.qos.logback/logback-classic "1.1.2"]
                                  [org.slf4j/log4j-over-slf4j "1.7.7"]
                                  [org.slf4j/jul-to-slf4j "1.7.7"]
                                  [org.slf4j/jcl-over-slf4j "1.7.7"]]}}
  :deploy-repositories [["releases" :clojars]])

