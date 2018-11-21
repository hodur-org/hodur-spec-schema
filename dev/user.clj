(ns user
  (:require [clojure.repl :refer :all]
            [clojure.tools.namespace.repl :as repl]))

(repl/set-refresh-dirs "src" "dev")

(defn reset [] (repl/refresh))
