(ns test-fns
  (:require [clojure.test.check.generators :as gen]))

(defn email? [s]
  (let [email-regex #"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,63}$"]
    (re-matches email-regex s)))

(defn keyword-gen []
  gen/keyword)

(defn email-gen []
  (gen/elements #{"asd@qwe.com" "qwe@asd.com" "foo@bar.edu" "bar@edu.br"}))
