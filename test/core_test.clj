(ns core-test
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.string :as string]
            [clojure.test :refer :all]
            [hodur-engine.core :as engine]
            [hodur-spec-schema.core :as hodur-spec]
            [test-fns]))

(def basic-schema
  '[^{:spec/tag true}
    default

    Person
    [^String first-name
     ^{:type String
       :optional true}
     middle-name
     ^String last-name
     ^Gender gender
     ^Float height
     [^Unit unit]]

    ^:enum
    Gender
    [MALE FEMALE]

    ^{:implements Animal}
    Pet
    [^String name
     ^DateTime dob]

    ^{:interface true
      :spec/alias :beings/animal}
    Animal
    [^String race]
    
    ^:union
    SearchResult
    [Person Pet]

    ^:enum
    Unit
    [METERS FEET]

    QueryRoot
    [^{:type SearchResult
       :cardinality [0 n]}
     search
     [^String term
      ^{:type Integer
        :optional true}
      limit
      ^{:type Integer
        :optional true}
      offset]]])

(def circular-schema
  '[^{:spec/tag true}
    default

    Person
    [^String name
     ^{:type Person
       :optional true} parent
     ^{:type Boolean
       :optional true} is-related
     [^Person other-person]]])

(def cardinality-schema
  '[^{:spec/tag true}
    default

    PersonTwo
    [^String name
     ^GenderTwo gender]
    
    ^:enum
    GenderTwo
    [MALE FEMALE UNKOWN]

    CardinalityEntity
    [^{:type String
       :cardinality [0 n]}
     many-strings
     ^{:type String
       :optional true
       :cardinality [0 n]}
     many-strings-optional
     ^{:type GenderTwo
       :cardinality [0 n]}
     many-genders
     ^{:type PersonTwo
       :cardinality [0 n]}
     many-people
     ^{:type PersonTwo
       :cardinality [3 5]}
     exactly-three-to-five-people
     ^{:type String
       :cardinality [4 4]}
     exactly-four-strings
     ^{:type Integer
       :cardinality [0 n]
       :spec/distinct true}
     distinct-integers
     ^{:type Integer
       :cardinality [0 n]
       :spec/distinct true
       :spec/kind list?}
     distinct-integers-in-a-list]])

(def aliases-schema
  '[^{:spec/tag true}
    default

    ^{:spec/alias :my-entity/alias}
    AliasesEntity
    [^{:type String
       :spec/alias [:my-field/alias1
                    :my-field/alias2]}
     an-aliased-field
     [^{:type String
        :spec/alias :my-param/alias}
      an-aliased-param]]])

(def extend-override-schema
  '[^{:spec/tag true}
    default

    ExtendOverrideEntity
    [^{:type String
       :spec/extend test-fns/email?
       :spec/gen test-fns/email-gen}
     email-field
     ^{:spec/override keyword?
       :spec/gen test-fns/keyword-gen}
     keyword-field
     [^{:spec/override keyword?
        :spec/gen test-fns/keyword-gen}
      keyword-param]]])

(def meta-db-basic (engine/init-schema basic-schema))

(def meta-db-circular (engine/init-schema circular-schema))

(def meta-db-cardinality (engine/init-schema cardinality-schema))

(def meta-db-aliases (engine/init-schema aliases-schema))

(def meta-db-extend-override (engine/init-schema extend-override-schema))

(deftest basic-with-and-without-prefix
  (let [res-no-prefix (hodur-spec/defspecs meta-db-basic)
        res-prefix (hodur-spec/defspecs meta-db-basic {:prefix :app})]
    (is (= (count res-no-prefix)
           (count res-prefix)))
    (is (= 38 (count res-prefix)))
    (is (= 37 (count (filter #(string/starts-with? (namespace %) "core-test")
                             res-no-prefix))))
    (is (= 37 (count (filter #(string/starts-with? (namespace %) "app")
                             res-prefix))))

    (is (s/valid? :core-test/animal {:race "Human"}))
    (is (s/valid? :core-test/animal {:race "Human"}))

    (is (s/valid? :core-test/animal {:race "Human"}))

    (is (s/valid? :core-test.person.height/unit "METERS"))

    (is (s/valid? :core-test/pet {:name "bla" :dob #inst "2000-10-10" :race "cat"}))

    (is (s/valid? :core-test/person {:first-name "Tiago"
                                     :last-name "Luchini"
                                     :gender "MALE"
                                     :height 1.78}))

    (is (not (s/valid? :core-test/person {:firs-name "Tiago"
                                          :middle-name nil
                                          :last-name "Luchini"
                                          :gender "MALE"
                                          :height 1.78})))

    (is (s/valid? :core-test.query-root/search
                  [{:name "Lexie"
                    :dob #inst "2016-10-10"
                    :race "Dog"}
                   {:first-name "Tiago"
                    :last-name "Luchini"
                    :gender "MALE"
                    :height 1.78}]))

    (is (s/valid? :core-test.query-root/search%
                  {:term "my search"}))

    (is (s/valid? :core-test.query-root/search%
                  {:term "my search"
                   :limit 4
                   :offset 5}))

    (is (not (s/valid? :core-test.query-root/search%
                       {:term 1
                        :limit ""
                        :offset nil})))

    (is (s/valid? :core-test.query-root/search-ordered%
                  ["my search" 1 5]))

    (is (not (s/valid? :core-test.query-root/search-ordered%
                       ["my search"])))))

(deftest schema-and-macros-should-yield-the-same
  (let [res-macro (hodur-spec/defspecs meta-db-basic)
        res-schema (hodur-spec/schema meta-db-basic)]
    (is (= (count res-macro)
           (count res-schema)))
    (is (= 38 (count res-schema)))))

(deftest cardinalities-should-work
  (hodur-spec/defspecs meta-db-cardinality)

  (is (s/valid? :core-test.cardinality-entity/many-strings []))

  (is (s/valid? :core-test.cardinality-entity/many-strings ["foo" "bar"]))

  (is (s/valid? :core-test.cardinality-entity/many-strings-optional nil))
  (is (s/valid? :core-test.cardinality-entity/many-strings-optional []))
  (is (s/valid? :core-test.cardinality-entity/many-strings-optional ["foo" "bar"]))

  (is (s/valid? :core-test.cardinality-entity/many-genders ["MALE" "UNKOWN"]))

  (is (s/valid? :core-test.cardinality-entity/exactly-four-strings ["foo" "bar" "foo2" "bar2"]))

  (is (s/valid? :core-test.cardinality-entity/exactly-three-to-five-people
                [{:name "Name" :gender "MALE"}
                 {:name "Name" :gender "MALE"}
                 {:name "Name" :gender "MALE"}]))

  (is (s/valid? :core-test.cardinality-entity/exactly-three-to-five-people
                [{:name "Name" :gender "MALE"}
                 {:name "Name" :gender "MALE"}
                 {:name "Name" :gender "MALE"}
                 {:name "Name" :gender "MALE"}
                 {:name "Name" :gender "MALE"}]))

  (is (not (s/valid? :core-test.cardinality-entity/exactly-three-to-five-people
                     [{:name "Name" :gender "MALE"}])))
  
  (is (s/valid? :core-test.cardinality-entity/distinct-integers [1 2 3]))

  (is (not (s/valid? :core-test.cardinality-entity/distinct-integers [1 2 3 2])))

  (is (s/valid? :core-test.cardinality-entity/distinct-integers-in-a-list '(1 2 3)))

  (is (not (s/valid? :core-test.cardinality-entity/distinct-integers-in-a-list [1 2 3]))))

(hodur-spec/defspecs meta-db-aliases)

(deftest aliases-should-work
  (is (s/valid? :my-param/alias "foo"))
  (is (s/valid? :my-field/alias1 "bar"))
  (is (s/valid? :my-field/alias2 "foo"))
  (is (s/valid? :my-entity/alias {:an-aliased-field "bar"})))

(hodur-spec/defspecs meta-db-extend-override)

(deftest extends-and-overrides-should-work
  (is (s/valid? :core-test.extend-override-entity/keyword-field :foo))

  (is (not (s/valid? :core-test.extend-override-entity/keyword-field "foo")))
  
  (is (s/valid? :core-test.extend-override-entity/email-field "qwe@asd.com"))

  (is (not (s/valid? :core-test.extend-override-entity/email-field "foobar"))))

(hodur-spec/defspecs meta-db-extend-override {:prefix :app})

(deftest custom-generators-should-work
  (doseq [k (gen/sample (s/gen :app.extend-override-entity/keyword-field))]
    (is (keyword? k))
    (is (s/valid? :app.extend-override-entity/keyword-field k)))

  (doseq [email (gen/sample (s/gen :app.extend-override-entity/email-field))]
    (is (test-fns/email? email))
    (is (s/valid? :app.extend-override-entity/email-field email)))

  (doseq [entity (gen/sample (s/gen :app/extend-override-entity))]
    (is (s/valid? :app/extend-override-entity entity))))

(hodur-spec/defspecs meta-db-circular {:prefix :circular})

(deftest circular-reference-should-work
  (is (s/valid? :circular/person {:name "Foo"
                                  :parent {:name "Bar"}}))
  (is (s/valid? :circular.person/is-related% {:other-person {:name "Foo"}}))
  (is (s/valid? :circular.person/is-related true)))
