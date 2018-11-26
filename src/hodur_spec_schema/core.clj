(ns hodur-spec-schema.core
  (:require [clojure.spec.alpha :as s]
            [datascript.core :as d]
            [datascript.query-v3 :as q]
            [camel-snake-kebab.core :refer [->kebab-case-string]]
            [hodur-engine.utils :as hodur-utils]))

(defn ^:private get-topo-ids [conn]
  (hodur-utils/topological-sort conn
                                {:direction {:type->field-children :rtl
                                             :field->param-children :rtl
                                             :type->field-return :ltr
                                             :type->param-return :ltr
                                             :interface->type :ltr
                                             :union->type :ltr}
                                 :tag :spec/tag}))

(defn ^:private prepend-core-ns [sym]
  (when (not (nil? sym))
    (if (namespace sym)
      sym
      (symbol "clojure.core" (str sym)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(declare get-spec-form)

(defmulti ^:private get-spec-name
  (fn [obj opts]
    (cond
      (and (seqable? obj)
           (every? #(= :param (:node/type %)) obj))
      :param-group

      :default
      (:node/type obj))))

(defn ^:private default-prefix []
  (str (ns-name *ns*)))

(defn ^:private get-spec-entity-name
  [type-name
   {:keys [prefix] :or {prefix (default-prefix)}}]
  (keyword (name prefix)
           (->kebab-case-string type-name)))

(defn ^:private get-spec-field-name
  [type-name field-name
   {:keys [prefix] :or {prefix (default-prefix)}}]
  (keyword (str (name prefix) "." (->kebab-case-string type-name))
           (->kebab-case-string field-name)))

(defn ^:private get-spec-param-name
  [type-name field-name param-name
   {:keys [prefix] :or {prefix (default-prefix)}}]
  (keyword (str (name prefix) "." (->kebab-case-string type-name) "."
                (->kebab-case-string field-name))
           (->kebab-case-string param-name)))

(defn ^:private get-spec-param-group-name
  [type-name field-name
   {:keys [prefix params-postfix] :or {prefix (default-prefix)
                                       params-postfix "%"} :as opts}]
  (keyword (str (name prefix) "." (->kebab-case-string type-name))
           (str (->kebab-case-string field-name) params-postfix)))

(defmethod get-spec-name :type
  [{:keys [type/kebab-case-name]} opts]
  (get-spec-entity-name (name kebab-case-name) opts))

(defmethod get-spec-name :field
  [{:keys [field/kebab-case-name
           field/parent] :as field} opts]
  (get-spec-field-name (name (:type/kebab-case-name parent))
                       (name kebab-case-name)
                       opts))

(defmethod get-spec-name :param
  [param opts]
  (let [type-name (-> param :param/parent :field/parent :type/kebab-case-name)
        field-name (-> param :param/parent :field/kebab-case-name)
        param-name (-> param :param/kebab-case-name)]
    (get-spec-param-name type-name
                         field-name
                         param-name
                         opts)))

(defmethod get-spec-name :param-group
  [params opts]
  (let [type-name (-> params first :param/parent :field/parent :type/kebab-case-name)
        field-name (-> params first :param/parent :field/kebab-case-name)]
    (get-spec-param-group-name type-name
                               field-name
                               opts)))

(defmethod get-spec-name :default
  [obj opts]
  (if (nil? obj)
    nil
    (throw (ex-info "Unable to name a spec for object"
                    {:obj obj}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ^:private get-cardinality [dep-obj]
  (or (:field/cardinality dep-obj)
      (:param/cardinality dep-obj)))

(defn ^:private card-type [dep-obj]
  (let [cardinality (get-cardinality dep-obj)]
    (when cardinality
      (if (and (= 1 (first cardinality))
               (= 1 (second cardinality)))
        :one
        :many))))

(defn ^:private many-cardinality? [dep-obj]
  (= :many (card-type dep-obj)))

(defn ^:private one-cardinality? [dep-obj]
  (= :one (card-type dep-obj)))

(defmulti ^:private get-spec-form*
  (fn [obj opts]
    (cond
      (many-cardinality? obj)
      :many-ref
      
      (:type/enum obj)
      :enum

      (:type/union obj)
      :union
      
      (and (:field/name obj)
           (-> obj :field/parent :type/enum))
      :enum-entry

      (:field/union-type obj)
      :union-field
      
      (:type/name obj)
      :entity

      (and (seqable? obj)
           (every? #(= :param (:node/type %)) obj))
      :param-group

      (:field/name obj) ;; simple field, dispatch type name
      (-> obj :field/type :type/name)

      (:param/name obj) ;; simple param, dispatch type name
      (-> obj :param/type :type/name))))

(defn ^:private get-spec-form [obj opts]
  (if (map? obj)
    (let [{:keys [spec/override spec/extend spec/gen]} obj
          override' (prepend-core-ns override)
          extend' (prepend-core-ns extend)
          gen' (prepend-core-ns gen)
          target-form (if override'
                        override'
                        (if extend'
                          (list* `s/and [extend' (get-spec-form* obj opts)])
                          (get-spec-form* obj opts)))]
      (if gen'
        (list* `s/with-gen [target-form gen'])
        target-form))
    (get-spec-form* obj opts)))

(defn ^:private get-counts [obj]
  (let [many? (many-cardinality? obj)
        card (get-cardinality obj)
        from (first card)
        to (second card)]
    (when many?
      (cond-> {}
        (= from to)
        (assoc :count from)

        (and (not= from to)
             (not= 'n from))
        (assoc :min-count from)

        (and (not= from to)
             (not= 'n to))
        (assoc :max-count to)))))

(defn ^:private get-many-meta-specs [{:keys [spec/distinct spec/kind] :as obj}]
  (let [kind' (prepend-core-ns kind)]
    (cond-> {}
      distinct (assoc :distinct distinct)
      kind (assoc :kind kind'))))

(defmethod get-spec-form* :many-ref
  [obj opts]
  (let [entity-spec (get-spec-form (dissoc obj :field/cardinality :param/cardinality) opts)
        other-nodes (merge (get-counts obj)
                           (get-many-meta-specs obj))]
    (list* `s/coll-of
           (reduce-kv (fn [c k v]
                        (conj c k v))
                      [entity-spec] other-nodes))))

(defmethod get-spec-form* :enum
  [{:keys [field/_parent]} opts]
  (list* `s/or
         (reduce (fn [c {:keys [field/kebab-case-name] :as field}]
                   (conj c kebab-case-name (get-spec-name field opts)))
                 [] _parent)))

(defmethod get-spec-form* :union
  [{:keys [field/_parent]} opts]
  (list* `s/or
         (reduce (fn [c {:keys [field/kebab-case-name] :as field}]
                   (conj c kebab-case-name (get-spec-name field opts)))
                 [] _parent)))

(defmethod get-spec-form* :enum-entry
  [{:keys [field/name]} _]
  `#(= ~name %))

(defmethod get-spec-form* :union-field
  [{:keys [field/union-type]} opts]
  (get-spec-name union-type opts))

(defmethod get-spec-form* :entity
  [{:keys [field/_parent type/implements]} opts]
  (let [filter-fn (fn [pred c]
                    (->> c
                         (filter pred)
                         (map #(get-spec-name % opts))
                         vec))
        req (filter-fn #(not (:field/optional %)) _parent)
        opt (filter-fn #(:field/optional %) _parent)
        form `(s/keys :req-un ~req :opt-un ~opt)]
    (if implements
      (list* `s/and
             (reduce (fn [c interface]
                       (conj c (get-spec-name interface opts)))
                     [form] implements))
      form)))

(defmethod get-spec-form* :param-group
  [params opts]
  (let [filter-fn (fn [pred c]
                    (->> c
                         (filter pred)
                         (map #(get-spec-name % opts))
                         vec))
        req (filter-fn #(not (:param/optional %)) params)
        opt (filter-fn #(:param/optional %) params)]
    `(s/keys :req-un ~req :opt-un ~opt)))

(defmethod get-spec-form* "String" [_ _] `string?)

(defmethod get-spec-form* "ID" [_ _] `string?)

(defmethod get-spec-form* "Integer" [_ _] `integer?)

(defmethod get-spec-form* "Boolean" [_ _] `boolean?)

(defmethod get-spec-form* "Float" [_ _] `float?)

(defmethod get-spec-form* "DateTime" [_ _] `inst?)

(defmethod get-spec-form* :default [obj opts]
  (let [ref-type (or (-> obj :field/type)
                     (-> obj :param/type))]
    (if ref-type
      (get-spec-name ref-type opts)
      (throw (ex-info "Unable to create a spec form for object"
                      {:obj obj})))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti ^:private pull-node
  (fn [conn node]
    (:node/type node)))

(def ^:private param-selector
  '[*
    {:param/parent [*
                    {:field/parent [*]}]}
    {:param/type [*]}])

(def ^:private field-selector
  `[~'*
    {:param/_parent ~param-selector}
    {:field/parent ~'[*]}
    {:field/type ~'[*]}
    {:field/union-type ~'[*]}])

(def ^:private type-selector
  `[~'* {:type/implements ~'[*]
         :field/_parent ~field-selector}])

(defmethod pull-node :type
  [conn {:keys [db/id]}]
  (d/pull @conn type-selector id))

(defmethod pull-node :field
  [conn {:keys [db/id]}]
  (d/pull @conn field-selector id))

(defmethod pull-node :param
  [conn {:keys [db/id]}]
  (d/pull @conn param-selector id))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ^:private build-aliases-spec [conn opts]
  (let [eids (-> (q/q '[:find ?e
                        :where
                        (or [?e :spec/alias]
                            [?e :spec/aliases])]
                      @conn)
                 vec flatten)
        objs (d/pull-many @conn '[*] eids)]
    (reduce (fn [c {:keys [spec/alias spec/aliases] :as obj}]
              (let [aliases' (or aliases alias)
                    aliases'' (if (seqable? aliases') aliases' [aliases'])
                    node (pull-node conn obj)]
                (into c (map #(hash-map % (get-spec-name node opts))
                             aliases''))))
            [] objs)))

(defn ^:private build-param-group-specs [conn opts]
  (let [eids (-> (q/q '[:find ?f
                        :where
                        [_ :param/parent ?f]]
                      @conn)
                 vec flatten)
        objs (d/pull-many @conn '[{:param/_parent [*]}] eids)]
    (reduce (fn [c {:keys [param/_parent] :as field}]
              (let [nodes (map #(pull-node conn %) _parent)]
                (conj c (hash-map (get-spec-name nodes opts)
                                  (get-spec-form nodes opts)))))
            [] objs)))

(defn ^:private build-node-spec [conn node opts]
  (-> conn
      (pull-node node)
      (#(hash-map (get-spec-name % opts)
                  (get-spec-form % opts)))))

(defn ^:private compile-all
  [conn ids opts]
  (let [aliases-specs      (build-aliases-spec conn opts)
        param-groups-specs (build-param-group-specs conn opts)]
    (->> ids
         (d/pull-many @conn [:db/id :node/type])
         (reduce
          (fn [c node]
            (conj c (build-node-spec conn node opts)))
          [])
         (#(into % aliases-specs))
         (#(into % param-groups-specs))
         (mapv (fn [entry]
                 (let [k (first (keys entry))
                       v (first (vals entry))]
                   #_(do (println " - " k)
                         (println " =>" v)
                         (println " ")) 
                   `(s/def ~k ~v)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn schema
  ([conn]
   (schema conn nil))
  ([conn opts]
   (let [all-ids (get-topo-ids conn)]
     (compile-all conn all-ids opts))))

(defmacro defspecs
  ([conn]
   `(defspecs ~conn nil))
  ([conn opts]
   (mapv (fn [form] form)
         (schema (eval conn) (eval opts)))))






(comment
  (require '[clojure.spec.gen.alpha :as gen])
  (require '[hodur-engine.core :as engine])
  (require 'test-fns)


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

  (def cardinality-schema
    '[^{:spec/tag true}
      default

      PersonTwo
      [^String name
       ^Gender gender]
      
      ^:enum
      GenderTwo
      [MALE FEMALE UNKOWN]

      CardinalityEntity
      [^{:type String
         :cardinality [0 n]}
       many-strings
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
        keyword-param]]]))

(comment
  (def meta-db (engine/init-schema basic-schema
                                   cardinality-schema
                                   aliases-schema
                                   extend-override-schema))

  (let [s (schema meta-db {:prefix :my-app})]
    (clojure.pprint/pprint s)))


(comment

  (defspecs meta-db {:prefix :my-app})


  (count (schema meta-db {:prefix :my-app}))
  (count (filter #(or (clojure.string/starts-with? (namespace %) "my-app")
                      (clojure.string/starts-with? (namespace %) "beings")
                      (clojure.string/starts-with? (namespace %) "my-entity")
                      (clojure.string/starts-with? (namespace %) "my-field")
                      (clojure.string/starts-with? (namespace %) "my-param"))
                 (keys (s/registry))))
  
  (s/valid? :my-app.person.height/unit "METERS")

  (s/valid? :my-app/pet {:name "bla" :dob #inst "2000-10-10" :race "cat"})

  (s/valid? :my-app/person {:first-name "Tiago"
                            :last-name "Luchini"
                            :gender "MALE"
                            :height 1.78})

  (s/explain :my-app/person {:first-name "Tiago"
                             :last-name "Luchini"
                             :gender "MALE"
                             :height 1.78})

  (s/valid? :my-app.query-root/search
            [{:name "Lexie"
              :dob #inst "2016-10-10"
              :race "Dog"}
             {:first-name "Tiago"
              :last-name "Luchini"
              :gender "MALE"
              :height 1.78}])

  (s/valid? :my-app.cardinality-entity/many-strings [])
  (s/valid? :my-app.cardinality-entity/many-strings ["foo" "bar"])
  (s/valid? :my-app.cardinality-entity/many-genders ["MALE" "UNKOWN"])
  (s/valid? :my-app.cardinality-entity/exactly-four-strings ["foo" "bar" "foo2" "bar2"])
  (s/valid? :my-app.cardinality-entity/exactly-three-to-five-people
            [{:name "Name" :gender "MALE"}
             {:name "Name" :gender "MALE"}
             {:name "Name" :gender "MALE"}])
  (s/valid? :my-app.cardinality-entity/distinct-integers [1 2 3])
  (s/valid? :my-app.cardinality-entity/distinct-integers-in-a-list '(1 2 3))

  (s/valid? :my-param/alias "qwe")
  (s/valid? :my-field/alias1 "qwe")
  (s/valid? :my-field/alias2 "qwe")
  (s/valid? :my-entity/alias {:an-aliased-field "qwe"})

  (s/valid? :my-app.extend-override-entity/keyword-field :qwe)
  (s/valid? :my-app.extend-override-entity/email-field "qwe@asd.com")

  (gen/generate (s/gen :my-app/animal))

  (gen/generate (s/gen :my-app.extend-override-entity/keyword-field))
  (gen/generate (s/gen :my-app.extend-override-entity/email-field))

  (gen/generate (s/gen :my-app/extend-override-entity))
  
  )
