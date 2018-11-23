(ns hodur-spec-schema.core
  (:require [clojure.spec.alpha :as s]
            [datascript.core :as d]
            [datascript.query-v3 :as q]
            [camel-snake-kebab.core :refer [->kebab-case-string]]))

(def ^:private selector
  '[* {:type/implements [*]
       :field/_parent
       [* {:param/_parent [* {:param/parent [* {:field/parent [*]}]}
                           {:param/type [*]}]}
        {:field/parent [*]}
        {:field/type
         [* {:field/_parent [*]}]}]}])

(defn ^:private get-types [conn]
  (let [eids (-> (q/q '[:find ?t
                        :where
                        [?t :type/name]
                        [?t :spec/tag true]
                        [?t :type/nature :user]]
                      @conn)
                 vec flatten)]
    (->> eids
         (d/pull-many @conn selector)
         (sort-by :type/name))))

(defn ^:private get-type-by-name [conn type-name]
  (let [eids (-> (q/q '[:find ?t
                        :in $ ?type-name
                        :where
                        [?t :type/name ?type-name]
                        [?t :spec/tag true]
                        [?t :type/nature :user]]
                      @conn type-name)
                 vec flatten)]
    (->> eids
         (d/pull-many @conn selector)
         first)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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
      (and (-> obj :field/name)
           (-> obj :field/parent :type/union))
      :union-field

      (and (vector? obj)
           (every? #(:param/name %) obj))
      :param-group

      (:type/name obj)
      :entity
      
      (:field/name obj)
      :field

      (:param/name obj)
      :param)))

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

(defmethod get-spec-name :entity
  [{:keys [type/kebab-case-name]} opts]
  (get-spec-entity-name (name kebab-case-name) opts))

(defmethod get-spec-name :field
  [{:keys [field/kebab-case-name
           field/parent]} opts]
  (get-spec-field-name (name (:type/kebab-case-name parent))
                       (name kebab-case-name)
                       opts))

(defmethod get-spec-name :union-field
  [{:keys [field/kebab-case-name]} opts]
  (get-spec-entity-name (name kebab-case-name) opts))

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

      (:type/name obj)
      :entity

      (and (vector? obj)
           (every? #(:param/name %) obj))
      :param-group

      (:field/name obj) ;; simple field, dispatch type name
      (-> obj :field/type :type/name)

      (:param/name obj) ;; simple param, dispatch type name
      (-> obj :param/type :type/name))))

(defn ^:private get-spec-form [{:keys [spec/override spec/extend spec/gen] :as obj} opts]
  (let [override' (prepend-core-ns override)
        extend' (prepend-core-ns extend)
        gen' (prepend-core-ns gen)
        base-form (get-spec-form* obj opts)
        target-form (if override'
                      override'
                      (if extend'
                        (list* `s/and [extend' base-form])
                        base-form))]
    (if gen'
      (list* `s/with-gen [target-form gen'])
      target-form)))

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
    (when ref-type (get-spec-name ref-type opts))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ^:private build-alias-spec [{:keys [spec/alias] :as obj} main-spec]
  (when alias
    (let [aliases (if (seqable? alias) alias [alias])]
      (map #(hash-map % (first (keys main-spec)))
           aliases))))

(defn ^:private conj-param [coll param {:keys [conn] :as opts}]
  (let [param-spec (hash-map (get-spec-name param opts)
                             (get-spec-form param opts))
        alias-spec (build-alias-spec param param-spec)]
    (cond-> coll
      alias-spec (into alias-spec)
      :always    (conj param-spec))))

(defn ^:private conj-field [coll {:keys [param/_parent] :as field} opts]
  (let [conjd-params (reduce (fn [c param]
                               (conj-param c param opts))
                             coll _parent)
        params-spec (hash-map (get-spec-name _parent opts)
                              (get-spec-form _parent opts))
        params-spec? (not (= {nil nil} params-spec))
        field-spec (hash-map (get-spec-name field opts)
                             (get-spec-form field opts))
        alias-spec (build-alias-spec field field-spec)]
    (cond-> conjd-params
      params-spec? (conj params-spec)
      alias-spec   (into alias-spec)
      :always      (conj field-spec))))

(defn ^:private conj-type [coll {:keys [field/_parent] :as t} opts]
  (let [type-spec (hash-map (get-spec-name t opts)
                            (get-spec-form t opts))
        alias-spec (build-alias-spec t type-spec)
        conj-fields-fn (fn [inner-coll]
                         (reduce (fn [c field]
                                   (conj-field c field opts))
                                 inner-coll _parent))]
    (cond-> coll
      (not (:type/union t))
      conj-fields-fn

      alias-spec
      (into alias-spec)

      :always
      (conj type-spec))))

(defn ^:private compile-types
  [types opts]
  (->> types
       (reduce
        (fn [c t]
          (conj-type c t opts))
        [])
       (mapv (fn [entry]
               (let [k (first (keys entry))
                     v (first (vals entry))]
                 (println " -" k)
                 `(s/def ~k ~v))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn schema
  ([conn]
   (schema conn nil))
  ([conn opts]
   (let [types (get-types conn)]
     (compile-types types (assoc opts :conn conn)))))




(require '[hodur-engine.core :as engine])

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

    Person
    [^String name
     ^Gender gender]
    
    ^:enum
    Gender
    [MALE FEMALE]

    CardinalityEntity
    [^{:type String
       :cardinality [0 n]}
     many-strings
     ^{:type Gender
       :cardinality [0 n]}
     many-genders
     ^{:type Person
       :cardinality [0 n]}
     many-people
     ^{:type Person
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

    ^{:spec/extend map?}
    ExtendOverrideEntity
    [^{:type String
       :spec/extend hodur-spec-schema.test-fns/email?}
     email-field
     ^{:type String
       :spec/override keyword?
       :spec/gen hodur-spec-schema.test-fns/keyword-gen}
     keyword-field
     [^{:type String
        :spec/override keyword?
        :spec/gen hodur-spec-schema.test-fns/keyword-gen}
      keyword-param]]])

(def meta-db (engine/init-schema extend-override-schema))

(let [s (schema meta-db {:prefix :my-app})]
  (clojure.pprint/pprint s))


(comment
  (s/def :app.person/first-name string?)
  (s/def :app.person/middle-name string?)
  (s/def :app.person/last-name string?)
  (s/def :app/person (s/keys :req-un [:app.person/first-name
                                      :app.person/last-name]
                             :opt-un [:app.person/middle-name]))

  (s/valid? :app/person
            {:first-nam "Tiago"
             :last-name "Luchini"})

  (s/explain :app/person
             {:first-name "Tiago"
              :last-name "Luchini"
              :middle-name "2"})

  )
