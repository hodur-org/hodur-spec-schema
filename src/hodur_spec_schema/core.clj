(ns hodur-spec-schema.core
  (:require [clojure.spec.alpha :as s]
            [datascript.core :as d]
            [datascript.query-v3 :as q]
            [camel-snake-kebab.core :refer [->kebab-case-string]]))

(defn ^:private get-ids-by-node-type [conn node-type]
  (case node-type
    :type (d/q '[:find [?e ...]
                 :in $ ?node-type
                 :where
                 [?e :node/type ?node-type]
                 [?e :spec/tag true]
                 [?e :type/nature :user]]
               @conn node-type)
    (d/q '[:find [?e ...]
           :in $ ?node-type
           :where
           [?e :node/type ?node-type]
           [?e :spec/tag true]]
         @conn node-type)))

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

(defn ^:private get-spec-entity-name
  [type-name
   {:keys [prefix]}]
  (keyword (name prefix)
           (->kebab-case-string type-name)))

(defn ^:private get-spec-field-name
  [type-name field-name
   {:keys [prefix]}]
  (keyword (str (name prefix) "." (->kebab-case-string type-name))
           (->kebab-case-string field-name)))

(defn ^:private get-spec-param-name
  [type-name field-name param-name
   {:keys [prefix]}]
  (keyword (str (name prefix) "." (->kebab-case-string type-name) "."
                (->kebab-case-string field-name))
           (->kebab-case-string param-name)))

(defn ^:private get-spec-param-group-name
  [type-name field-name
   {:keys [prefix params-postfix group-type] :or {params-postfix "%"} :as opts}]
  (keyword (str (name prefix) "." (->kebab-case-string type-name))
           (str (->kebab-case-string field-name)
                (case group-type
                  :map ""
                  :tuple "-ordered")
                params-postfix)))

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
      (:field/optional obj)
      :optional-field

      (:param/optional obj)
      :optional-param

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
  `#(= ~name (when (or (string? %)
                       (keyword? %))
               (name %))))

(defmethod get-spec-form* :union-field
  [{:keys [field/union-type]} opts]
  (get-spec-name union-type opts))

(defmethod get-spec-form* :optional-param
  [obj opts]
  (let [entity-spec (get-spec-form (dissoc obj :param/optional) opts)]
    (list* `s/nilable [entity-spec])))

(defmethod get-spec-form* :optional-field
  [obj opts]
  (let [entity-spec (get-spec-form (dissoc obj :field/optional) opts)]
    (list* `s/nilable [entity-spec])))

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
  [params {:keys [group-type] :as opts}]
  (let [filter-fn (fn [pred c]
                    (->> c
                         (filter pred)
                         (map #(get-spec-name % opts))
                         vec))
        req (filter-fn #(not (:param/optional %)) params)
        opt (filter-fn #(:param/optional %) params)]
    (case group-type
      :map `(s/keys :req-un ~req :opt-un ~opt)
      :tuple (list* `s/tuple (map #(get-spec-name % opts) params)))))

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
                        [?e :spec/tag true]
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

(defn ^:private build-param-group-specs [conn group-type opts]
  (let [eids (-> (q/q '[:find ?f
                        :where
                        [_ :param/parent ?f]
                        [?f :spec/tag true]]
                      @conn)
                 vec flatten)
        objs (d/pull-many @conn '[{:param/_parent [:db/id :node/type]}] eids)]
    (reduce (fn [c {:keys [param/_parent] :as field}]
              (let [nodes (map #(pull-node conn %) _parent)
                    opts' (assoc opts :group-type group-type)]
                (conj c (hash-map (get-spec-name nodes opts')
                                  (get-spec-form nodes opts')))))
            [] objs)))

(defn ^:private build-dummy-types-specs [conn opts]
  (let [eids (get-ids-by-node-type conn :type)
        objs (d/pull-many @conn '[*] eids)]
    (reduce (fn [c obj]
              (conj c (hash-map (get-spec-name obj opts)
                                'any?)))
            [] objs)))

(defn ^:private build-node-spec [conn node opts]
  (-> conn
      (pull-node node)
      (#(hash-map (get-spec-name % opts)
                  (get-spec-form % opts)))))

(defn ^:private build-by-type-specs [conn node-type opts]
  (let [eids (get-ids-by-node-type conn node-type)
        nodes (d/pull-many @conn [:db/id :node/type] eids)]
    (map #(build-node-spec conn % opts) nodes)))

(defn ^:private compile-all
  [conn opts]
  (let [dummy-types-specs          (build-dummy-types-specs conn opts)
        params-specs               (build-by-type-specs conn :param opts)
        field-specs                (build-by-type-specs conn :field opts)
        type-specs                 (build-by-type-specs conn :type opts)
        aliases-specs              (build-aliases-spec conn opts)
        param-groups-specs         (build-param-group-specs conn :map opts)
        param-groups-ordered-specs (build-param-group-specs conn :tuple opts)]
    (->> (concat dummy-types-specs
                 params-specs
                 field-specs
                 type-specs
                 aliases-specs
                 param-groups-specs
                 param-groups-ordered-specs)
         (mapv (fn [entry]
                 (let [k (first (keys entry))
                       v (first (vals entry))]
                   #_(do (println " - " k)
                         (println " =>" v)
                         (println " ")) 
                   `(s/def ~k ~v)))))))

(defn ^:private eval-default-prefix []
  (eval '(str (ns-name *ns*))))

(defn ^:private default-prefix []
  (str (ns-name *ns*)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn schema
  ([conn]
   (schema conn nil))
  ([conn {:keys [prefix] :as opts}]
   (let [opts' (if-not prefix (assoc opts :prefix (default-prefix)) opts)]
     (compile-all conn opts'))))

(defmacro defspecs
  ([conn]
   `(defspecs ~conn nil))
  ([conn {:keys [prefix] :as opts}]
   (let [opts# (if-not prefix (assoc opts :prefix (eval-default-prefix)) (eval opts))
         conn# (eval conn)]
     (mapv (fn [form] form)
           (schema conn# opts#)))))






(comment
  (require '[clojure.spec.gen.alpha :as gen])
  (require '[hodur-engine.core :as engine])
  (require 'test-fns)
  (use 'core-test)


  )

(comment
  (def meta-db (engine/init-schema basic-schema
                                   #_cardinality-schema
                                   #_aliases-schema
                                   #_extend-override-schema))

  (let [s (schema meta-db {:prefix :my-app})]
    #_(clojure.pprint/pprint s))

  )


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
