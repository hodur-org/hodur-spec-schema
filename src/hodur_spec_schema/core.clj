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

(declare get-spec-form)

(defmulti ^:private get-spec-name
  (fn [obj opts]
    (cond
      (and (-> obj :field/name)
           (-> obj :field/parent :type/union))
      :union-field

      (= :primitive (:type/nature obj))
      :primitive
      
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

(defmethod get-spec-name :primitive
  [t opts]
  (get-spec-form t opts))

#_(defmethod get-spec-name :default
    [obj]
    (println "FIXME!!!!")
    (clojure.pprint/pprint obj)
    nil)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti ^:private get-spec-form
  (fn [obj opts]
    (cond
      (:type/enum obj)
      :enum

      (:type/union obj)
      :union
      
      (and (:field/name obj)
           (-> obj :field/parent :type/enum))
      :enum-entry

      (:type/name obj)
      :entity

      (:field/name obj)
      (-> obj :field/type :type/name))))

(defmethod get-spec-form :enum
  [{:keys [field/_parent]} opts]
  (list* `s/or
         (reduce (fn [c {:keys [field/kebab-case-name] :as field}]
                   (conj c kebab-case-name (get-spec-name field opts)))
                 [] _parent)))

(defmethod get-spec-form :union
  [{:keys [field/_parent]} opts]
  (list* `s/or
         (reduce (fn [c {:keys [field/kebab-case-name] :as field}]
                   (conj c kebab-case-name (get-spec-name field opts)))
                 [] _parent)))

(defmethod get-spec-form :enum-entry
  [{:keys [field/name]} _]
  `#(= ~name %))

(defmethod get-spec-form :entity
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

(defmethod get-spec-form "String" [_ _] `string?)

(defmethod get-spec-form "ID" [_ _] `string?)

(defmethod get-spec-form "Integer" [_ _] `integer?)

(defmethod get-spec-form "Boolean" [_ _] `boolean?)

(defmethod get-spec-form "Float" [_ _] `float?)

(defmethod get-spec-form "DateTime" [_ _] `inst?)

(defmethod get-spec-form :default [obj opts]
  (let [ref-type (-> obj :field/type)]
    (get-spec-name ref-type opts)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ^:private conj-param [coll param {:keys [conn] :as opts}]
  (let [t (->> param :param/type :type/name (get-type-by-name conn))]
    (conj coll (hash-map (get-spec-name param opts)
                         (get-spec-name t opts)))))

(defn ^:private conj-field [coll {:keys [param/_parent] :as field} opts]
  (let [conjd-params (reduce (fn [c param]
                               (conj-param c param opts))
                             coll _parent)
        field-spec (hash-map (get-spec-name field opts)
                             (get-spec-form field opts))]
    (conj conjd-params field-spec)))

(defn ^:private conj-type [coll {:keys [field/_parent] :as t} opts]
  (let [type-spec (hash-map (get-spec-name t opts)
                            (get-spec-form t opts))]
    (if (:type/union t)
      (conj coll type-spec)
      (let [conjd-fields (reduce (fn [c field]
                                   (conj-field c field opts))
                                 coll _parent)]
        (conj conjd-fields type-spec)))))

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

(def meta-db (engine/init-schema
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

                ^:interface
                Animal
                [^String race]
                
                ^:union
                SearchResult
                [Person Pet]

                ^:enum
                Unit
                [METERS FEET]]))

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
