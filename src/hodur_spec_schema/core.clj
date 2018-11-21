(ns hodur-spec-schema.core
  (:require [clojure.spec.alpha :as s]
            [datascript.core :as d]
            [datascript.query-v3 :as q]))

(defn ^:private get-types [conn]
  (let [selector '[* {:type/implements [*]
                      :field/_parent
                      [* {:field/parent [*]}
                       {:field/type
                        [* {:field/_parent [*]}]}]}]
        eids (-> (q/q '[:find ?t
                        :where
                        [?t :type/name]
                        [?t :spec/tag true]
                        [?t :type/nature :user]]
                      @conn)
                 vec flatten)]
    (->> eids
         (d/pull-many @conn selector)
         (sort-by :type/name))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti ^:private get-spec-name
  (fn [obj]
    (cond
      (:type/name obj) :entity
      (:field/name obj) :field)))

(defmethod get-spec-name :entity
  [{:keys [type/kebab-case-name]}]
  (keyword (str (ns-name *ns*))
           (name kebab-case-name)))

(defmethod get-spec-name :field
  [{:keys [field/kebab-case-name
           field/parent]}]
  (keyword (str (ns-name *ns*) "." (name (:type/kebab-case-name parent)))
           (name kebab-case-name)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmulti ^:private get-spec-form
  (fn [obj]
    (cond
      (:type/enum obj)
      :enum

      (and (:field/name obj)
           (-> obj :field/parent :type/enum))
      :enum-entry

      (:type/name obj)
      :entity

      (:field/name obj)
      (-> obj :field/type :type/name))))

(defmethod get-spec-form :enum
  [{:keys [field/_parent]}]
  (list* `(s/or)
         (reduce (fn [c {:keys [field/kebab-case-name] :as field}]
                   (conj c kebab-case-name (get-spec-name field)))
                 [] _parent)))

(defmethod get-spec-form :enum-entry
  [{:keys [field/name]}]
  `#(= ~name %))

(defmethod get-spec-form :entity
  [{:keys [field/_parent]}]
  (let [filter-fn (fn [pred c]
                    (->> c
                         (filter pred)
                         (map #(get-spec-name %))
                         vec))
        req (filter-fn #(not (:field/optional %)) _parent)
        opt (filter-fn #(:field/optional %) _parent)]
    `(s/keys :req-un ~req :opt-un ~opt)))

(defmethod get-spec-form "String" [_] `string?)

(defmethod get-spec-form "ID" [_] `string?)

(defmethod get-spec-form "Integer" [_] `integer?)

(defmethod get-spec-form "Boolean" [_] `boolean?)

(defmethod get-spec-form "Float" [_] `float?)

(defmethod get-spec-form "DateTime" [_] `inst?)

(defmethod get-spec-form :default [obj]
  (let [ref-type (-> obj :field/type)]
    (get-spec-name ref-type)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

     (defn ^:private conj-field [coll field]
       (conj coll (hash-map (get-spec-name field)
                            (get-spec-form field))))

     (defn ^:private conj-type [coll {:keys [field/_parent] :as t}]
       (let [conjd-fields (reduce (fn [c field]
                                    (conj-field c field))
                                  coll _parent)]
         (conj conjd-fields (hash-map (get-spec-name t)
                                      (get-spec-form t)))))

(defn ^:private compile-types
  [types]
  (->> types
       (reduce
        (fn [c t]
          (conj-type c t))
        [])
       (mapv (fn [entry]
               (let [k (first (keys entry))
                     v (first (vals entry))]
                 `(s/def ~k ~v))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn schema
  [conn]
  (let [types (get-types conn)]
    (compile-types types)))


     (require '[hodur-engine.core :as engine])

(let [meta-db (engine/init-schema
               '[^{:spec/tag true}
                 default

                 Person
                 [^String first-name
                  ^{:type String
                    :optional true}
                  middle-name
                  ^String last-name
                  ^Gender gender]

                 ^:enum
                 Gender
                 [MALE FEMALE]])
      s (schema meta-db)]
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
              :middle-name 'q})

  )
