(ns pg-client.messages.codec
  (:refer-clojure :exclude [char])
  (:require
   [clojure.core :as c]
   [org.clojars.smee.binary.core :as b]))

(def char
  (b/compile-codec :byte byte c/char))

(def c-string
  (b/c-string "UTF8"))

(def string
  (b/string "UTF8"))

(def key-value
  (b/compile-codec
   (b/repeated [c-string c-string])
   #(map (fn [[k v]] [(name k) v])
         %)
   #(reduce (fn [acc [k v]] (assoc acc (keyword k) v))
            {} %)))
