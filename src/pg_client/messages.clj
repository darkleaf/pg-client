(ns pg-client.messages
  (:require
   [org.clojars.smee.binary.core :as b])
  (:import
   [java.io ByteArrayInputStream ByteArrayOutputStream]
   [java.nio ByteBuffer]))

(defn- ->byte-array [codec value]
  (with-open [out (ByteArrayOutputStream.)]
    (b/encode codec out value)
    (.toByteArray out)))

(def tag
  (b/compile-codec
   (b/string "ASCII" :length 1)
   str
   first))

(def startup-message
  (b/compile-codec
   [(b/ordered-map
     :version-major :short-be
     :version-minor :short-be
     :parameters    (b/repeated [(b/c-string "UTF8")
                                 (b/c-string "UTF8")]))
    (b/constant :byte 0)]
   (fn [val]
     [(update val :parameters #(map (fn [[k v]] [(name k) v]) %))
      0])
   (constantly :not-used)))

(defn encode-startup-message [parameters]
  (let [value     {:version-major 3
                   :version-minor 0
                   :parameters    parameters}
        bytes     (->byte-array startup-message value)
        length    (+ 4 (count bytes))
        res-codec (b/ordered-map
                   :length :int-be
                   :body   (b/blob))
        res-bytes (->byte-array res-codec {:length length
                                           :body   bytes})]
    (ByteBuffer/wrap res-bytes)))

(def header
  (b/ordered-map
   :tag    tag
   :length :int-be))

(defn decode-header [buff]
  (.rewind buff)
  (let [length (.remaining buff)
        arr    (byte-array length)]
    (.get buff arr)
    (with-open [in (ByteArrayInputStream. arr)]
      (b/decode header in))))
