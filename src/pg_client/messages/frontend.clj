(ns pg-client.messages.frontend
  (:require
   [org.clojars.smee.binary.core :as b]
   [pg-client.messages.codec :as codec])
  (:import
   [java.io ByteArrayOutputStream]
   [java.nio ByteBuffer]))

(defn- encode-as-byte-array [codec value]
  (with-open [out (ByteArrayOutputStream.)]
    (b/encode codec out value)
    (.toByteArray out)))

(def ^:private message-with-tag
  (b/ordered-map :tag    codec/char
                 :length :int-be
                 :body   (b/blob)
                 :end    (b/constant :byte 0)))

(def ^:private message-without-tag
  (b/ordered-map :length :int-be
                 :body   (b/blob)
                 :end    (b/constant :byte 0)))

(defn encode [spec value]
  (let [tag       (:tag spec)
        codec     (:codec spec)
        arr       (encode-as-byte-array codec value)
        length    (+ 5 (count arr)) ;; tag не считается
        res-codec (if (some? tag) message-with-tag message-without-tag)
        res-bytes (encode-as-byte-array res-codec {:tag    tag
                                                   :length length
                                                   :body   arr})]
    (ByteBuffer/wrap res-bytes)))

(def StartupMessage
  {:codec (b/ordered-map :version-major :short-be
                         :version-minor :short-be
                         :parameters    codec/key-value)})

(def PasswordMessage
  {:tag \p
   :codec (b/ordered-map :password codec/string)})

(def Query
  {:tag \Q
   :codec (b/ordered-map :query codec/string)})
