(ns pg-client.messages.backend
  (:require
   [clojure.set :as set]
   [org.clojars.smee.binary.core :as b]
   [pg-client.messages.codec :as codec])
  (:import
   [java.io ByteArrayInputStream]))

(defn- buff->byte-array [buff]
  (.rewind buff)
  (let [length (.remaining buff)
        arr    (byte-array length)]
    (.get buff arr)
    arr))

(defn- decode-from-buffer [codec buff]
  (let [arr (buff->byte-array buff)]
    (with-open [in (ByteArrayInputStream. arr)]
      (b/decode codec in))))

(def ^:private header
  (b/ordered-map :tag    codec/char
                 :length :int-be))

(def header-length 5)

(defn decode-header [buff]
  (decode-from-buffer header buff))

(defn decode-body [spec buff]
  (decode-from-buffer (:codec spec) buff))

(def ^:private auth-codes
  {0 :AuthenticationOk
   5 :AuthenticationMD5Password})

(def ^:private auth-header
  (b/compile-codec :int-be
                   (set/map-invert auth-codes)
                   auth-codes))

(def ^:private auth-code->codec
  {:AuthenticationOk          (b/ordered-map)
   :AuthenticationMD5Password (b/ordered-map :salt (b/blob :length 4))})

(def Authentication
  {:tag \R
   :codec (b/header auth-header
                    auth-code->codec
                    "not-used"
                    :keep-header? true)})

(def ParameterStatus
  {:tag \S
   :codec (b/ordered-map :key   codec/c-string
                         :value codec/c-string)})

(def BackendKeyData
  {:tag \K
   :codec (b/ordered-map :pid        :int-be
                         :secret-key :int-be)})

(def ^:private transaction-statuses
  {\I :idle
   \T :in-transaction
   \E :in-failed-transaction})

(def ^:private transaction-status
  (b/compile-codec :byte
                   (comp byte (set/map-invert transaction-statuses))
                   (comp transaction-statuses char)))

(def ReadyForQuery
  {:tag \Z
   :codec (b/ordered-map :status transaction-status)})

(def ErrorResponse
  {:tag \E
   :codec (b/repeated [codec/char codec/c-string])})

(def tag->spec
  (let [specs [Authentication
               ParameterStatus
               BackendKeyData
               ReadyForQuery
               ErrorResponse]]
    (reduce (fn [acc {:as spec, :keys [tag]}]
              (assoc acc tag spec))
            {}
            specs)))
