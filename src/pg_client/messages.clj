(ns pg-client.messages
  (:require
   [org.clojars.smee.binary.core :as b])
  (:import
   [java.io ByteArrayInputStream ByteArrayOutputStream]
   [java.nio ByteBuffer]))

(defn- not-used [& args]
  (throw (ex-info "Not used" {:args args})))

(defn- encode-as-byte-array [codec value]
  (with-open [out (ByteArrayOutputStream.)]
    (b/encode codec out value)
    (.toByteArray out)))

(defn buff->byte-array [buff]
  (.rewind buff)
  (let [length (.remaining buff)
        arr    (byte-array length)]
    (.get buff arr)
    arr))

(defn- decode-from-buffer [codec buff]
  (let [arr (buff->byte-array buff)]
    (with-open [in (ByteArrayInputStream. arr)]
      (b/decode codec in))))

(def ^:private tag
  (b/compile-codec :byte byte char))

(def ^:private c-string
  (b/c-string "UTF8"))

(def ^:private message-with-tag
  (b/ordered-map :tag    tag
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

(def ^:private header
  (b/ordered-map :tag    tag
                 :length :int-be))

(def header-length 5)

(defn decode-header [buff]
  (decode-from-buffer header buff))

(defn decode-body [spec buff]
  (decode-from-buffer (:codec spec) buff))

;; ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

(def StartupMessage
  {:tag nil
   :codec (b/compile-codec
           (b/ordered-map :version-major :short-be
                          :version-minor :short-be
                          :parameters    (b/repeated [c-string c-string]))
           (fn [val]
             (update val :parameters #(map (fn [[k v]] [(name k) v]) %)))
           not-used)})

(def PasswordMessage
  {:tag \p
   :codec (b/ordered-map :password (b/string "UTF8"))})

(def ^:private auth-code->codec
  {0 (b/compile-codec
      (b/ordered-map)
      not-used
      #(assoc % :name :AuthenticationOk))
   5 (b/compile-codec
      (b/ordered-map :salt (b/blob :length 4))
      not-used
      #(assoc % :name :AuthenticationMD5Password))})

(def Authentication
  {:tag \R
   :codec (b/header :int-be
                    auth-code->codec
                    not-used)})

(def ParameterStatus
  {:tag 'S
   :codec (b/compile-codec
           (b/ordered-map :key   c-string
                          :value c-string)
           not-used
           #(assoc % :name :ParameterStatus))})

(def BackendKeyData
  {:tag \K
   :codec (b/compile-codec
           (b/ordered-map :pid        :int-be
                          :secret-key :int-be)
           not-used
           #(assoc % :name :BackendKeyData))})

(def ReadyForQuery
  {:tag \Z
   :codec (b/compile-codec
           (b/ordered-map :status (b/compile-codec
                                   :byte
                                   not-used
                                   (comp {\I :idle, \T :transaction, \E :error} char)))
           not-used
           #(assoc % :name :ReadyForQuery))})

(def Query
  {:tag \Q
   :codec (b/ordered-map :query c-string)})

(def tag->spec
  {\R Authentication
   \S ParameterStatus
   \K BackendKeyData
   \Z ReadyForQuery})
