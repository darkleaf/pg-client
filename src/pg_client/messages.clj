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

(def ^:private tag
  (b/compile-codec
   (b/string "ASCII" :length 1)
   str
   first))

(def ^:private c-string
  (b/c-string "UTF8"))

(def ^:private header-with-tag
  (b/ordered-map :tag    tag
                 :length :int-be
                 :body   (b/blob)))

(def ^:private header-without-tag
  (b/ordered-map :length :int-be
                 :body   (b/blob)))

(defn encode [spec value]
  (let [tag       (:tag spec)
        codec     (:codec spec)
        arr       (encode-as-byte-array codec value)
        length    (+ 4 (count arr)) ;; tag не считается
        res-codec (if (some? tag) header-with-tag header-without-tag)
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
           [(b/ordered-map :version-major :short-be
                           :version-minor :short-be
                           :parameters    (b/repeated [c-string c-string]))
            (b/constant :byte 0)]
           (fn [val]
             [(update val :parameters #(map (fn [[k v]] [(name k) v]) %))
              0])
           not-used)})

(def ^:private auth-code->codec
  {5 (b/compile-codec
      (b/ordered-map :salt (b/blob :length 4))
      not-used
      #(assoc % :name :AuthenticationMD5Password))})

(def Authentication
  {:tag \R
   :codec (b/header :int-be
                    auth-code->codec
                    not-used)})

(def tag->spec
  {\R Authentication})
