(ns pg-client.util
  (:require
   [clojure.string :as str])
  (:import
   [java.security MessageDigest]))

(defn md5 [byte-arr]
  (let [md5   (MessageDigest/getInstance "MD5")]
    (.update md5 byte-arr)
    (.digest md5)))

(defn concat-byte-arrays [a b]
  (let [buf (byte-array (+ (count a) (count b)))]
    (System/arraycopy a 0 buf 0         (count a))
    (System/arraycopy b 0 buf (count a) (count b))
    buf))

(defn byte-array->hex [arr]
  (->> arr
       (map #(format "%02x" %))
       (str/join)))

(defn postgres-md5-password-hash [user password salt]
  (let [inner (-> (str password user)
                  (.getBytes)
                  (md5)
                  (byte-array->hex)
                  (.getBytes))
        outer (md5 (concat-byte-arrays inner salt))
        hex   (byte-array->hex outer)]
    (str "md5" hex)))
