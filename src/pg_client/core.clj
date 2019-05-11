(ns pg-client.core
  (:require
   [pg-client.messages :as m])
  (:import
   [java.util.concurrent CompletableFuture]
   [java.util.function Function]
   [java.net InetSocketAddress]
   [java.nio.channels AsynchronousSocketChannel CompletionHandler]
   [java.nio ByteBuffer]))

;; канал можно создать в группе, а в группу можно передать тредпул, может быть так можно сделать
;; connection pooling?

(defn ->Function [f]
  (reify Function
    (apply [_ value]
      (f value))))

(defn then-apply [future f & args]
  (.thenApply future (->Function (fn [val] (apply f val args)))))

(defn then-compose [future f & args]
  (.thenCompose future (->Function (fn [val] (apply f val args)))))

(defn then-next [future future-builder]
  (then-compose future (fn [_] (future-builder))))

(def future-handler
  (reify CompletionHandler
    (completed [_ result future]
      (.complete future result))
    (failed [_ ex future]
      (.completeExceptionally future ex))))

(defn sock-connect [sock address]
  (let [future (CompletableFuture.)]
    (.connect sock address future future-handler)
    future))

(defn sock-read [sock length]
  (let [buff   (ByteBuffer/allocateDirect length)
        future (CompletableFuture.)]
    (.read sock buff future future-handler)
    ;; наверное, нужно еще проверять сколько байтов на самом деле вычитали
    (.thenApply future (->Function (constantly buff)))))

(defn sock-write [sock buff]
  (let [future (CompletableFuture.)]
    (.write sock buff future future-handler)
    future))


(comment
  (let [address (InetSocketAddress. "localhost" 4401)
        sock    (AsynchronousSocketChannel/open)]
    (-> (sock-connect sock address)
        (then-next #(sock-write sock (m/encode m/StartupMessage
                                               {:version-major 3
                                                :version-minor 0
                                                :parameters    {:user "postgres"}})))
        (then-next #(sock-read sock m/header-length))
        (then-apply m/decode-header)
        (.get))))
