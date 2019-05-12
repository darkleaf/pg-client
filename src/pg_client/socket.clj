(ns pg-client.socket
  (:refer-clojure :exclude [read])
  (:require
   [pg-client.future :as future])
  (:import
   [java.net InetSocketAddress]
   [java.nio.channels AsynchronousSocketChannel]
   [java.nio ByteBuffer]))

;; канал можно создать в группе, а в группу можно передать тредпул, может быть так можно сделать
;; connection pooling?

(defn open []
  (AsynchronousSocketChannel/open))

(defn connect [sock host port]
  (let [address (InetSocketAddress. host port)
        future  (future/new)]
    (.connect sock address future future/handler)
    future))

(defn read [sock length]
  (let [buff   (ByteBuffer/allocateDirect length)
        future (future/new)]
    (.read sock buff future future/handler)
    (-> future
        ;; наверное, нужно еще проверять сколько байтов на самом деле вычитали
        #_(future/then-apply #(doto %
                                prn))
        (future/then-apply (constantly buff)))))

(defn write [sock buff]
  (let [future (future/new)]
    (.write sock buff future future/handler)
    future))
