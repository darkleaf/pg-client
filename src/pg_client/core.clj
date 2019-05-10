(ns pg-client.core
  (:require
   [pg-client.messages :as m]
   [clojure.core.async :as a])
  (:import
   [java.net InetSocketAddress]
   [java.nio.channels AsynchronousSocketChannel CompletionHandler]
   [java.nio ByteBuffer]))

;; канал можно создать в группе, а в группу можно передать тредпул, может быть так можно сделать
;; connection pooling?


(def channel-handler
  (reify CompletionHandler
    (completed [_ result chan]
      (if (some? result)
        (a/put! chan result))
      (a/close! chan))
    (failed [_ ex chan]
      (a/put! chan ex)
      (a/close! chan))))

(defn sock-connect [sock address]
  (let [chan (a/promise-chan)]
    (.connect sock address chan channel-handler)
    chan))

(defn sock-read [sock]
  (a/go
    (let [buff   (ByteBuffer/allocateDirect 5)
          chan   (a/promise-chan)
          _      (.read sock buff chan channel-handler)
          readed (a/<! chan)]
      (prn (m/decode-header buff)))))

(defn sock-write [sock buff]
  (let [chan (a/promise-chan)]
    (.write sock buff chan channel-handler)
    chan))

(defn connect []
  (a/go
    (let [address (InetSocketAddress. "localhost" 4401)
          sock    (AsynchronousSocketChannel/open)]
      (a/<! (sock-connect sock address))
      (a/<! (sock-write sock (m/encode m/StartupMessage
                                       {:version-major 3
                                        :version-minor 0
                                        :parameters    {:user "postgres"}})))
      (a/<! (sock-read sock))
      {:sock sock})))

(comment
  (a/<!! (connect)))
