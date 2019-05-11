(ns pg-client.future
  (:import
   [java.util.concurrent CompletableFuture]
   [java.util.function Function]
   [java.nio.channels CompletionHandler]))

(def handler
  (reify CompletionHandler
    (completed [_ result future]
      (.complete future result))
    (failed [_ ex future]
      (.completeExceptionally future ex))))

(defn- ->Function [f]
  (reify Function
    (apply [_ value]
      (f value))))

(defn new []
  (CompletableFuture.))

(defn completed [val]
  (CompletableFuture/completedFuture val))

(defn then-apply [future f & args]
  (.thenApply future (->Function (fn [val] (apply f val args)))))

(defn then-compose [future f & args]
  (.thenCompose future (->Function (fn [val] (apply f val args)))))

(defn then-next [future future-builder]
  (then-compose future (fn [_] (future-builder))))

(defn get [future]
  (.get future))
