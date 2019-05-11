(ns pg-client.core
  (:require
   [pg-client.messages :as m]
   [pg-client.future :as future]
   [pg-client.socket :as sock]))

(comment
  (let [sock (sock/open)]
    (-> (nio/sock-connect sock "localhost" 4401)
        (future/then-next #(sock-write sock (m/encode m/StartupMessage
                                                      {:version-major 3
                                                       :version-minor 0
                                                       :parameters    {:user "postgres"}})))
        (future/then-next #(sock-read sock m/header-length))
        (future/then-apply m/decode-header)
        (future/then-compose (fn [{:keys [tag length]}]
                               (let [spec (m/tag->spec tag)]
                                 (-> (sock-read sock length)
                                     (future/then-apply #(m/decode-body spec %))))))
        (future/get))))
