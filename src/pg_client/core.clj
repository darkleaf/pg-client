(ns pg-client.core
  (:require
   [pg-client.messages :as m]
   [pg-client.future :as future]
   [pg-client.socket :as sock]
   [pg-client.util :as util]))

(defmulti handle-response (fn [resp ctx sock] (:name resp)))

(defn round-trip [sock req-spec data ctx]
  (-> (sock/write sock (m/encode req-spec data))
      (future/then-next #(sock/read sock m/header-length))
      (future/then-apply m/decode-header)
      (future/then-compose (fn [{:keys [tag length]}]
                             (let [resp-spec (m/tag->spec tag)]
                               (-> (sock/read sock length)
                                   (future/then-apply #(m/decode-body resp-spec %))))))
      (future/then-compose handle-response ctx sock)))

(defmethod handle-response :AuthenticationMD5Password [{:keys [salt]}
                                                       {:keys [user password] :as ctx}
                                                       sock]
  (let [digest (util/postgres-md5-password-hash user password salt)]
    (round-trip sock m/PasswordMessage {:password digest} ctx)))

(defmethod handle-response :AuthenticationOk [resp ctx sock]
  (future/completed :AuthenticationOk))

(defn connect [{:keys [host port] :as opts}]
  (let [sock (sock/open)]
    (-> (sock/connect sock host port)
        (future/then-next
         #(round-trip sock
                      m/StartupMessage
                      {:version-major 3
                       :version-minor 0
                       :parameters    (select-keys opts [:database :user])}
                      opts)))))

(comment
  (future/get (connect {:host     "localhost"
                        :port     4401
                        :database "postgres"
                        :user     "postgres"
                        :password "password"})))
