(ns pg-client.core
  (:require
   [pg-client.messages :as m]
   [pg-client.future :as future]
   [pg-client.socket :as sock]
   [pg-client.util :as util]))

(defmulti handle-response (fn [sock resp ctx] (:name resp)))

(defn- send-request [sock req-spec data]
  (sock/write sock (m/encode req-spec data)))

(defn- receive-response [sock ctx]
  (-> (sock/read sock m/header-length)
      (future/then-apply m/decode-header)
      (future/then-compose (fn [{:keys [tag length]}]
                             (prn [tag length])
                             (let [resp-spec (m/tag->spec tag)]
                               (-> (sock/read sock (- length 4))
                                   (future/then-apply #(m/decode-body resp-spec %))))))
      (future/then-compose #(handle-response sock % ctx))))

(defn round-trip [sock req-spec data ctx]
  (-> (send-request sock req-spec data)
      (future/then-next #(receive-response sock ctx))))

(defmethod handle-response :AuthenticationMD5Password [sock
                                                       {:keys [salt]}
                                                       {:keys [user password]}]
  (let [digest (util/postgres-md5-password-hash user password salt)]
    (round-trip sock m/PasswordMessage {:password digest} nil)))

(defmethod handle-response :AuthenticationOk [sock resp ctx]
  (receive-response sock {}))

(defmethod handle-response :ParameterStatus [sock resp ctx]
  (prn resp)

  (receive-response sock {}))

(defmethod handle-response :BackendKeyData [sock resp ctx]
  (prn resp)

  (receive-response sock {}))

(defmethod handle-response :ReadyForQuery [sock resp ctx]
  (prn resp)
  (future/completed :ok))

(defn connect [{:keys [host port] :as opts}]
  (let [sock (sock/open)]
    (-> (sock/connect sock host port)
        (future/then-next
         #(round-trip sock
                      m/StartupMessage
                      {:version-major 3
                       :version-minor 0
                       :parameters    (select-keys opts [:database :user])}
                      opts))
        (future/then-apply (constantly {:sock sock})))))

(defn query [{:keys [sock]} q]
  (round-trip sock m/Query {:query q} nil))

(comment
  (let [conn (future/get (connect {:host     "localhost"
                                   :port     4401
                                   :database "postgres"
                                   :user     "postgres"
                                   :password "password"}))]
    (future/get (query conn "select 1 + 1"))))
