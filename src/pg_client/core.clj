(ns pg-client.core
  (:require
   [pg-client.messages.frontend :as m.f]
   [pg-client.messages.backend  :as m.b]
   [pg-client.future :as future]
   [pg-client.socket :as sock]
   [pg-client.util :as util]))

(defmulti handle-response (fn [sock spec resp ctx] spec))

(defn- send-request [sock req-spec data]
  (sock/write sock (m.f/encode req-spec data)))

(defn- receive-response [sock ctx]
  (-> (sock/read sock m.b/header-length)
      (future/then-apply m.b/decode-header)
      (future/then-compose
       (fn [{:keys [tag length]}]

         (prn [tag length])

         (let [resp-spec (m.b/tag->spec tag)]
           (-> (sock/read sock (- length 4))
               (future/then-apply #(m.b/decode-body resp-spec %))
               (future/then-compose #(handle-response sock resp-spec % ctx))))))))

(defn round-trip [sock req-spec data ctx]
  (-> (send-request sock req-spec data)
      (future/then-next #(receive-response sock ctx))))

(defmulti handle-auth-response (fn [sock resp ctx] (:header resp)))

(defmethod handle-response m.b/Authentication [sock spec resp ctx]
  (handle-auth-response sock resp ctx))

(defmethod handle-auth-response :AuthenticationMD5Password [sock resp {:keys [user password]}]
  (let [salt   (-> resp :body :salt)
        digest (util/postgres-md5-password-hash user password salt)]
    (round-trip sock m.f/PasswordMessage {:password digest} nil)))

(defmethod handle-auth-response :AuthenticationOk [sock resp ctx]
  (receive-response sock {}))

(defmethod handle-response m.b/ParameterStatus [sock spec resp ctx]
  (prn resp)

  (receive-response sock {}))

(defmethod handle-response m.b/BackendKeyData [sock spec resp ctx]
  (prn resp)

  (receive-response sock {}))

(defmethod handle-response m.b/ReadyForQuery [sock spec resp ctx]
  (prn resp)
  (future/completed :ok))

(defn connect [{:keys [host port] :as opts}]
  (let [sock (sock/open)]
    (-> (sock/connect sock host port)
        (future/then-next
         #(round-trip sock
                      m.f/StartupMessage
                      {:version-major 3
                       :version-minor 0
                       :parameters    (select-keys opts [:database :user])}
                      opts))
        (future/then-apply (constantly {:sock sock})))))

(defn query [{:keys [sock]} q]
  (round-trip sock m.f/Query {:query q} nil))

(comment
  (let [conn (future/get (connect {:host     "localhost"
                                   :port     4401
                                   :database "postgres"
                                   :user     "postgres"
                                   :password "password"}))]
    (future/get (query conn "select 1"))))
