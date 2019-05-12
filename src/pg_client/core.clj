(ns pg-client.core
  (:require
   [pg-client.messages.frontend :as m.f]
   [pg-client.messages.backend  :as m.b]
   [pg-client.future :as future]
   [pg-client.socket :as sock]
   [pg-client.util :as util]))

(defmulti handle-response (fn [conn spec resp] spec))

(defn- send-request [conn req-spec data]
  (let [sock (:sock @conn)]
    (sock/write sock (m.f/encode req-spec data))))

(defn- receive-response [conn]
  (let [sock (:sock @conn)]
    (-> (sock/read sock m.b/header-length)
        (future/then-apply m.b/decode-header)
        (future/then-compose
         (fn [{:keys [tag length]}]

           (prn [tag length])

           (let [resp-spec (m.b/tag->spec tag)]
             (-> (sock/read sock (- length 4))
                 (future/then-apply #(m.b/decode-body resp-spec %))
                 (future/then-compose #(handle-response conn resp-spec %)))))))))

(defn round-trip [conn req-spec data]
  (-> (send-request conn req-spec data)
      (future/then-next #(receive-response conn))))

(defmulti handle-auth-response (fn [conn resp] (:header resp)))

(defmethod handle-response m.b/Authentication [conn spec resp]
  (handle-auth-response conn resp))

(defmethod handle-auth-response :AuthenticationMD5Password [conn resp]
  (let [user     (-> @conn :opts :user)
        password (-> @conn :opts :password)
        salt     (-> resp :body :salt)
        digest   (util/postgres-md5-password-hash user password salt)]
    (round-trip conn m.f/PasswordMessage {:password digest})))

(defmethod handle-auth-response :AuthenticationOk [sock resp]
  (receive-response sock))

(defmethod handle-response m.b/ParameterStatus [conn spec resp]
  (prn resp)

  (receive-response conn))

(defmethod handle-response m.b/BackendKeyData [conn spec resp]
  (prn resp)

  (receive-response conn))

(defmethod handle-response m.b/ReadyForQuery [conn spec resp]
  (prn resp)

  (future/completed nil))

(defmethod handle-response m.b/ErrorResponse [conn spec resp]
  (prn resp)

  (future/completed nil))



(defn connect [{:keys [host port] :as opts}]
  (let [sock (sock/open)
        conn (atom {:sock sock
                    :opts opts})]
    (-> (sock/connect sock host port)
        (future/then-next
         #(round-trip conn
                      m.f/StartupMessage
                      {:version-major 3
                       :version-minor 0
                       :parameters    (select-keys opts [:database :user])}))
        (future/then-apply (constantly conn)))))

(defn query [conn q]
  (round-trip conn m.f/Query {:query q}))

(comment
  (let [conn (future/get (connect {:host     "localhost"
                                   :port     4401
                                   :database "postgres"
                                   :user     "postgres"
                                   :password "password"}))]
    (future/get (query conn "select 1"))))
