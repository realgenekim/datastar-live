(ns datastar-live.core
  "Request-bound Datastar SSE hubs and process-local live views.

   Hubs are the explicit low-level migration surface. Local views own their
   route, stable region, authoritative renderer, scoped connections, and
   refresh operation. Neither surface imposes a maximum stream age."
  (:require
   [hiccup2.core :as h]
   [starfederation.datastar.clojure.adapter.http-kit :as hk]
   [starfederation.datastar.clojure.api :as d*])
  (:import
   (java.nio.charset StandardCharsets)
   (java.util UUID)
   (java.util.concurrent ArrayBlockingQueue ExecutorService RejectedExecutionException
                          ThreadFactory ThreadPoolExecutor ThreadPoolExecutor$AbortPolicy
                          TimeUnit)))

(defrecord Hub [id state ^ExecutorService writer enqueue-lock on-event])
(defrecord LocalView [id path scope render region-id hub])

(defn- now-ms [] (System/currentTimeMillis))

(defn- emit! [hub event data]
  (when-let [f (:on-event hub)]
    (try
      (f (merge {:event event :hub (:id hub) :at-ms (now-ms)} data))
      (catch Throwable _))))

(defn- daemon-thread-factory [id]
  (reify ThreadFactory
    (newThread [_ runnable]
      (doto (Thread. runnable (str "datastar-live-" (name id)))
        (.setDaemon true)))))

(defn hub
  "Create an independent request-bound connection hub.

   Options:
   - `:id` required keyword, used only for identity and telemetry.
   - `:on-event` optional callback receiving lifecycle maps.

   Broadcast work is serialized on one daemon writer. There is no timer or
   per-connection sleeping future."
  [{:keys [id on-event queue-capacity]
    :or {queue-capacity 1024}}]
  (when-not (keyword? id)
    (throw (ex-info "hub :id must be a keyword" {:id id})))
  (when-not (pos-int? queue-capacity)
    (throw (ex-info "hub :queue-capacity must be a positive integer"
                    {:queue-capacity queue-capacity})))
  (->Hub id
         (atom {:stopped? false
                :connections {}
                :opened 0
                :closed 0
                :write-failures 0})
         (ThreadPoolExecutor. 1 1 0 TimeUnit/MILLISECONDS
                              (ArrayBlockingQueue. queue-capacity)
                              (daemon-thread-factory id)
                              (ThreadPoolExecutor$AbortPolicy.))
         (Object.)
         on-event))

(defn stopped? [hub]
  (true? (:stopped? @(:state hub))))

(defn connections
  "Return an immutable snapshot of the hub's current SDK generators.
   The registry itself is never exposed."
  [hub]
  (vec (keys (:connections @(:state hub)))))

(defn subscriber-count [hub]
  (count (:connections @(:state hub))))

(defn- as-hub [value]
  (if (instance? LocalView value) (:hub value) value))

(defn stats
  "Read-only operational counters. Connection membership is intentionally
   summarized rather than exposed as mutable state."
  [hub-or-view]
  (let [hub (as-hub hub-or-view)
        {:keys [stopped? connections opened closed write-failures]} @(:state hub)]
    {:id (:id hub)
     :stopped? stopped?
     :connections (count connections)
     :opened opened
     :closed closed
     :write-failures write-failures
     :queued-writes (if (instance? ThreadPoolExecutor (:writer hub))
                      (.size (.getQueue ^ThreadPoolExecutor (:writer hub)))
                      0)}))

(defn- register! [hub sse-gen connection-data]
  (let [accepted? (volatile! false)
        opened-at (now-ms)]
    (swap! (:state hub)
           (fn [state]
             (if (:stopped? state)
               state
               (do
                 (vreset! accepted? true)
                 (-> state
                     (assoc-in [:connections sse-gen]
                               {:opened-at-ms opened-at
                                :data connection-data})
                     (update :opened inc))))))
    (if @accepted?
      (do (emit! hub :connected {:connections (subscriber-count hub)}) true)
      (do
        (try (d*/close-sse! sse-gen) (catch Throwable _))
        false))))

(defn retire!
  "Idempotently remove a generator from a hub. Returns true only when present."
  [hub sse-gen]
  (let [removed? (volatile! false)]
    (swap! (:state hub)
           (fn [state]
             (if (contains? (:connections state) sse-gen)
               (do
                 (vreset! removed? true)
                 (-> state
                     (update :connections dissoc sse-gen)
                     (update :closed inc)))
               state)))
    (when @removed?
      (emit! hub :disconnected {:connections (subscriber-count hub)}))
    @removed?))

(defn- record-write-failure! [hub sse-gen error]
  (swap! (:state hub) update :write-failures inc)
  (retire! hub sse-gen)
  (try (d*/close-sse! sse-gen) (catch Throwable _))
  (emit! hub :write-failed {:error error
                            :connections (subscriber-count hub)}))

(defn send!
  "Synchronously and serially apply `write-fn` to one registered generator.
   Returns false and retires the connection when the write throws."
  [hub sse-gen write-fn]
  (if-not (contains? (:connections @(:state hub)) sse-gen)
    false
    (try
      (d*/lock-sse! sse-gen (write-fn sse-gen))
      true
      (catch Throwable error
        (record-write-failure! hub sse-gen error)
        false))))

(defn- submit! [hub task]
  (try
    (.execute ^ExecutorService (:writer hub) ^Runnable task)
    true
    (catch RejectedExecutionException error
      (emit! hub :queue-full {:error error})
      false)))

(defn- publish-selected! [hub selected write-fn]
  (if (or (stopped? hub) (empty? selected))
    0
    (if (submit! hub
                 (reify Runnable
                   (run [_]
                     (doseq [sse-gen selected]
                       (send! hub sse-gen write-fn)))))
      (count selected)
      0)))

(defn publish!
  "Queue one ordered broadcast. `write-fn` receives each SDK generator.
   Returns the number of connections captured for scheduling, not a delivery
   acknowledgement. Failed writes retire their connection."
  [hub write-fn]
  (locking (:enqueue-lock hub)
    (publish-selected! hub (connections hub) write-fn)))

(defn await-idle!
  "Wait until all work queued before this call has run. Intended for tests and
   controlled shutdown, not request handlers."
  ([hub] (await-idle! hub 5000))
  ([hub timeout-ms]
   (let [done (promise)
         deadline (+ (System/nanoTime) (* 1000000 timeout-ms))
         marker (reify Runnable
                  (run [_] (deliver done true)))]
     (loop []
       (let [submitted? (try
                          (.execute ^ExecutorService (:writer hub) ^Runnable marker)
                          true
                          (catch RejectedExecutionException _ false))]
         (if submitted?
           (boolean (deref done
                           (max 1 (quot (- deadline (System/nanoTime)) 1000000))
                           false))
           (if (< (System/nanoTime) deadline)
             (do (Thread/sleep 1) (recur))
             false)))))))

(defn sse-response
  "Create an http-kit Datastar SSE response owned by `hub`.

   `:on-connect` is queued as the first write for the connecting generator and
   must send complete current state only to that generator. `:connection-data`
   is private metadata used by scoped local views."
  ([hub request] (sse-response hub request {}))
  ([hub request {:keys [on-connect connection-data]}]
   (hk/->sse-response
    request
    {hk/on-open
     (fn [sse-gen]
       (locking (:enqueue-lock hub)
         (when (register! hub sse-gen connection-data)
           (when on-connect
             (submit! hub
                      (reify Runnable
                        (run [_]
                          (send! hub sse-gen on-connect))))))))

     hk/on-close
     (fn [sse-gen _status]
       (retire! hub sse-gen))

     :headers {"X-Accel-Buffering" "no"}})))

(defn stop!
  "Terminal, idempotent hub shutdown. New connections are refused, current
   connections are closed, and the writer daemon is stopped."
  [hub-or-view]
  (let [hub (as-hub hub-or-view)
        stopped-now? (volatile! false)
        to-close (volatile! [])]
    (swap! (:state hub)
           (fn [state]
             (if (:stopped? state)
               state
               (do
                 (vreset! stopped-now? true)
                 (vreset! to-close (vec (keys (:connections state))))
                 (-> state
                     (assoc :stopped? true :connections {})
                     (update :closed + (count @to-close)))))))
    (when @stopped-now?
      (doseq [sse-gen @to-close]
        (try (d*/close-sse! sse-gen) (catch Throwable _)))
      (.shutdownNow ^ExecutorService (:writer hub))
      (emit! hub :stopped {:closed-connections (count @to-close)}))
    @stopped-now?))

(defn- qualified-id? [value]
  (and (keyword? value) (namespace value)))

(defn- scope-key? [value]
  (or (keyword? value)
      (string? value)
      (integer? value)
      (instance? UUID value)
      (and (vector? value) (every? scope-key? value))))

(defn- hex-id [value]
  (let [bytes (.getBytes (str value) StandardCharsets/UTF_8)]
    (str "datastar-live-"
         (apply str (map #(format "%02x" (bit-and (int %) 0xff)) bytes)))))

(defn local-view
  "Create an opaque process-local view.

   Required options are a qualified keyword `:id`, absolute `:path`, request
   to stable-key `:scope`, and scope to Hiccup `:render`."
  [{:keys [id path scope render on-event]}]
  (when-not (qualified-id? id)
    (throw (ex-info "local-view :id must be a qualified keyword" {:id id})))
  (when-not (and (string? path) (.startsWith path "/"))
    (throw (ex-info "local-view :path must be an absolute path" {:path path})))
  (when-not (fn? scope)
    (throw (ex-info "local-view :scope must be a function" {})))
  (when-not (fn? render)
    (throw (ex-info "local-view :render must be a function" {})))
  (->LocalView id path scope render (hex-id id) (hub {:id id :on-event on-event})))

(defn- rendered-html [view scope]
  (let [html (str (h/html ((:render view) scope)))
        duplicate-id (re-pattern
                      (str "(?i)\\bid=[\\\"']"
                           (java.util.regex.Pattern/quote (:region-id view))
                           "[\\\"']"))]
    (when (re-find duplicate-id html)
      (throw (ex-info "local-view render must not reproduce its owned region id"
                      {:view (:id view) :region-id (:region-id view)})))
    html))

(defn- write-view! [view sse-gen scope]
  (d*/patch-elements!
   sse-gen
   (rendered-html view scope)
   {d*/selector (str "#" (:region-id view))
    d*/patch-mode d*/pm-inner}))

(defn route
  "Return the only documented Reitit route for a local view."
  [view]
  [(:path view)
   {:get
    {:muuntaja false
     :handler
     (fn [request]
       (let [scope ((:scope view) request)]
         (if (nil? scope)
           {:status 204 :body ""}
           (do
             (when-not (scope-key? scope)
               (throw (ex-info "local-view scope returned an unsupported key"
                               {:view (:id view) :scope scope})))
             (sse-response
              (:hub view) request
              {:connection-data {:scope scope}
               :on-connect #(write-view! view % scope)})))))}}])

(defn region
  "Render the stable view region and its generated app-lifetime subscription."
  ([view] (region view {}))
  ([view attrs]
   (when (or (contains? attrs :id) (contains? attrs :data-star-init))
     (throw (ex-info "local-view region owns :id and :data-star-init"
                     {:reserved (select-keys attrs [:id :data-star-init])})))
   [:div (merge attrs
                {:id (:region-id view)
                 :data-star-init
                 (str "@get('" (:path view)
                      "',{retry:'always',retryMaxCount:1000000})")})]))

(defn refresh!
  "Render and queue current authoritative state for one scope. Returns the
   number of matching connections scheduled."
  [view scope]
  (when-not (scope-key? scope)
    (throw (ex-info "refresh! requires a stable scope key"
                    {:view (:id view) :scope scope})))
  (let [hub (:hub view)
        selected (->> (:connections @(:state hub))
                      (keep (fn [[sse-gen {:keys [data]}]]
                              (when (= scope (:scope data)) sse-gen)))
                      vec)
        html (rendered-html view scope)]
    (locking (:enqueue-lock hub)
      (publish-selected!
       hub selected
       #(d*/patch-elements!
         % html
         {d*/selector (str "#" (:region-id view))
          d*/patch-mode d*/pm-inner})))))

(defn refresh-all!
  "Refresh every currently connected scope once."
  [view]
  (let [scopes (->> (:connections @(:state (:hub view)))
                    vals
                    (keep #(get-in % [:data :scope]))
                    set)]
    (reduce + (map #(refresh! view %) scopes))))

(defn view-stats [view]
  (assoc (stats (:hub view))
         :view-id (:id view)
         :path (:path view)
         :region-id (:region-id view)))

(defn stop-view! [view]
  (stop! (:hub view)))
