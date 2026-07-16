(ns datastar-live.core
  "Request-bound Datastar SSE hubs and process-local live views.

   Hubs are the explicit low-level migration surface. Local views own their
   route, stable region, authoritative renderer, scoped connections, and
   refresh operation. Both surfaces bound stream age and background work."
  (:require
   [hiccup2.core :as h]
   [starfederation.datastar.clojure.adapter.http-kit :as hk]
   [starfederation.datastar.clojure.api :as d*])
  (:import
   (java.nio.charset StandardCharsets)
   (java.util UUID)
   (java.util.concurrent ArrayBlockingQueue ExecutorService RejectedExecutionException ScheduledThreadPoolExecutor ThreadFactory ThreadPoolExecutor ThreadPoolExecutor$AbortPolicy TimeUnit)
   (java.util.concurrent.atomic AtomicLong)))

(defrecord Hub [id state ^ExecutorService writer ^ScheduledThreadPoolExecutor scheduler
                enqueue-lock on-event heartbeat-ms max-age-ms
                recent-retirements-limit])
(defrecord LocalView [id path scope render region-id hub])

(defn- now-ms [] (System/currentTimeMillis))

(declare heartbeat! emit!)

(defn- guarded-heartbeat! [hub]
  (try
    (heartbeat! hub)
    (catch Throwable error
      (swap! (:state hub) update :scheduler-failures inc)
      (emit! hub :scheduler-failed
             {:error-class (.getName (class error))}))))

(defn- emit! [hub event data]
  (when-let [f (:on-event hub)]
    (try
      (f (merge {:event event :hub (:id hub) :at-ms (now-ms)} data))
      (catch Throwable _))))

(defn- daemon-thread-factory [id role]
  (let [counter (AtomicLong.)]
    (reify ThreadFactory
      (newThread [_ runnable]
        (doto (Thread. runnable
                       (str "datastar-live-" (name id) "-" (name role) "-"
                            (.incrementAndGet counter)))
          (.setDaemon true))))))

(defn- append-recent [entries entry limit]
  (let [entries (conj (vec entries) entry)]
    (if (> (count entries) limit)
      (subvec entries (- (count entries) limit))
      entries)))

(defn- safe-close-status [status]
  (cond
    (or (nil? status) (keyword? status) (number? status) (boolean? status)) status
    :else (str "<" (.getName (class status)) ">")))

(defn- public-connection [connection]
  (select-keys connection
               [:connection-id :opened-at-ms :phase :last-successful-write-at-ms
                :last-heartbeat-at-ms :successful-writes]))

(defn- retirement-record [connection reason details retired-at]
  (merge (public-connection connection)
         {:retired-at-ms retired-at
          :lifetime-ms (max 0 (- retired-at (:opened-at-ms connection)))
          :retirement-reason reason}
         (select-keys details
                      [:close-callback-at-ms :close-status :error-class :write-kind])))

(defn hub
  "Create an independent request-bound connection hub.

   Options:
   - `:id` required keyword, used only for identity and telemetry.
   - `:on-event` optional callback receiving lifecycle maps.
   - `:queue-capacity` positive integer, default 1024.
   - `:heartbeat-ms` positive integer, default 15000. One shared scheduler
     offers an empty Datastar signals patch to detect generators whose close
     callback was missed. At most one heartbeat broadcast can be pending.
   - `:max-age-ms` positive integer, default 60000. The same scheduler retires
     every connection by this age, bounding leaks even when the HTTP adapter
     omits its close callback and accepts writes after the peer disappeared.
   - `:recent-retirements-limit` bounded diagnostic history, default 128.

   Broadcast work is serialized on one bounded daemon writer. The heartbeat
   scheduler is shared by the hub; there is no timer or sleeper per connection."
  [{:keys [id on-event queue-capacity heartbeat-ms max-age-ms
           recent-retirements-limit]
    :or {queue-capacity 1024
         heartbeat-ms 15000
         max-age-ms 60000
         recent-retirements-limit 128}}]
  (when-not (keyword? id)
    (throw (ex-info "hub :id must be a keyword" {:id id})))
  (when-not (pos-int? queue-capacity)
    (throw (ex-info "hub :queue-capacity must be a positive integer"
                    {:queue-capacity queue-capacity})))
  (when-not (pos-int? heartbeat-ms)
    (throw (ex-info "hub :heartbeat-ms must be a positive integer"
                    {:heartbeat-ms heartbeat-ms})))
  (when-not (pos-int? max-age-ms)
    (throw (ex-info "hub :max-age-ms must be a positive integer"
                    {:max-age-ms max-age-ms})))
  (when-not (pos-int? recent-retirements-limit)
    (throw (ex-info "hub :recent-retirements-limit must be a positive integer"
                    {:recent-retirements-limit recent-retirements-limit})))
  (let [writer (ThreadPoolExecutor. 1 1 0 TimeUnit/MILLISECONDS
                                    (ArrayBlockingQueue. queue-capacity)
                                    (daemon-thread-factory id :writer)
                                    (ThreadPoolExecutor$AbortPolicy.))
        scheduler (doto (ScheduledThreadPoolExecutor.
                          1 (daemon-thread-factory id :scheduler))
                    (.setRemoveOnCancelPolicy true))
        hub (->Hub id
                   (atom {:stopped? false
                          :connections {}
                          :opened 0
                          :closed 0
                          :write-failures 0
                          :successful-writes 0
                          :heartbeats 0
                          :scheduler-failures 0
                          :queue-rejections 0
                          :heartbeat-pending? false
                          :retired-by {}
                          :recent-retirements []})
                   writer scheduler (Object.) on-event heartbeat-ms max-age-ms
                   recent-retirements-limit)]
    (.scheduleWithFixedDelay scheduler
                             ^Runnable #(guarded-heartbeat! hub)
                             heartbeat-ms heartbeat-ms TimeUnit/MILLISECONDS)
    hub))

(defn stopped? [hub]
  (true? (:stopped? @(:state hub))))

(defn connections
  "Return an immutable snapshot of the hub's current SDK generators.
   The registry itself is never exposed."
  [hub]
  (vec (keys (:connections @(:state hub)))))

(defn- as-hub [value]
  (if (instance? LocalView value) (:hub value) value))

(defn connection-stats
  "Return privacy-safe lifecycle snapshots for current connections. Raw SDK
   generators and application-owned connection data are never exposed."
  [hub-or-view]
  (let [hub (as-hub hub-or-view)]
    (->> (:connections @(:state hub))
         vals
         (map public-connection)
         (sort-by :opened-at-ms)
         vec)))

(defn subscriber-count [hub]
  (count (:connections @(:state hub))))

(defn stats
  "Read-only operational counters. Connection membership is intentionally
   summarized rather than exposed as mutable state."
  [hub-or-view]
  (let [hub (as-hub hub-or-view)
        {:keys [stopped? connections opened closed write-failures successful-writes
                heartbeats scheduler-failures queue-rejections heartbeat-pending?
                retired-by recent-retirements]} @(:state hub)]
    {:id (:id hub)
     :stopped? stopped?
     :connections (count connections)
     :opened opened
     :closed closed
     :write-failures write-failures
     :successful-writes successful-writes
     :heartbeats heartbeats
     :scheduler-failures scheduler-failures
     :heartbeat-ms (:heartbeat-ms hub)
     :max-age-ms (:max-age-ms hub)
     :heartbeat-pending? heartbeat-pending?
     :queue-rejections queue-rejections
     :retired-by retired-by
     :recent-retirements recent-retirements
     :queued-writes (if (instance? ThreadPoolExecutor (:writer hub))
                      (.size (.getQueue ^ThreadPoolExecutor (:writer hub)))
                      0)
     :scheduled-tasks (.size (.getQueue ^ScheduledThreadPoolExecutor
                                        (:scheduler hub)))}))

(defn- register!
  ([hub sse-gen connection-data]
   (register! hub sse-gen connection-data false))
  ([hub sse-gen connection-data initializing?]
   (let [result (volatile! :stopped)
         opened-at (now-ms)
         connection-id (str (UUID/randomUUID))]
     (swap! (:state hub)
            (fn [state]
              (cond
                (:stopped? state) state
                (contains? (:connections state) sse-gen)
                (do (vreset! result :existing) state)
                :else
                (do
                  (vreset! result :new)
                  (-> state
                      (assoc-in [:connections sse-gen]
                                {:connection-id connection-id
                                 :opened-at-ms opened-at
                                 :phase (if initializing? :initializing :live)
                                 :last-successful-write-at-ms nil
                                 :last-heartbeat-at-ms nil
                                 :successful-writes 0
                                 :data connection-data})
                      (update :opened inc))))))
     (case @result
       :new (do (emit! hub :connected {:connection-id connection-id
                                       :opened-at-ms opened-at
                                       :phase (if initializing? :initializing :live)
                                       :connections (subscriber-count hub)})
                true)
       :existing true
       (do
         (try (d*/close-sse! sse-gen) (catch Throwable _))
         false)))))

(defn- mark-live! [hub sse-gen]
  (swap! (:state hub)
         (fn [state]
           (if (contains? (:connections state) sse-gen)
             (assoc-in state [:connections sse-gen :phase] :live)
             state))))

(defn retire!
  "Idempotently remove a generator from a hub. Returns true only when present."
  ([hub sse-gen]
   (retire! hub sse-gen :explicit {}))
  ([hub sse-gen reason]
   (retire! hub sse-gen reason {}))
  ([hub sse-gen reason details]
   (let [removed (volatile! nil)
         retired-at (now-ms)
         details (cond-> details
                   (contains? details :close-status)
                   (update :close-status safe-close-status))]
     (swap! (:state hub)
            (fn [state]
              (if-let [connection (get-in state [:connections sse-gen])]
                (let [retirement (retirement-record connection reason details retired-at)]
                  (vreset! removed retirement)
                  (-> state
                      (update :connections dissoc sse-gen)
                      (update :closed inc)
                      (update-in [:retired-by reason] (fnil inc 0))
                      (update :recent-retirements append-recent retirement
                              (:recent-retirements-limit hub))))
                state)))
     (when-let [retirement @removed]
       (emit! hub :disconnected
              (assoc retirement :connections (subscriber-count hub))))
     (boolean @removed))))

(defn- record-write-failure! [hub sse-gen write-kind reason error]
  (let [connection (get-in @(:state hub) [:connections sse-gen])
        error-class (when error (.getName (class error)))]
    (when (retire! hub sse-gen reason {:error-class error-class
                                       :write-kind write-kind})
      (swap! (:state hub) update :write-failures inc)
      (try (d*/close-sse! sse-gen) (catch Throwable _))
      (emit! hub :write-failed {:connection-id (:connection-id connection)
                                :write-kind write-kind
                                :retirement-reason reason
                                :error-class error-class
                                :connections (subscriber-count hub)}))))

(defn- record-write-success! [hub sse-gen write-kind]
  (let [at (now-ms)]
    (swap! (:state hub)
           (fn [state]
             (if (contains? (:connections state) sse-gen)
               (let [state (-> state
                               (assoc-in [:connections sse-gen :last-successful-write-at-ms]
                                         at)
                               (update-in [:connections sse-gen :successful-writes] inc)
                               (update :successful-writes inc))]
                 (if (= write-kind :heartbeat)
                   (-> state
                       (assoc-in [:connections sse-gen :last-heartbeat-at-ms] at)
                       (update :heartbeats inc))
                   state))
               state)))))

(defn- send-kind! [hub sse-gen write-kind write-fn]
  (if-not (contains? (:connections @(:state hub)) sse-gen)
    false
    (try
      (if (false? (d*/lock-sse! sse-gen (write-fn sse-gen)))
        (do
          (record-write-failure! hub sse-gen write-kind :write-returned-false nil)
          false)
        (do (record-write-success! hub sse-gen write-kind) true))
      (catch Throwable error
        (record-write-failure! hub sse-gen write-kind :write-threw error)
        false))))

(defn send!
  "Synchronously apply `write-fn` to one registered generator. Returns false
   and retires the connection when the write throws or returns exactly false.
   The SDK/http-kit result means only that a write was accepted; it is not a
   network-delivery acknowledgement."
  [hub sse-gen write-fn]
  (send-kind! hub sse-gen :application write-fn))

(defn- submit! [hub task write-kind]
  (try
    (.execute ^ExecutorService (:writer hub) ^Runnable task)
    true
    (catch RejectedExecutionException error
      (swap! (:state hub) update :queue-rejections inc)
      (emit! hub :queue-full {:write-kind write-kind
                              :error-class (.getName (class error))})
      false)))

(defn- publish-selected! [hub selected write-kind write-fn]
  (if (or (stopped? hub) (empty? selected))
    0
    (if (submit! hub
                 (reify Runnable
                   (run [_]
                     (doseq [sse-gen selected]
                       (send-kind! hub sse-gen write-kind write-fn))))
                 write-kind)
      (count selected)
      0)))

(defn publish!
  "Queue one ordered broadcast. `write-fn` receives each SDK generator.
   Returns the number of connections captured for scheduling, not a delivery
   acknowledgement. Failed writes retire their connection."
  [hub write-fn]
  (locking (:enqueue-lock hub)
    (publish-selected! hub (connections hub) :application write-fn)))

(defn- heartbeat-connections [hub]
  (->> (:connections @(:state hub))
       (keep (fn [[sse-gen connection]]
               (when (= :live (:phase connection)) sse-gen)))
       vec))

(defn- retire-expired-connections! [hub]
  (let [at (now-ms)
        max-age-ms (:max-age-ms hub)]
    (doseq [[sse-gen connection] (:connections @(:state hub))
            :when (>= (- at (:opened-at-ms connection)) max-age-ms)]
      (when (retire! hub sse-gen :max-age)
        (try (d*/close-sse! sse-gen) (catch Throwable _))))))

(defn- clear-heartbeat-claim! [hub]
  (swap! (:state hub) assoc :heartbeat-pending? false))

(defn- heartbeat! [hub]
  (retire-expired-connections! hub)
  (let [selected (heartbeat-connections hub)
        claimed? (volatile! false)]
    (when (seq selected)
      (swap! (:state hub)
             (fn [state]
               (if (or (:stopped? state) (:heartbeat-pending? state))
                 state
                 (do (vreset! claimed? true)
                     (assoc state :heartbeat-pending? true)))))
      (when @claimed?
        (when-not
          (submit! hub
                   (reify Runnable
                     (run [_]
                       (try
                         (doseq [sse-gen selected]
                           (send-kind! hub sse-gen :heartbeat
                                       #(d*/patch-signals! % "{}")))
                         (finally (clear-heartbeat-claim! hub)))))
                   :heartbeat)
          (clear-heartbeat-claim! hub))))))

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
          (when (register! hub sse-gen connection-data (boolean on-connect))
            (when on-connect
              (when-not
                (submit! hub
                         (reify Runnable
                           (run [_]
                             (when (send-kind! hub sse-gen :initial on-connect)
                               (mark-live! hub sse-gen))))
                         :initial)
                ;; A browser without its authoritative first paint is not a
                ;; usable subscriber. Fail closed and let Datastar reconnect.
                (retire! hub sse-gen :initial-queue-full)
                (try (d*/close-sse! sse-gen) (catch Throwable _)))))))

      hk/on-close
      (fn [sse-gen status]
        (retire! hub sse-gen :on-close
                 {:close-callback-at-ms (now-ms)
                  :close-status status}))

      :headers {"X-Accel-Buffering" "no"}})))

(defn stop!
  "Terminal, idempotent hub shutdown. New connections are refused, current
   connections are closed, and the writer daemon is stopped."
  [hub-or-view]
  (let [hub (as-hub hub-or-view)
        stopped-now? (volatile! false)
        retired-connections (volatile! [])
        retired-at (now-ms)]
    (swap! (:state hub)
           (fn [state]
             (if (:stopped? state)
               state
               (do
                 (vreset! stopped-now? true)
                 (let [connections (:connections state)
                       retirements (mapv #(retirement-record % :hub-stopped {} retired-at)
                                         (vals connections))]
                   (vreset! retired-connections
                            (mapv vector (keys connections) retirements))
                   (-> state
                       (assoc :stopped? true
                              :connections {}
                              :heartbeat-pending? false)
                       (update :closed + (count connections))
                       (update-in [:retired-by :hub-stopped] (fnil + 0)
                                  (count connections))
                       (update :recent-retirements
                               (fn [recent]
                                 (reduce #(append-recent %1 %2
                                                         (:recent-retirements-limit hub))
                                         recent retirements)))))))))
    (when @stopped-now?
      (doseq [[sse-gen retirement] @retired-connections]
        (try (d*/close-sse! sse-gen) (catch Throwable _))
        (emit! hub :disconnected (assoc retirement :connections 0)))
      (.shutdownNow ^ScheduledThreadPoolExecutor (:scheduler hub))
      (.shutdownNow ^ExecutorService (:writer hub))
      (emit! hub :stopped {:closed-connections (count @retired-connections)}))
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
        hub selected :application
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
