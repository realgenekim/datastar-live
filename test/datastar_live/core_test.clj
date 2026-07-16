(ns datastar-live.core-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [datastar-live.core :as live]
   [starfederation.datastar.clojure.adapter.http-kit :as hk]
   [starfederation.datastar.clojure.adapter.test :as at]
   [starfederation.datastar.clojure.api :as d*]))

(defn- callbacks-response [captured]
  (fn [_request opts]
    (reset! captured opts)
    {:status 200 :body :stream}))

(defn- wait-until [timeout-ms pred]
  (let [deadline (+ (System/nanoTime) (* timeout-ms 1000000))]
    (loop []
      (if (pred)
        true
        (if (< (System/nanoTime) deadline)
          (do (Thread/sleep 5) (recur))
          false)))))

(deftest lifecycle-registration-is-private-and-idempotent
  (let [captured (atom nil)
        events (atom [])
        hub (live/hub {:id ::test :on-event #(swap! events conj %)})
        gen (at/->sse-recorder)]
    (with-redefs [hk/->sse-response (callbacks-response captured)]
      (is (= {:status 200 :body :stream}
             (live/sse-response hub {})))
      ((get @captured hk/on-open) gen)
      (is (= 1 (live/subscriber-count hub)))
      (is (= [gen] (live/connections hub)))
      ((get @captured hk/on-close) gen :client-close)
      ((get @captured hk/on-close) gen :duplicate-close)
      (is (zero? (live/subscriber-count hub)))
      (is (= 1 (:closed (live/stats hub))))
      (is (= [:connected :disconnected]
             (mapv :event @events))))
    (is (true? (live/stop! hub)))
    (is (false? (live/stop! hub)))))

(deftest on-connect-is-targeted-and-ordered-before-broadcast
  (let [captured (atom nil)
        writes (atom [])
        hub (live/hub {:id ::ordered})
        gen-a (at/->sse-recorder)
        gen-b (at/->sse-recorder)]
    (with-redefs [hk/->sse-response (callbacks-response captured)]
      (live/sse-response hub {} {:on-connect #(swap! writes conj [:initial %])})
      ((get @captured hk/on-open) gen-a)
      (live/publish! hub #(swap! writes conj [:broadcast %]))
      (is (true? (live/await-idle! hub)))
      (is (= [[:initial gen-a] [:broadcast gen-a]] @writes))

      (reset! writes [])
      (live/sse-response hub {} {:on-connect #(swap! writes conj [:initial %])})
      ((get @captured hk/on-open) gen-b)
      (is (true? (live/await-idle! hub)))
      (is (= [[:initial gen-b]] @writes)))
    (live/stop! hub)))

(deftest failed-broadcast-retires-only-the-failed-connection
  (let [captured (atom nil)
        hub (live/hub {:id ::reaping})
        good (at/->sse-recorder)
        bad (at/->sse-recorder)
        seen (atom [])]
    (with-redefs [hk/->sse-response (callbacks-response captured)]
      (live/sse-response hub {})
      ((get @captured hk/on-open) good)
      ((get @captured hk/on-open) bad)
      (is (= 2 (live/publish! hub
                              (fn [gen]
                                (if (identical? gen bad)
                                  (throw (ex-info "boom" {}))
                                  (swap! seen conj gen))))))
      (is (true? (live/await-idle! hub)))
      (is (= [good] @seen))
      (is (= [good] (live/connections hub)))
      (is (= 1 (:write-failures (live/stats hub)))))
    (live/stop! hub)))

(deftest false-write-result-retires-dead-http-kit-connection
  (let [captured (atom nil)
        events (atom [])
        hub (live/hub {:id ::false-write
                       :on-event #(swap! events conj %)})
        gen (at/->sse-recorder)]
    (with-redefs [hk/->sse-response (callbacks-response captured)]
      (live/sse-response hub {})
      ((get @captured hk/on-open) gen)
      (is (false? (live/send! hub gen (constantly false))))
      (is (zero? (live/subscriber-count hub)))
      (is (= 1 (:write-failures (live/stats hub))))
      (is (= [:connected :disconnected :write-failed]
             (mapv :event @events)))
      (is (= :write-returned-false
             (-> @events last :retirement-reason))))
    (live/stop! hub)))

(deftest lifecycle-telemetry-is-bounded-and-privacy-safe
  (let [captured (atom nil)
        events (atom [])
        hub (live/hub {:id ::telemetry
                       :recent-retirements-limit 2
                       :on-event #(swap! events conj %)})
        secret "never-emit-this-secret"
        generators (repeatedly 3 at/->sse-recorder)]
    (with-redefs [hk/->sse-response (callbacks-response captured)]
      (doseq [gen generators]
        (live/sse-response hub {} {:connection-data {:access-token secret}})
        ((get @captured hk/on-open) gen)
        (is (= #{:connection-id :opened-at-ms :phase
                 :last-successful-write-at-ms :last-heartbeat-at-ms
                 :successful-writes}
               (set (keys (first (live/connection-stats hub))))))
        (is (true? (live/send! hub gen (constantly true))))
        ((get @captured hk/on-close) gen (Object.))))
    (let [stats (live/stats hub)]
      (is (= 3 (:opened stats)))
      (is (= 3 (:closed stats)))
      (is (= 2 (count (:recent-retirements stats))))
      (is (= {:on-close 3} (:retired-by stats)))
      (is (every? #(= :on-close (:retirement-reason %))
                  (:recent-retirements stats)))
      (is (not (str/includes? (pr-str stats) secret)))
      (is (not (str/includes? (pr-str @events) secret))))
    (live/stop! hub)))

(deftest heartbeat-retires-an-sdk-generator-that-reports-write-failure
  (let [captured (atom nil)
        hub (live/hub {:id ::heartbeat-failure
                       :heartbeat-ms 10
                       :max-age-ms 1000})
        gen (at/->sse-recorder)]
    (with-redefs [hk/->sse-response (callbacks-response captured)
                  d*/patch-signals! (constantly false)]
      (live/sse-response hub {})
      ((get @captured hk/on-open) gen)
      (is (wait-until 500 #(zero? (live/subscriber-count hub))))
      (is (= 1 (:write-failures (live/stats hub))))
      (is (= 1 (get-in (live/stats hub)
                       [:retired-by :write-returned-false]))))
    (live/stop! hub)))

(deftest local-view-owns-route-region-scope-and-refresh
  (let [captured (atom nil)
        rendered (atom [])
        view (live/local-view
               {:id ::status
                :path "/api/live/status"
                :scope #(get-in % [:identity :account])
                :render (fn [scope]
                          (swap! rendered conj scope)
                          [:span.status (str "account " scope)])})
        [_ route-data] (live/route view)
        handler (get-in route-data [:get :handler])
        region (live/region view {:class "slot"})
        gen-a (at/->sse-recorder)
        gen-b (at/->sse-recorder)]
    (is (= :div (first region)))
    (is (= "slot" (get-in region [1 :class])))
    (is (str/starts-with? (get-in region [1 :id]) "datastar-live-"))
    (is (str/includes? (get-in region [1 :data-star-init]) "/api/live/status"))
    (is (= {:status 204 :body ""} (handler {})))

    (with-redefs [hk/->sse-response (callbacks-response captured)]
      (handler {:identity {:account :a}})
      ((get @captured hk/on-open) gen-a)
      (handler {:identity {:account :b}})
      ((get @captured hk/on-open) gen-b)
      (is (true? (live/await-idle! (:hub view))))
      (is (= [:a :b] @rendered))

      (reset! rendered [])
      (is (= 1 (live/refresh! view :a)))
      (is (true? (live/await-idle! (:hub view))))
      (is (= [:a] @rendered))
      (is (= 2 (:connections (live/stats view)))))
    (live/stop! view)))

(deftest region-rejects-transport-overrides
  (let [view (live/local-view {:id ::reserved
                               :path "/api/live/reserved"
                               :scope (constantly :local)
                               :render (constantly [:span "ok"])})]
    (is (thrown? Exception (live/region view {:id "caller-id"})))
    (is (thrown? Exception (live/region view {:data-star-init "@get('/wrong')"})))
    (live/stop! view)))

(deftest frame-output-is-valid-datastar-sse
  (let [view (live/local-view {:id ::wire
                               :path "/api/live/wire"
                               :scope (constantly :local)
                               :render (constantly [:span "hello"])})
        captured (atom nil)
        [_ route-data] (live/route view)]
    (with-redefs [hk/->sse-response (callbacks-response captured)]
      ((get-in route-data [:get :handler]) {})
      (let [gen (at/->sse-recorder)]
        ((get @captured hk/on-open) gen)
        (is (true? (live/await-idle! (:hub view))))
        (let [events @(.-!rec gen)]
          (is (= 1 (count events)))
          (is (str/includes? (first events) "event: datastar-patch-elements"))
          (is (str/includes? (first events) "data: mode inner"))
          (is (str/includes? (first events) "<span>hello</span>")))))
    (live/stop! view)))

(deftest local-view-rejects-unstable-scopes-and-region-recursion
  (let [unstable (live/local-view {:id ::unstable
                                   :path "/api/live/unstable"
                                   :scope (constantly (Object.))
                                   :render (constantly [:span "nope"])})
        [_ route-data] (live/route unstable)]
    (is (thrown-with-msg? Exception #"unsupported key"
                          ((get-in route-data [:get :handler]) {})))
    (live/stop! unstable))

  (let [recursive (atom nil)
        view (live/local-view {:id ::recursive
                               :path "/api/live/recursive"
                               :scope (constantly :local)
                               :render (fn [_] [:div {:id @recursive}])})]
    (reset! recursive (:region-id view))
    (is (thrown-with-msg? Exception #"must not reproduce"
                          (live/refresh! view :local)))
    (live/stop! view)))

(deftest bounded-writer-queue-rejects-overflow-explicitly
  (let [captured (atom nil)
        events (atom [])
        hub (live/hub {:id ::bounded
                       :queue-capacity 1
                       :on-event #(swap! events conj %)})
        gen (at/->sse-recorder)
        rejected-gen (at/->sse-recorder)
        started (promise)
        release (promise)]
    (with-redefs [hk/->sse-response (callbacks-response captured)]
      (live/sse-response hub {})
      ((get @captured hk/on-open) gen)
      (is (= 1 (live/publish! hub (fn [_]
                                    (deliver started true)
                                    @release))))
      (is (true? (deref started 1000 false)))
      (is (= 1 (live/publish! hub (constantly nil))))
      (is (zero? (live/publish! hub (constantly nil))))
      (is (some #(= :queue-full (:event %)) @events))
      (live/sse-response hub {} {:on-connect (constantly nil)})
      ((get @captured hk/on-open) rejected-gen)
      (is (= [gen] (live/connections hub))
          "a connection whose mandatory initial paint cannot queue is retired")
      (deliver release true)
      (is (true? (live/await-idle! hub))))
    (live/stop! hub)))
