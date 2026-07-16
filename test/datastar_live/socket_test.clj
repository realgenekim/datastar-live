(ns datastar-live.socket-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [datastar-live.core :as live]
   [org.httpkit.server :as server]
   [starfederation.datastar.clojure.adapter.http-kit :as hk]
   [starfederation.datastar.clojure.api :as d*])
  (:import
   (java.io BufferedReader InputStreamReader OutputStreamWriter)
   (java.net Socket)))

(defn- wait-until [timeout-ms pred]
  (let [deadline (+ (System/nanoTime) (* timeout-ms 1000000))]
    (loop []
      (if (pred)
        true
        (if (< (System/nanoTime) deadline)
          (do (Thread/sleep 5) (recur))
          false)))))

(defn- open-client! [port]
  (let [socket (doto (Socket. "127.0.0.1" (int port))
                 (.setSoTimeout 3000))
        writer (OutputStreamWriter. (.getOutputStream socket))]
    (.write writer
            (str "GET /events HTTP/1.1\r\n"
                 "Host: 127.0.0.1:" port "\r\n"
                 "Accept: text/event-stream\r\n"
                 "Connection: close\r\n\r\n"))
    (.flush writer)
    {:socket socket
     :reader (BufferedReader. (InputStreamReader. (.getInputStream socket)))}))

(defn- await-first-event! [{:keys [^BufferedReader reader]}]
  (loop [reading-headers? true
         event-lines []]
    (let [line (.readLine reader)]
      (cond
        (nil? line)
        (throw (ex-info "SSE socket closed before its first event" {}))

        reading-headers?
        (recur (not (str/blank? line)) event-lines)

        (and (str/blank? line) (seq event-lines))
        event-lines

        (str/blank? line)
        (recur false event-lines)

        :else
        (recur false (conj event-lines line))))))

(defn- close-abruptly! [{:keys [^Socket socket]}]
  (when-not (.isClosed socket)
    (.setSoLinger socket true 0)
    (.close socket)))

(defn- start-server! [handler]
  (server/run-server handler {:ip "127.0.0.1" :port 0}))

(defn- server-port [stop-server]
  (:local-port (meta stop-server)))

(defn- live-handler [hub]
  (fn [request]
    (live/sse-response hub request
                       {:on-connect #(d*/patch-signals! % "{}")})))

(defn- hub-thread-count [hub-id]
  (let [prefix (str "datastar-live-" (name hub-id) "-")]
    (->> (keys (Thread/getAllStackTraces))
         (filter #(.isAlive ^Thread %))
         (filter #(str/starts-with? (.getName ^Thread %) prefix))
         count)))

(deftest real-http-kit-overlap-reconnects-leave-no-subscribers-or-writers
  (let [hub-id ::overlap-100
        hub (live/hub {:id hub-id :heartbeat-ms 5 :max-age-ms 40})
        stop-server (start-server! (live-handler hub))
        port (server-port stop-server)
        current (atom nil)
        maximum-subscribers (atom 0)]
    (try
      (dotimes [_ 100]
        (let [next-client (open-client! port)]
          (await-first-event! next-client)
          (swap! maximum-subscribers max (live/subscriber-count hub))
          (when-let [old-client @current]
            (close-abruptly! old-client)
            (is (wait-until 1000 #(<= (live/subscriber-count hub) 1))))
          (reset! current next-client)))
      (close-abruptly! @current)
      (reset! current nil)
      (is (wait-until 1000 #(zero? (live/subscriber-count hub))))
      (is (= 100 (:opened (live/stats hub))))
      (is (= 100 (:closed (live/stats hub))))
      (is (= 100 (reduce + (vals (:retired-by (live/stats hub))))))
      (is (<= @maximum-subscribers 2)
          "overlap never creates an unbounded registry")
      (is (<= (hub-thread-count hub-id) 2)
          "one writer and one scheduler are shared by every connection")
      (finally
        (when-let [client @current] (close-abruptly! client))
        (stop-server :timeout 100)
        (live/stop! hub)))
    (is (wait-until 1000 #(zero? (hub-thread-count hub-id)))
        "stopping the hub retires its bounded background executors")))

(deftest max-age-retires-a-real-socket-when-hub-close-callback-is-omitted
  (let [hub (live/hub {:id ::missed-on-close
                       :heartbeat-ms 20
                       :max-age-ms 100})
        real-response hk/->sse-response
        handler (fn [request]
                  ;; Reproduce the historical integration bug: the adapter
                  ;; closes its SDK generator, but the hub never hears the
                  ;; channel's close callback.
                  (with-redefs [hk/->sse-response
                                (fn [req opts]
                                  (real-response req (dissoc opts hk/on-close)))]
                    ((live-handler hub) request)))
        stop-server (start-server! handler)
        client (open-client! (server-port stop-server))]
    (try
      (await-first-event! client)
      (is (wait-until 1000 #(= 1 (live/subscriber-count hub))))
      (close-abruptly! client)
      (is (wait-until 1000 #(zero? (live/subscriber-count hub)))
          "the shared scheduler imposes a hard bound despite the missed callback")
      (let [stats (live/stats hub)]
        (is (= 1 (get-in stats [:retired-by :max-age])))
        (is (pos? (:heartbeats stats))))
      (finally
        (close-abruptly! client)
        (stop-server :timeout 100)
        (live/stop! hub)))))
