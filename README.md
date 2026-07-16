# datastar-live

Request-bound Datastar SSE lifecycle and process-local live views for Clojure
and http-kit.

The library has two intentionally separate surfaces:

- `hub`, `sse-response`, `publish!`, and `send!` are the low-level migration
  boundary for applications that already own their render and patch functions.
- `local-view`, `route`, `region`, and `refresh!` are the semantic API for new
  server-authoritative regions.

Both surfaces keep connection registries private, serialize broadcasts, reap
failed writers, target reconnect paint to the connecting stream, and impose a
finite stream lifetime. One writer and one scheduler are shared per hub; no
thread, timer, or sleeper is created per connection.

```clojure
io.github.realgenekim/datastar-live
{:git/url "https://github.com/realgenekim/datastar-live.git"
 :git/sha "<pin-an-immutable-sha>"}
```

The first release targets the Datastar Clojure SDK RC8 used by
social-media-writer. The public API does not expose an SDK version promise, and
the same suite runs under the RC11 override with `clojure -M:test-rc11`.

## Transport migration

```clojure
(require '[datastar-live.core :as live])

(defonce browser-streams (live/hub {:id ::browser-streams}))

(defn handler [request]
  (live/sse-response
   browser-streams request
   {:on-connect (fn [sse-gen]
                  ;; Complete current state, sent only to this connection.
                  (push-all-to! sse-gen))}))

(live/publish! browser-streams
  (fn [sse-gen]
    (d*/patch-elements! sse-gen html)))
```

The low-level hub defaults to a 15-second heartbeat and a 60-second maximum
connection age. The heartbeat catches generators that report a failed write.
Maximum age is the hard safety bound for adapters that miss their close callback
and continue accepting writes after the browser socket has disappeared. Datastar
reconnects after the server closes the aged stream. These durations can be
overridden with `:heartbeat-ms` and `:max-age-ms` for infrastructure-specific
needs and deterministic tests.

`publish!` reports how many current connections were captured for bounded queue
submission. It never claims network delivery. A write result other than exact
`false` means only that the SDK accepted the write attempt.

## Local view

```clojure
(defonce status-view
  (live/local-view
   {:id ::status
    :path "/api/live/status"
    :scope (constantly :local)
    :render (fn [_] [:span "Ready"])}))

(live/route status-view)       ; Reitit route data
(live/region status-view)      ; stable region with generated subscription
(live/refresh! status-view :local)
(live/stats status-view)
(live/connection-stats status-view)
(live/stop! status-view)
```

`local-view` is process-local. It is not a distributed invalidation system.

## Operational telemetry

`stats` exposes aggregate open, close, accepted-write, failed-write, heartbeat,
queue-rejection, scheduler-failure, and retirement-reason counters plus a
bounded recent-retirement history. `connection-stats` exposes generated
connection IDs and lifecycle timestamps. Neither surface returns raw SDK
generators, request data, local-view scope values, URLs, query parameters, or
application-provided connection metadata.

The optional `:on-event` callback receives the same privacy-safe lifecycle
fields. Callback failures are isolated from connection lifecycle processing.
Call `stop!` during component shutdown to close streams and terminate both
bounded executors.
