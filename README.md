# datastar-live

Request-bound Datastar SSE lifecycle and process-local live views for Clojure
and http-kit.

The library has two intentionally separate surfaces:

- `hub`, `sse-response`, `publish!`, and `send!` are the low-level migration
  boundary for applications that already own their render and patch functions.
- `local-view`, `route`, `region`, and `refresh!` are the semantic API for new
  server-authoritative regions.

Both surfaces keep connection registries private, serialize broadcasts, reap
failed writers, target reconnect paint to the connecting stream, and impose no
hidden maximum stream lifetime.

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
(live/stop! status-view)
```

`local-view` is process-local. It is not a distributed invalidation system.
