# Perfect Datastar SSE API Study Plan

Status: working design record, 2026-07-14. This document records evidence and
decisions as they develop. It does not authorize code or consumer changes.

## Objective

Make a dependency-only upgrade of `datastar-helpers` operationally safer for
every existing consumer without changing the meaning of any existing public
interface. Put new semantics behind a new interface, then migrate consumers
deliberately with compatibility tests at every step.

This has four coupled parts:

1. Preserve the contracts already used by consumers.
2. Introduce an additive, safer SSE interface with an explicit lifecycle.
3. Standardize the Clojure dependency coordinate without confusing coordinate
   identity with source or namespace identity.
4. Build a consumer matrix and executable compatibility harness before changing
   any pins.

## Current source and dependency identities

There is one source repository and three consumer-chosen dependency coordinates:

| Coordinate | Resolution | Current consumers |
| --- | --- | --- |
| `genek/datastar-kit` | Git repository at `e7ad76c` | order-hub, vizier |
| `genek/datastar-helpers` | Same Git repository at `6021f00` or `bb8a704` | reddit server2, social-media-writer |
| `net.gaiwan/datastar-helpers` | Local root `/Users/genekim/src.local/datastar-helpers` | video-editor POC |

All provide the same Clojure namespaces from `src/datastar_kit/`. The coordinate
is the library identity used by tools.deps resolution; it does not define the
namespace. Different coordinates for the same repository are nevertheless
dangerous because tools.deps treats them as different libraries. A graph can
therefore contain two revisions of the same namespace under two library
identities.

The intended canonical coordinate is not yet decided. Coordinate unification
must be a separate migration from API evolution: first prove one source revision
works everywhere, then change one consumer coordinate at a time.

## Existing public contracts to freeze

The compatibility floor is the union of public behavior at all deployed pins,
not merely the API at repository HEAD.

### `datastar-kit.sse-sdk`

Existing consumers rely on some or all of:

- Public `subscribers`, whose value is an atom containing a set of raw SSE
  generators. Social Media Writer aliases and dereferences this atom directly.
- `subscriber-count`.
- `push!`, `push-1!`, and `push-signals!`, including asynchronous return and
  FIFO ordering through the existing push agent.
- The one-argument `(sse-response request)` form.
- The legacy two-argument `(sse-response request on-connect-fn)` form. The
  callback is invoked with the exact new generator, its exception is logged and
  swallowed, and the connection remains registered.
- The newer options-map form used by Social Media Writer:
  `:on-connect`, `:max-stream-ms`, `:flap-threshold`, and `:heartbeat?`.
- Default heartbeat enabled and default maximum stream age `nil`.

Additive functions such as `broadcast!` and `execute-script!` must also remain
once released, even if a newer interface offers better equivalents.

### `datastar-kit.sse`

The raw http-kit channel interface is a separate public product. Its
`subscribers`, `subscriber-count`, `trigger-push!`, and `sse-handler` behavior
must be assessed independently. It must not change merely to make the SDK-flavor
interface internally elegant.

### `datastar-kit.ds` and browser resources

A helper pin bump also changes expression builders, SSE string constructors,
and vendored JavaScript. Compatibility therefore includes:

- Function arities and generated JavaScript/SSE bytes.
- Hiccup attribute maps.
- Resource paths and browser-global names.
- Exact-byte browser-owned text journal behavior.

The SSE plan cannot call a release compatible until these surfaces are checked.

## Changes since the oldest deployed pin

Relevant commits after `e7ad76c` include:

- `a85fb3d`: flap detection.
- `fac8f8c`: multi-line SSE data encoding fix.
- `2c32e3e`: opt-in bounded recycle in raw SSE.
- `72aa61a`: browser-owned text helpers and runtime.
- `1910d02`: bounded recycle in SDK SSE.
- `bb8a704`: application-owned heartbeat option.
- `fa39dba`: clean-close reconnect documentation.

The SDK `sse-response` two-argument slot changed from “callback” to
“callback or map.” Valid existing callback uses remain source-compatible. This
does not justify adding further meanings to that slot. New lifecycle semantics
should receive a new identity rather than making the overload increasingly
context-sensitive.

## Preliminary adversarial findings

1. SDK write failure is not detected reliably. The Clojure SDK commonly returns
   exactly `false` after catching an I/O failure. The helper currently treats any
   non-throwing call as success. Multi-patch `doseq` also discards individual
   return values.
2. SDK bounded recycle creates one sleeping future per connection and does not
   cancel it on ordinary close. A 50-connection churn probe left 50 sleeping
   future threads.
3. Bounded recycle cannot be a default. Datastar `@get` defaults to retry mode
   `auto`, which stops after a clean EOF. Only a client using `retry: 'always'`
   can make intentional server recycling transparent.
4. The Clojure RC8 adapter already supplies event-stream/no-cache headers,
   HTTP/1.1-only keep-alive, immediate header send, a lock per generator, and
   event flushing. It does not supply `X-Accel-Buffering: no`.
5. The existing public subscriber atom prevents replacing its values with rich
   connection records. New metadata must live alongside it or behind a new API.
6. The deployed browser clients share the basic retry modes but differ in request
   ownership and visibility-reopen details. Most direct consumers serve Datastar
   Aliased RC8, while Social Media Writer serves Datastar Aliased v1.0.2. Server
   compatibility still cannot be assessed independently of the exact bundle.

## Browser client compatibility

The helper repository ships a browser resource at
`resources/public/vendor/datastar-aliased.js`. That path is itself a public API.
Some consumers have a project-local resource at the same path; others obtain it
from the helper's classpath resources. Replacing the bytes at that path can
therefore change application behavior during what appears to be a server-helper
upgrade.

### Deployed bundle identities

| Client family | SHA-256 | Where observed |
| --- | --- | --- |
| Datastar Aliased v1.0.0-RC.8 | `caf79678...97b301` | helper, vizier, reddit server2, and most local Datastar apps |
| Datastar Aliased v1.0.2 | `dfe05292...1348` | Social Media Writer |
| Datastar v1.0.0-beta.11 | `fc47d2b4...20372` | several non-direct local implementations |

The checked-in files match the corresponding official release-tag bundles
byte-for-byte. This lets the compatibility harness test named client releases
rather than treating minified files as unknown artifacts.

### RC8 `@get` behavior

Correction after Fable's adversarial review: the earlier version of this study
characterized the wrong RC8 source path. The exact deployed RC8 bundle and the
official `v1.0.0-RC.8` `library/src/plugins/actions/fetch.ts` both implement
`retry`, `requestCancellation`, success reset, and `Last-Event-ID`. The local
bundle's SHA-256 exactly matches the official tagged aliased bundle; the artifact
was not mislabeled.

- Retry modes are `auto`, `error`, `always`, and `never`.
- Normal response EOF stops under `auto` and retries under `always`.
- Network/processing errors retry with exponential backoff, starting at 1 second,
  capped at 30 seconds, and stop after the retry limit.
- A successful HTTP 200 resets the retry count and retry interval.
- The SSE wire `retry:` field changes the retry delay only.
- `Last-Event-ID` is retained across retries within the fetch action.
- GET defaults to `openWhenHidden: false`. Hiding the document aborts the
  request; making it visible explicitly opens a new request. This path does not
  depend on clean-EOF retry.
- Default request cancellation uses a weak map keyed by initiating element.
  Reinvoking the action on the same element aborts the prior request; a different
  element using the same URL owns a separate request.
- Visibility reopen reuses the request input already built for that action. It
  does not rebuild current payload the way v1.0.2 does.

Consequences:

- `retry: 'always'` is valid portable markup across the deployed RC8 and v1.0.2
  bundles. A deliberate clean close is transparent only for actions that
  actually select it; legacy actions using default `auto` still stop on clean
  EOF.
- Every persistent RC8 connection needs a per-connection full-state initializer
  because ordinary tab visibility changes close and recreate the stream.
- The new live-view API can generate `retry: 'always'` without exposing a retry
  knob. This protects ordinary clean EOF and graceful restart while the server
  remains request-bound and never deliberately retires a healthy request.
- Bundle-driven tests must quantify retry exhaustion and outage envelopes rather
  than treating “always” as infinite delivery.

### v1.0.2 `@get` behavior

v1.0.2 retains the explicit retry modes but changes request ownership and rebuild
behavior.

- `auto` retries thrown/network failures but stops after normal EOF.
- `always` also retries normal EOF.
- A successful HTTP 200 connection resets the retry count and retry interval.
- `Last-Event-ID` is retained across reconnect attempts within one fetch action.
- GET still closes while the document is hidden and recreates when visible by
  default; the reopened request rebuilds its current input/body.
- Default request cancellation is keyed by HTTP method and URL. Starting a new
  request for the same method and URL aborts the previous one, even if a
  different element initiated it. Cleanup-on-element-removal is an explicit
  mode rather than the default.

Social Media Writer's `retry: 'always'` is also supported by RC8. Its meaning
must still be proven from exact bytes for every future browser migration.

### Current upstream behavior

Current upstream retains the four retry modes, clean-EOF behavior, success
reset, `Last-Event-ID`, and visibility-driven close/reopen. Request cancellation
has evolved again: `auto` now installs cleanup through the initiating element's
runtime cleanup registry rather than using v1.0.2's global method-and-URL map.

This evolution is evidence that browser request ownership must be an explicit
compatibility dimension. The helper must not infer client capabilities merely
from a `Datastar-Request` header, which carries no client version.

### Browser resource compatibility rule

Do not replace the RC8 bytes at the existing
`/vendor/datastar-aliased.js` classpath resource as part of legacy hardening.
Either:

1. keep that resource frozen and add explicitly versioned resource paths for new
   clients; or
2. stop advertising the helper resource to new applications while retaining it
   indefinitely for legacy consumers.

A browser-client upgrade must be a named migration with its own tests. It must
not arrive incidentally through a server-helper pin bump.

## Comparison with other implementations

### Official SDK conventions

The official implementations converge on a small transport responsibility:

- The stream is bound to request cancellation or handler/stream completion.
- Headers are sent immediately and buffering is disabled where needed.
- Writes are serialized per connection and every event is flushed.
- Event encoding belongs to the SDK.
- Reconnect policy belongs to the browser action, not the server event encoder.
- No official transport imposes a hidden maximum stream age.

Go exposes request context and closed state. PHP explicitly disables Nginx
buffering and stops work on user abort. Rust delegates lifetime and cancellation
to the web framework's stream type. These are evidence for explicit ownership,
not for copying a language-specific class shape into Clojure.

### Current stable browser and latest Clojure SDK shape

As of 2026-07-14, the official browser release is Datastar `v1.0.2` (published
2026-06-02). The official Clojure SDK has not yet published a stable 1.0; its
latest Clojars artifacts are `dev.data-star.clojure/sdk` and
`dev.data-star.clojure/http-kit` `1.0.0-RC11` (published 2026-06-05).

RC11 targets the stable v1.0.2 browser in its own `CDN-url`, but its public
backend shape remains intentionally low level:

```clojure
(http-kit/->sse-response
  request
  {http-kit/on-open  (fn [sse-gen] ...)
   http-kit/on-close (fn [sse-gen status] ...)})

(d*/patch-elements! sse-gen html opts) ; -> true/false
(d*/patch-signals!  sse-gen json opts) ; -> true/false
(d*/lock-sse! sse-gen ...)
(d*/close-sse! sse-gen)
```

It owns event encoding, generator locking/closing, headers, and adapter write
profiles. RC11 also adds an `http-kit2` response path with Reitit
middleware/interceptor support so status/headers can flow through an ordinary
Ring response before `on-open` runs. It still deliberately does **not** own an
application subscriber registry, scoped hub, initial full-state render,
broadcast semantics, reconnect markup, coalescing, or backpressure policy.

The core RC8-to-RC11 generator API used by the legacy helper is largely source
compatible, but this is not permission to bump it invisibly. RC11's http-kit
artifact depends on http-kit `2.9.0-beta3`, while current consumers pin 2.8.0;
adapter close/error behavior and transitive resolution must be characterized in
the cohort harness.

Target policy:

- The new semantic API is designed against the stable v1.0.2 wire/browser
  contract and tested against its exact bundle.
- RC8 browser and Clojure SDK behavior remain a frozen compatibility floor for
  unchanged consumers.
- RC11 is the backend migration candidate today, not “stable Clojure 1.0.” Move
  to it only as a named SDK migration; if the Clojure SDK reaches stable before
  implementation, re-baseline on that release instead.
- The high-level API hides generators, so its application surface need not change
  when the adapter moves from RC8 to RC11 or a later stable release.

### Local implementation families

The local repository scan shows at least three different use cases:

1. Persistent broadcast subscriptions: application registry, full-state pushes,
   heartbeat, disconnect cleanup. Examples include video-publisher, mothership,
   and Social Media Writer.
2. Finite task streams: generate zero or more events and close when work is
   complete. Examples include dinklink, video-library, and the-conn.
3. Scoped or addressed subscriptions: registries keyed by project, seat, or
   stream purpose rather than one global subscriber set. Examples include
   mothership and marvin-voice-remote.

These should not be forced through one implicit lifecycle. A new API should make
finite versus persistent and broadcast versus addressed operation explicit.

### Consumer census

This is the local `~/src.local/**` census as of 2026-07-14. It deliberately
distinguishes dependency consumers from source copies and independent
implementations. Only the first group is made safer merely by upgrading the
`datastar-helpers` revision.

#### Direct dependency consumers

| Consumer/cohort | Declared coordinate | Source selection | Helper surface | Browser/SDK constraint |
| --- | --- | --- | --- | --- |
| `gaiwan/does/ecommerce/order-hub` | `genek/datastar-kit` | Git SHA `e7ad76c` | `datastar-kit.sse-sdk`, principally legacy callback `sse-response` and `push-1!` | Datastar Clojure SDK RC8; Aliased RC8 client |
| `gaiwan/does/vizier` | `genek/datastar-kit` | Git SHA `e7ad76c` | `datastar-kit.ds`; also hosts `order-hub` through `:local/root` | Same RC8 pins as `order-hub`; this is one dependency-resolution cohort, not two independent upgrades |
| `gaiwan/does/video-editor-2026/poc` | `net.gaiwan/datastar-helpers` | absolute `:local/root` | `datastar-kit.sse-sdk`, one-argument `sse-response`, `push-1!` | Datastar Clojure SDK RC8; persistent connection has no helper-supplied initial snapshot |
| `reddit-scraper-fulcro/server2` | `genek/datastar-helpers` | Git SHA `6021f00`, with same-symbol local override | `datastar-kit.ds` and `sse-sdk`; callback `sse-response`, `push!`, `subscriber-count` | Datastar Clojure SDK RC8; Aliased RC8 client |
| `social-media-writer` | `genek/datastar-helpers` | Git SHA `bb8a704` | `datastar-kit.ds` and `sse-sdk`; options-map `sse-response`; aliases and dereferences the public `subscribers` atom | Datastar Clojure SDK RC8, but application-owned Aliased v1.0.2 browser bundle |

This table defines the compatibility floor. In particular, the options-map
arity and the raw public subscriber atom are no longer hypothetical internals:
deployed consumers use them. They must remain available under the old identity.

#### Source copies and unresolved imports

These repositories will not follow a dependency revision automatically:

- `gaiwan/does/conference-ingest-video`, `gaiwan/does/stagesync-2026`,
  `mcp-clojure-template`, and `ppd-report-assistant` contain copied
  `datastar-kit.ds` source.
- `marvin-voice-remote` imports `datastar-kit.sse`; its default `deps.edn` basis
  does not declare `datastar-helpers`, and no matching copied namespace was found.
- `sync-zoom-slack` likewise imports `datastar-kit.sse` without a matching direct
  dependency or local source copy in its default basis.

The last two are reproducibility defects or evidence of an undocumented build
context. The migration plan must first make their source provenance explicit;
it must not silently classify them as ordinary dependency consumers.

#### Independent Datastar SSE implementations

The scan also found implementations that use Datastar but do not consume this
helper API. They are comparative evidence and potential future adopters, not
compatibility fixtures for the legacy facade:

- persistent/global or scoped hubs: `battle-cab`, `stagesync-2026`,
  `video-publisher`, `joe-payne-app`, `mothership`, and
  `marvin-voice-remote`;
- finite or task-oriented streams: `dinklink`, `video-library-2026`, and parts
  of `the-conn`;
- experimental/spike implementations under `social-media-writer` and other
  application-local namespaces.

Reference material under `claude-skills`, `kiloclaw/skills-for-buster`, and
snapshot/archive trees is not a runtime consumer and stays outside the upgrade
matrix.

#### Census artifact required by the harness

The eventual harness should replace this prose-only snapshot with a checked-in
EDN manifest containing, for every direct consumer: repository path, cohort,
coordinate, source SHA/root, namespaces, public vars and arities, SDK version,
browser bundle path/hash, stream lifecycle, reconnect policy, and test command.
The inventory job should fail when scanning discovers an unclassified import or
coordinate. That turns “all consumers” from a periodic search into a maintained
boundary.

### What the local implementations teach us

The useful conventions are accompanied by recurring failure modes. The new
interface should encode the conventions, not canonize every existing pattern.

| Implementation | Useful convention | Compatibility/safety warning |
| --- | --- | --- |
| stagesync | Registers, sends a targeted full snapshot, uses one daemon scheduler, and serializes writes | One global lock causes unrelated slow clients to block each other |
| joe-payne-app | Moves fan-out off request threads and sends heartbeats through the same ordering mechanism | Reconnect has no targeted full-state initializer; a visible-again RC8 page can remain stale until another mutation |
| video-publisher | Maintains explicit subscribe/unsubscribe and one heartbeat timer | `on-open` calls a global `push-all!`, repainting existing clients when one connects; ordinary pushes are synchronous |
| mothership monitor | Uses separate subscriber sets by stream purpose and targeted initial state | Sends the snapshot before registration, leaving a state-change race; ordinary broadcasts are synchronous |
| marvin bridge3 | Uses a registry keyed by seat and one render function for complete current state | Uses a global write lock and one sleeping heartbeat future per connection |
| dinklink/video-library | Finite request stream sends its frames and closes explicitly | This lifecycle must not inherit persistent reconnect or registry behavior |

Common invariants worth adopting:

- A new connection receives complete current state through that connection only.
- Persistent and finite streams have different constructors or policies.
- Registries are hub/application-owned and can be global, scoped, or absent.
- Every connection has serialized writes.
- Slow or dead clients do not execute socket writes on request/state-watch
  threads.
- Disconnect cleanup is idempotent.
- Heartbeat scheduling is shared, not one sleeper per connection.

Common patterns not worth preserving in a new interface:

- A single process-global subscriber atom for unrelated stream purposes.
- Global broadcast from an `on-open` callback.
- Snapshot-before-register or register-before-snapshot without an ordering
  mechanism.
- One global writer lock/queue that lets one slow client head-of-line block all
  clients.
- Treating every outbound message as equally droppable or equally durable.

### Join ordering and the initialization cut

Neither “snapshot, then register” nor “register, then synchronously write” is a
complete concurrency contract by itself. The former can miss a mutation between
the snapshot and registration. The latter can allow a normal broadcast to arrive
before the initial frame.

The new persistent interface should create an explicit ordering cut:

1. Install an initializing connection in the hub under a hub lock.
2. Capture/enqueue its full initial frame while normal publish admission is
   ordered by that same lock.
3. Mark the connection live and release publication.
4. Serialize that connection's writes in enqueue order.

If application state mutation and publish are not causally paired, no transport
library can repair the application race. The API should state that a published
frame represents authoritative state at publish time.

### State frames versus commands

Local consumers send two semantically different things:

- **State frames:** complete, idempotent projections. Newer frames supersede
  older queued frames and may be coalesced under backpressure.
- **Commands:** scripts or imperative effects whose order and occurrence matter.
  They must not be coalesced or silently dropped.

The new interface should use different operations for these. A per-connection
serial mailbox avoids cross-client head-of-line blocking; a bounded/coalescing
state slot can protect memory for slow clients. Command overflow needs an
explicit policy such as reject/close, never accidental dropping. Legacy
`push!`/`execute-script!` retain their existing global-agent behavior.

## Compatibility architecture

Use one private lifecycle engine with two public identities:

1. **Frozen legacy facade:** retain every existing namespace, var, arity,
   default, callback timing, return convention, resource path, and registry
   representation. Delegate to safer internals only where characterization tests
   prove the result observationally compatible.
2. **New semantic API:** use a new namespace and new values. Do not teach an old
   function a stricter or incompatible meaning. `open2`/`OpenEx` is the naming
   precedent, not necessarily the final Clojure name.

The private engine may be shared; the public abstractions should not be. A rich
connection record cannot replace the legacy raw `sse-gen` set, and strict result
semantics cannot leak back through legacy `nil`-returning callbacks.

## Perfect API strawman: one endpoint value, one correct path

“Impossible to use wrong” is achievable only inside a deliberately small
boundary. The API cannot prevent a caller from bypassing it and writing raw SSE,
but it can make the ordinary path contain no independent knobs whose combination
may be invalid.

The central value should own the complete bilateral contract: server handler,
browser subscription attributes, hub, initialization, rendering, lifetime, and
client capability. The consumer must not separately spell a URL, retry policy,
max age, registry, or on-connect broadcast.

Provisional Clojure shape:

```clojure
(defonce dashboard
  (feed/create
    {:id ::dashboard
     :path "/api/dashboard/events"
     :scope (fn [request] (-> request :identity :account-id))
     :render (fn [account-id] (views/dashboard account-id))
     :client feed/datastar-rc8}))

;; Server route: the endpoint supplies the Ring handler.
["/api/dashboard/events" {:get (feed/handler dashboard)}]

;; Browser view: the same endpoint supplies URL and compatible action options.
[:main (merge {:id "dashboard"} (feed/subscribe dashboard))]

;; State changed: ask the endpoint to publish authoritative state for a scope.
(feed/refresh! dashboard account-id)
```

That is the whole persistent-state interface. `handler`, `subscribe`, and
`refresh!` all require the same opaque endpoint value. Consumers cannot provide
an SSE generator, construct raw event text, choose incompatible reconnect
options, or accidentally broadcast an initializer to existing tabs.

### What `feed/create` proves at construction

- `:id`, `:path`, scope function, and render function are present and valid.
- The selected client contract and lifetime policy are compatible.
- RC8 implies request-bound lifetime with no deliberate clean retirement.
- A renewable lifetime constructor is unavailable unless its client capability
  can represent clean-EOF retry (v1.0.2 or a proven later contract).
- The endpoint owns one scoped hub and one shared daemon scheduler; it creates no
  sleeper per connection.
- Endpoint shutdown is idempotent. Construction and shutdown fit an application
  component, while `defonce` remains safe for the simple development case.

Prefer named client capability values over `:client :rc8` keywords. A capability
object can generate the exact browser attributes, validate a request contract
token/header, and be exercised against the exact vendored bundle. It must not
merely assert a version string.

### What the handler owns

- Immediate SSE headers, including `X-Accel-Buffering: no` and protocol-correct
  connection headers.
- Request-bound cancellation and one idempotent retire operation.
- An initialization cut that registers, captures/enqueues the targeted full
  snapshot, and orders later publications without a lost-update window.
- Per-connection serialized output with no global socket-write lock.
- A bounded, coalescing state slot: a newer authoritative state frame supersedes
  an unsent older state frame for the same scope.
- Heartbeat scheduling shared across the endpoint, with heartbeat writes
  serialized with that connection's state frames.
- Read-only operational statistics; no mutable subscriber collection exposed to
  consumers.

### What `refresh!` means

`refresh!` does not accept caller-rendered HTML. It schedules the endpoint's
authoritative `:render` function for the named scope and publishes the resulting
full-state frame. This removes the easiest ways to send the wrong audience, use
a partial reconnect-unsafe projection, or allow render conventions to diverge
between initialization and updates.

Rendering and application mutation still need a causal contract. The helper can
order connection admission against publication, but it cannot infer a state
change that the application never publishes. The documentation and tests must
say this plainly rather than claiming distributed correctness from a transport
API.

### Frames, effects, and finite streams

The perfect first API should optimize for server-authoritative state and omit
arbitrary script execution. State has excellent reconnect and coalescing
semantics; commands do not. If effects are later necessary, put them behind a
separate `effect` API with explicit audience, ordering, acknowledgement model,
and reject-or-close overflow. Never let an effect enter the coalescing state
slot.

Finite streams are a different product and get a different constructor, for
example:

```clojure
(stream/respond request
  (fn [send]
    (send (frame/elements result-view))
    (send (frame/signals {:done true}))))
```

`stream/respond` has no hub, heartbeat, reconnect promise, or subscription
attributes. Its completion closes the response by definition. This prevents a
task stream from accidentally acquiring persistent-feed semantics through an
option flag.

### Generated frames, not event-shaped strings

The render path should return a validated frame value. Constructors own event
encoding and preserve multiline data. A composite state frame fixes ordering
such as elements before signals. A selector or stable element ID belongs in the
frame constructor, not in hand-built SSE lines. Raw events remain available only
from the frozen low-level/legacy namespaces, making the escape hatch obvious.

### Browser/server handshake

`feed/subscribe` generates both the endpoint URL and the client-specific action.
The request should carry a generated contract identifier which `feed/handler`
checks. This detects stale HTML/new server and new HTML/stale server combinations
instead of allowing a silent retry mismatch. The identifier is a compatibility
fence, not authentication, and rolling deployments must accept an explicitly
defined overlap set.

The existing `/vendor/datastar-aliased.js` resource remains byte-for-byte frozen
for legacy consumers. New client bundles use versioned resource paths or remain
application-owned. Upgrading a helper dependency must never silently replace the
browser runtime.

### Deliberate non-features

- No process-global public subscriber atom.
- No public on-connect callback.
- No caller-provided clean-close timer in the general constructor.
- No boolean matrix for persistent/finite, global/scoped, or state/effect.
- No arbitrary `sse-gen` access in the state API.
- No global writer agent or lock shared by unrelated connections.
- No silent event dropping, and no claim that commands can be made safe through
  state coalescing.

This narrowness is the Rich Hickey move: separate values by meaning and make
composition explicit. It does not add `2` to every old function; it leaves the
old vocabulary intact and introduces a smaller vocabulary whose nouns carry the
invariants.

## Constructive negotiation: Opus round 1 and Codex response

Opus 4.8 independently read this study and the three existing helper namespaces as
an ideal consumer, not as an adversarial reviewer. Its central recommendation
converged strongly with the strawman:

- Call the persistent semantic value a **live view**.
- Put it in `datastar-kit.live`, separate finite work in
  `datastar-kit.stream`, and represent encoded output with values rather than
  raw event strings.
- Make `live/view` return one opaque value from which handler, browser
  subscription, refresh, statistics, and shutdown are derived.
- Make `refresh!` mean “render current authoritative state for this scope,” and
  coalesce redundant refresh requests.
- Keep effects out of the first state API.
- Freeze `datastar-kit.sse-sdk`, `datastar-kit.sse`, `datastar-kit.ds`, and the
  legacy browser resource.

Opus's first recommended surface was:

```clojure
(live/view opts)                   ; -> opaque Closeable LiveView
(live/handler view)                ; -> Ring handler
(live/subscribe view attrs)        ; -> Hiccup attributes
(live/refresh! view scope)
(live/refresh-all! view)
(live/stats view)
(live/stop! view)

(stream/respond request emit-fn)   ; finite work; completion closes
```

It also proposed explicit browser capability values such as `client/rc8` and
`client/aliased-1_0_2`, based on the then-recorded assumption that RC8 lacked
clean-EOF retry. The later exact-bundle review invalidated that assumption: both
support `retry: 'always'`. Capability values remain a sound representation only
if a future product has genuinely different, behavior-tested client contracts;
they are unnecessary in the first live-view surface.

### Codex agreement and simplification

The proposal still repeats two facts outside the view and therefore does not yet
meet its own “one value, no invalid combinations” test:

1. The example supplies `:path "/api/dashboard/events"` to `live/view` and then
   writes the same route path again next to `(live/handler view)`. Those strings
   can diverge.
2. The render function targets `"#dashboard"` while `live/subscribe` receives
   `{:id "dashboard"}` independently. Those strings can diverge, or a caller can
   omit/override the target ID.

A macro cannot prove uniqueness of a DOM ID at runtime and would couple the API
to source shape. The library should own both duplicated structures instead:

```clojure
(defonce dashboard
  (live/view
    {:id ::dashboard
     :path "/api/dashboard/events"
     :scope #(get-in % [:identity :account-id])
     :render #(views/dashboard (db/account-snapshot %))}))

(live/route dashboard)             ; -> [path {:get owned-handler}]
(live/region dashboard {:class "dashboard"})
                                    ; -> owned target element, stable id,
                                    ;    and compatible subscription action

(db/apply-credit! account-id amount)
(live/refresh! dashboard account-id)
```

`live/region` returns the whole persistent DOM boundary, not merely attributes.
The view generates and reserves its target ID; `:render` returns the region's
children rather than an elements frame or selector. Refresh patches only the
owned interior, so it cannot delete its own subscription or target. Optional
presentation attributes are accepted only after reserved `:id` and Datastar
action keys are rejected. One view represents one region; multiple regions get
distinct views rather than an instance-key option matrix.

`live/route` is ordinary route data and need not add a Reitit dependency. This
constructive stage retained a lower-level owned Ring handler for adapters; the
later adversarial review showed that it recreated path divergence, so the first
prototype omits it.

### Remove client/lifetime policy from the first common API

The official Go and PHP designs and all deployed browser variants share a safer
server intersection: request-bound streams with no deliberate maximum age. The
later exact-bundle correction showed that both deployed browser releases also
share `retry: 'always'`; generating that behavior improves recovery from ordinary
clean EOF without turning server lifetime into a public policy.

Therefore the first `live/view` should expose none of `:client`, `:lifetime`,
`:max-stream-ms`, or `:heartbeat-ms`. The library uses a conservative heartbeat,
never intentionally retires a healthy request, and generates one tested common
`always` subscription. This removes an entire compatibility state space instead
of representing it more carefully.

If operational evidence later proves that planned clean retirement is required,
it should be a different semantic constructor/namespace that requires a tested,
immutable browser capability. It should not become another option on
`live/view`. A general `client/from-bundle` constructor is rejected: a hash says
which bytes exist, not what reconnect semantics those bytes implement. Supported
capabilities must be closed, named, and behavior-tested.

New browser resources, if the new API distributes them, use immutable versioned
paths. The helper retains every old resource path and byte sequence. Browser
runtime migration remains separate from dependency-coordinate and server-API
migration.

### Render/publish algorithm: serialize work, not socket writes globally

Opus proposed computing the initial render under a scope hub lock. That makes
an arbitrary database/render operation part of a critical section and can block
connect/publish admission for the scope. The stronger and simpler algorithm is a
per-scope dirty loop:

1. Joining installs the connection in the scope and marks the scope dirty.
2. `refresh!` marks the same scope dirty. If no renderer is running for it, one
   is scheduled.
3. One renderer per scope clears dirty, reads/renders current committed state,
   assigns the next state generation, and offers the complete frame to every
   currently live connection in that scope.
4. If the scope became dirty while rendering, the loop renders current state
   again. Otherwise it exits.
5. Each connection serializes its own writes and retains at most the newest
   unsent state generation.

All live-view messages are complete state, so there is no special partial
“initial” message that can be overtaken. The first deliverable frame and every
later frame have identical authoritative semantics. Per-scope render
serialization prevents an older slow render from finishing after and replacing
a newer one; coalescing removes redundant intermediate work. No render or socket
write occurs under the hub lock.

This still cannot repair a state change that the application fails to follow
with `refresh!`, nor can it make a non-atomic/impure render coherent. Those are
the honest boundary.

### Answers to Opus's six questions

1. **Coalescing:** yes, always coalesce live state. An audit log should be part of
   the authoritative rendered state; an animation or every-intermediate-value
   protocol is an effect/stream. Do not add `:coalesce false` to `refresh!` or an
   effect—separate the meaning.
2. **Scope lifecycle:** a scope entry contains only live connections and in-flight
   refresh state. Eagerly evict it after the last connection and render task are
   gone. `refresh!` with no live connections is a cheap no-op and buffers
   nothing; the next join renders current truth.
3. **Contract identity:** render source/arity is the wrong fingerprint. Content
   is state and may change freely. A contract identifies wire protocol, client
   semantics, and lifetime behavior. It should be library-owned, not exposed as
   routine `:contract`/`:accept-contracts` consumer knobs. Rolling-version
   behavior needs explicit integration tests before the handshake is promised;
   a rejection that merely creates a browser retry storm is not safety.
4. **Attributes versus macro:** neither in the normal path. Return an owned region
   element. A macro cannot prove DOM uniqueness and does not eliminate the
   duplicated target fact.
5. **Effects:** defer them. Current command consumers remain on the frozen API.
   Finite progress belongs to `stream/respond`. A future effect protocol needs a
   real acknowledgement and overflow design; reserving a name is unnecessary
   and risks prematurely fixing the wrong semantics.
6. **Capabilities:** do not allow `from-bundle`. More strongly, omit capabilities
   from the request-bound first API. A later renewable API may accept only closed,
   named, behavior-tested capability values.

### Provisional constructive consensus

The destination now has two primary concepts:

- A `live/view` owns a persistent server-authoritative DOM region and all of its
  server/browser transport mechanics. Consumers use `live/route`, `live/region`,
  and `live/refresh!`; they cannot supply selectors, event bytes, connection
  callbacks, registries, lifecycle policies, or browser retry options.
- `stream/respond` owns a finite response and accepts typed frames; completion
  closes it. It has no relationship to live-view registries or reconnect.

`frame` is part of the finite/low-level vocabulary, not required to publish live
state. The initial release has no effect/command API and no renewable-lifetime
API. This is smaller than both opening proposals and preserves room to design
those meanings honestly later.

This is an Opus/Codex provisional consensus. Fable will now approach it as a
distinct smaller-model consumer, not as the author of the first proposal. Only
after Fable can understand and confirm or improve the simplified contract should
it receive the separate adversarial-review prompt.

### Smaller-model design criterion

An API is not “perfect” if its correct use depends on a frontier model silently
reconstructing the concurrency proof. Fable is therefore a usability oracle as
well as another design intelligence.

The public contract should pass a deliberately constrained test:

1. Give a smaller model only the public API reference, one canonical example,
   and a consumer's existing endpoint/view—not the helper implementation or this
   internal study.
2. Ask it to migrate representative global, scoped, and finite consumers.
3. Compile and run the generated changes against lifecycle, reconnect,
   wrong-scope, and browser-contract tests.
4. Treat every invented lifecycle knob, repeated URL/selector, raw generator
   access, or plausible-but-unsafe composition as an API-design defect before
   blaming the model.
5. Improve names, constructors, reserved-key validation, and error messages until
   the smaller model repeatedly produces the one correct shape.

This is not “design for a less intelligent programmer” as condescension. It is
the same principle as a good type or data model: correctness should be carried by
the values and vocabulary, not remembered from a long explanation. Opus can help
prove the internals; Fable should be able to use the result from the surface.

### Implications from the telephone studies

The local `llm-telephone-game` and `llm-telephone-fidelity` results change how
model agreement should be interpreted:

- Structure/checklists relayed at 1.00 fidelity across eight hops in the
  experiment, while free “improve” prose was both slower and less faithful.
- The first handoff carried most of the measured loss. Mislabeling the first
  answer's model is therefore not cosmetic; it corrupts the provenance of the
  most consequential hop.
- Smaller models were strong carriers of structured intent but weaker guardians
  of arbitrary boundaries left in prose.
- Large-owner/small-delegator preserved arbitrary boundaries better than
  small-owner/large-advisor in the pilot behavioral result. Advice did not
  substitute for final integration authority.
- The safest control is architecture or a deterministic check, not a prompt
  prohibition remembered by the next model.

Consequently, the Opus answer and Fable answer do not have the same evidentiary
meaning. Opus generated the opening design. Fable received an already structured
Opus/Codex contract and accepted it. That acceptance is useful evidence of relay
fidelity and API legibility, not an independent replication of design discovery.
Any novel Fable refinement still requires owner review and executable tests.

For this study Codex retains integration authority, Fable is a structured design
and adversarial delegate, and deterministic characterization/concurrency/browser
tests are the actual safety authority. Prompts must carry frozen non-negotiables
as a checklist; “improve the design” is not a control.

## Fable constructive legibility result

Fable's independent smaller-model pass accepted all eight Opus/Codex reductions.
It successfully recovered the intended three-namespace split and supplied useful
mechanical refinements:

- DOM IDs are deterministic functions of the qualified view ID, never per-boot
  gensyms. Existing tabs therefore reconnect to the same target after restart.
- Reserved `:id` and Datastar action attributes are rejected rather than merged.
- A view may choose a semantic region tag without gaining transport policy.
- Join atomically installs the connection and marks the scope dirty; the render
  loop clears dirty at its top and releases/rechecks its running claim in one
  transition, closing the lost-wakeup window.
- `stream/respond`'s `send!` throws on a failed/closed write so producer work stops
  instead of continuing into the legacy exact-`false` black hole.

One refinement to Fable's region description: the subscribing region itself can
be the deterministic target when the library always uses an inner patch. Its
attributes and subscription survive while its children are replaced. This avoids
an extra interior wrapper that would be invalid inside tags such as `:tbody`.
The consumer still cannot select the target or patch mode.

### Resolution of Fable's two remaining questions

1. **Observe-only contract marker: yes, but as generated URL data.** Retain one
   library-owned protocol marker for telemetry, serve on absence/mismatch, and
   never expose an acceptance knob. Put it in the generated query string rather
   than a custom header unless browser tests prove the header works identically
   across RC8, v1.0.2, CORS, and proxies. It is not authentication or enforcement.
   Its sole purpose is to make rollout skew observable without creating a retry
   storm.
2. **Finite element modes: morph-by-ID only in the first release.** Do not ship a
   selector-plus-mode options map merely because the underlying SDK has one.
   Append/prepend/remove are non-idempotent effects and deserve separately named
   values if real finite-stream migrations require them. A v1 finite stream may
   render the complete progress/log region or publish signals. This favors one
   correct semantic path over preserving every low-level optimization.

The resulting first release has no public selector or patch-mode input anywhere.
`frame/elements` is one-arity and morphs by IDs present in the returned Hiccup.
The low-level SDK remains available under its explicitly legacy identity.

### Constructive consensus surface

```clojure
;; datastar-kit.live
(live/view {:id qualified-keyword
            :path string
            :scope request->scope
            :render scope->hiccup})        ; -> opaque Closeable LiveView
(live/route view)                          ; -> [path {:get owned-handler}]
(live/handler view)                        ; adapter escape, not normal path
(live/region view)
(live/region view {:tag :section :class "dashboard"})
(live/refresh! view scope)
(live/refresh-all! view)
(live/stats view)
(live/stop! view)

;; datastar-kit.stream
(stream/respond request (fn [send!] ...))

;; datastar-kit.frame — finite stream only in v1
(frame/elements hiccup)                    ; ID-based morph only
(frame/signals signal-map)
```

Unknown view keys and reserved region attributes throw. No public value accepts
or returns a generator, event string, selector, patch mode, callback, registry,
retry policy, client identity, heartbeat, or lifetime policy. Effects and clean
retirement are absent, not merely discouraged.

## Fable adversarial review and owner disposition

Fable 5 performed the separate structured adversarial round after constructive
consensus. It preserved the ten frozen constraints, inspected exact deployed
bundle bytes and helper/consumer sources, and returned 2 blockers, 9 additional
major findings, and 11 minor findings. Its verdict was **prototype only after
corrections**: the small public surface was promising, but the evidence and
state machine were not ready to implement.

### Browser blocker: accepted, provenance subclaim rejected

Fable correctly found that this study's original RC8 behavior section was wrong.
The exact RC8 bytes contain the four retry modes, request cancellation, clean-EOF
retry under `always`, retry reset on HTTP 200, and `Last-Event-ID` handling.

Fable incorrectly inferred that the aliased files could not be official release
artifacts. Owner verification downloaded the official tagged bundles and hashed
them:

| Bundle | Local SHA-256 | Official tagged SHA-256 | Result |
| --- | --- | --- | --- |
| `v1.0.0-RC.8/bundles/datastar-aliased.js` | `caf79678...97b301` | `caf79678...97b301` | exact match |
| `v1.0.2/bundles/datastar-aliased.js` | `dfe05292...1348` | `dfe05292...1348` | exact match |

The provenance was sound; the analysis had read an obsolete/wrong source path.
The corrected browser section above now derives from the official tag source and
exact bytes. Runtime bundle tests remain mandatory because static minified/source
inspection is not a browser-behavior harness.

This correction changes an internal default without adding a public knob: a new
live region should generate `retry: 'always'`. Both deployed RC8 and v1.0.2
support it. The server remains request-bound and never intentionally retires a
healthy request, while the browser recovers from ordinary clean EOF. Exact tests
must still quantify exhaustion/outage behavior; “always” is not a delivery
guarantee.

### Ranked review disposition

| ID | Severity | Finding | Owner disposition |
| --- | --- | --- | --- |
| C-1 | blocker | Browser behavior evidence contradicted exact bytes | Accept core; correct study. Reject artifact-mislabeled subclaim after hash verification |
| B-1 | blocker | Render throw after dirty-clear causes blank client or hot loop | Accept; add bounded failure/backoff state and falsifying tests |
| B-2 | major | Join/idle-eviction ABA can orphan a connection | Accept; one CAS-owned hub state machine including eviction and stop |
| E-1 | major | Retire-on-`false` and patch-failure propagation change frozen legacy observations | Accept; new API only, never legacy hardening |
| B-3 | major | http-kit/OS buffering makes total slow-socket memory boundedness unprovable | Accept; scope claim to the helper slot and expose enqueue observations, not liveness |
| C-2 | major | Restart/deploy reconnect envelope unquantified | Accept; generate `always` and test/document exact outage envelope per bundle |
| A-1 | major | Nil/wrong/unstable scope semantics undefined | Accept with refinement: nil stops before hub/SSE; refresh returns scheduled connection count; closed stable key domain |
| D-1 | major | Finite producer success and throw are both clean EOF | Accept more strongly: defer the finite product from the first prototype |
| B-4 | major | Shared fixed render pool can reproduce cross-scope head-of-line blocking | Accept defect; reject an executor growing without bound to scope count; use bounded fair execution and narrow the isolation claim |
| E-2 | major | Per-repository tests miss merged dual-coordinate cohorts | Accept; cohort bases and duplicate namespace-root detector are mandatory |
| F-1 | major | `refresh!` is silently process-local | Accept; encode locality in constructor name, not a prose warning |
| C-3 | minor | Duplicate mount/ancestor legacy morph can duplicate or kill subscription | Accept as application/coexistence boundary plus browser fixture |
| A-2 | minor | Lossy ID sanitizer can collide and future changes strand tabs | Accept; injective frozen encoding with property/golden tests |
| D-3 | minor | ID-only finite logs can be quadratic and may not cover consumers | Accept; finite product remains deferred; census records stream shape |
| B-5 | minor | stop/join race resurrects connection | Accept; terminal state is in same CAS state machine |
| B-6 | minor | Heartbeat and state write ownership unspecified | Accept; one connection writer, coalesced/skippable heartbeat class |
| E-3 | minor | Thin consumer tests cannot prove runtime compatibility | Accept; every census row requires a helper-owned behavioral fixture |
| C-4 | minor | Protocol marker observes markup skew only and may log-storm | Accept; name it accurately and deduplicate/throttle logs |
| A-3 | minor | Region is blank until first successful frame | Accept as explicit product behavior; measure time-to-first-frame |
| A-4 | minor | Public handler escape recreates path divergence | Accept more strongly: omit it from the first public surface |
| F-2 | minor | Eight absolute claims exceeded transport/application proof | Accept; replace with scoped invariants and test references |
| A-5 | minor | `defonce` captures stale render/scope closures under reload | Accept; canonical examples pass Vars, not anonymous captured functions |

### Revised first prototype: live local state only

The adversarial round exposed that `stream/respond` does not yet meet the
“cannot use it wrong” goal: normal return and producer failure are wire-identical
clean EOF unless the application remembers a terminal frame. Rather than turn a
warning into an alleged contract, defer `datastar-kit.stream` and
`datastar-kit.frame` from the first prototype. Existing finite consumers remain
on their current SDK/raw APIs until terminal success/failure semantics are
designed and bundle-tested.

The first prototype is therefore smaller:

```clojure
;; datastar-kit.live
(live/local-view {:id qualified-keyword
                  :path string
                  :scope request->stable-scope-key
                  :render scope->hiccup}) ; -> opaque Closeable LocalView

(live/route view)                ; the only documented mounting path
(live/region view)
(live/region view attrs)         ; presentation attrs/tag; reserved keys reject
(live/refresh! view scope)       ; -> number of live connections scheduled
(live/refresh-all! view)         ; -> number of live connections scheduled
(live/stats view)                ; observations, never delivery/liveness proof
(live/stop! view)                ; idempotent; also Closeable
```

The name `local-view` makes the in-process boundary part of the vocabulary.
`refresh!` cannot notify connections on other instances. A future distributed
product needs an external invalidation source and a different semantic identity;
it must not be an option on this constructor.

The region generates a deterministic, injective, golden-frozen ID from `:id`,
uses an inner patch so its subscription attributes survive, and generates the
common `retry: 'always'` browser action. It accepts no transport attributes.
Rendered Hiccup that reproduces the owned region ID is rejected before encoding.

`nil` from `:scope` creates no hub entry and returns a non-stream terminal
response. Because the generated persistent action retries clean/error responses,
the exact status must be selected from bundle tests so an unauthorized/non-scope
page does not retry forever; `204` is the leading protocol stop response. A thrown
scope function is a server error and never registers a connection.

Scope values use a closed, immutable key domain rather than arbitrary Java
objects: keywords, strings, integers, UUIDs, or vectors composed recursively from
those values. This makes equality/hash requirements structural. The scope
function remains an application authorization boundary; the helper cannot prove
that the correct account ID was chosen.

### Required internal state-machine corrections

- One top-level atomic state owns stopped status and all scope entries. Join,
  leave, dirty, render claim/release, failure, eviction, and stop are CAS
  transitions over values from that state; no separately timed remove can orphan
  an entry.
- A render failure enters a bounded-backoff failed state. It neither clears the
  only owed render forever nor hot-loops. Join/refresh during failure records
  dirty; the next scheduled attempt reads current state. Failure count/backoff
  and time-to-first-success appear in stats.
- Rendering and socket writes never occur inside the state CAS. Per-scope render
  serialization prevents stale completion inversion. A bounded fair executor
  provides finite resource use; latency isolation is promised only while
  capacity exists, not under arbitrary blocking consumer renders.
- Each connection has one writer and at most one newest unsent state generation
  inside the helper. A heartbeat is skipped when recent state proves write
  activity and never accumulates more than one pending heartbeat.
- Total process/socket memory and actual client liveness are not provable through
  http-kit enqueue success. Stats say offered/enqueued/last-enqueue, never
  delivered/alive. OS keepalive and infrastructure limits remain deployment
  controls.
- Stop is a terminal transition before draining. Concurrent joins either install
  before the transition and are drained or observe stopped and receive the tested
  terminal response; none can resurrect afterward.

### Adversarial gate before prototyping

1. Build a SHA-keyed browser harness for official RC8, v1.0.2, and tracked
   current: retry modes, clean EOF, status behavior, exhaustion/reset, visibility,
   request cancellation, duplicate init, and `Last-Event-ID`.
2. Implement the scope/hub state machine as a pure model first and model/fuzz the
   join-render-fail-evict-stop interleavings.
3. Characterize legacy false/nil returns, public atom timing, callback ordering,
   and response/resource snapshots before sharing any engine internals.
4. Construct cohort dependency bases and reject multiple classpath roots providing
   `datastar_kit/`.
5. Require a behavioral fixture for every consumer census row, including
   live/legacy coexistence and finite stream shape.

Only after these gates pass is `local-view` ready to prototype against the
current adapter. Stable browser migration, Clojure SDK migration, canonical
coordinate migration, and consumer API adoption remain separate commits/releases.

## Candidate helper-only safety improvements

These may be eligible for the legacy facade only if compatibility tests prove
they are observationally compatible:

- Centralize only the already-existing removal/close paths in an idempotent
  private retirement operation without adding a new trigger or changing timing.
- Replace per-connection sleeping futures with one cancellable daemon scheduler.
- Add `X-Accel-Buffering: no` only as an explicitly approved additive response
  difference, snapshot-tested against caller headers/status.
- Preserve unlimited request-bound lifetime as the default.

Exactly-`false` retirement, multi-patch failure propagation, strict option
validation, new callback failure semantics, rich connection records,
backpressure/drop policies, and different return values belong only in the new
interface. Under the frozen legacy facade a false-writing generator remains in
the public atom, a multi-patch failure does not reach the caller, and an
`on-connect` callback observes its own generator already present in
`@subscribers`; characterization tests pin these uncomfortable facts.

## Coordinate unification plan

1. Choose one canonical coordinate based on repository ownership and intended
   publication, not historical spelling.
2. Ensure the canonical coordinate resolves both in CI and production. A local
   root is an override, never the published identity.
3. Detect duplicate source identities in every consumer dependency basis.
4. Test the same candidate source revision under each existing coordinate
   without editing consumers.
5. Migrate dependency coordinates separately from helper API adoption.
6. After every consumer uses the canonical coordinate, add an automated scan
   that rejects the retired coordinates.
7. Keep namespace names stable; coordinate cleanup is not a namespace rename.

## Consumer compatibility harness

The harness should live with `datastar-helpers` and treat sibling applications
as contract fixtures without modifying them.

### Static inventory

- Find dependency declarations for all known coordinate spellings.
- Find imports of `datastar-kit.ds`, `datastar-kit.sse`, and
  `datastar-kit.sse-sdk`.
- Record public vars used, arities used, and direct dereferences such as
  `sse-sdk/subscribers`.
- Record Datastar browser retry policy for every persistent endpoint.
- Record the exact browser bundle hash/version and whether the bundle comes from
  application resources or dependency classpath resources.
- Fail when a new consumer or coordinate appears without a matrix entry.

### Library contract tests

- Snapshot public vars, metadata, arglists, and resource paths at the deployed
  compatibility floor.
- Golden-test generated JS and raw SSE bytes.
- Exercise legacy callback timing, exception handling, return values, heartbeat
  defaults, and options-map behavior.
- Pin legacy behavior: exact-`false` does not retire from the public atom, `nil`
  remains success, multi-patch failure does not reach callers, and `on-connect`
  observes self-registration. Separately prove the new API retires exact-`false`
  writes.
- Churn connections and assert bounded scheduler/thread state.
- Verify HTTP/1.1 and HTTP/2 headers and flush behavior.
- Verify no default clean close.
- Exercise the persistent dirty-loop cut: joining and publishing mark the scope
  dirty; one render per scope produces complete monotonic state generations;
  a slow older render can never overwrite a newer generation, and every new
  connection's first application event is complete state.
- Exercise the bounded helper state slot, fair render-executor saturation, and
  heartbeat/state serialization. Do not claim total transport memory or
  arbitrary slow-scope latency isolation.
- Run client-contract tests against the exact RC8 and v1.0.2 release bundles,
  plus a tracked current-upstream fixture. Prove behavior for normal EOF,
  network failure, visibility changes, repeated initialization, request cleanup,
  retry exhaustion/reset, and `Last-Event-ID`.
- Assert the legacy browser resource's bytes and path do not change.

### Consumer tests without edits

- Build a tools.deps basis for each dependency cohort, overriding every exact
  existing coordinate in that merged graph with the candidate local source.
- Fail if more than one classpath root provides `datastar_kit/`.
- Compile/load every importing namespace.
- Run each consumer's relevant unit/integration tests and a mandatory
  helper-owned behavioral fixture linked to its census row.
- Render persistent subscription attributes and prove the exact RC8 and v1.0.2
  bundles honor the generated `retry: 'always'` policy.
- Quantify clean EOF, HTTP status, network failure, and retry-exhaustion envelopes
  per exact bundle rather than treating the word `always` as infinite recovery.
- Run browser tests for reconnect, two-tab isolation, and initial full-state
  delivery where the consumer already has such a contract.

The harness must distinguish “consumer does not use the new API” from “consumer
is incompatible with the upgraded dependency.” Adoption is a later step.

## Proposed release sequence

1. **Characterization release:** no semantic changes; add API/resource snapshots,
   consumer inventory, and cross-consumer override tests.
2. **Legacy hardening release:** only proven observationally compatible internal
   fixes. Every existing consumer stays unchanged and passes the harness.
3. **State-machine/browser prototype:** build only after the adversarial gates;
   no consumer adoption and no legacy delegation yet.
4. **Additive interface release:** introduce `datastar-kit.live/local-view` after
   model/fuzz/browser tests pass. Legacy remains supported and tested.
5. **Coordinate migration:** move consumers one cohort at a time to the canonical
   coordinate while holding the source revision constant.
6. **Stable browser migration:** move each application-owned/runtime resource to
   exact v1.0.2 independently, retaining immutable old resource paths.
7. **Clojure SDK migration:** characterize RC11 (or stable if then available) and
   migrate cohorts independently of browser, coordinate, and helper API.
8. **API migration:** adopt `local-view` one consumer at a time, separately from
   every dependency migration. Preserve the legacy facade until usage reaches
   zero and an explicit removal decision is made.

## Open decisions

- Canonical coordinate: the leading candidate is
  `io.github.realgenekim/datastar-helpers`, matching repository ownership and a
  conventional globally unique Git library identity. Resolution/publication
  mechanics must be proven before deciding; changing the coordinate remains
  separate from changing source or API.
- The first prototype is resolved to `datastar-kit.live/local-view` only. Finite
  `stream`/`frame` remain a separate study until terminal success/failure is
  mechanically distinguishable; they are not first-release promises.
- The raw-channel and SDK transports may share private machinery only. The new
  state API exposes neither their generators nor a common connection protocol.
- Targeted connection handles and intentional maximum age are absent from the
  first live-view API. Maximum age remains a legacy behavior until a separately
  named renewable product is justified and designed.
- How the sibling-consumer test harness behaves when a repository is absent in
  CI: pinned checkout manifest, optional integration job, or hermetic fixtures.
- Whether helper resources distribute stable v1.0.2 for new consumers or the
  browser remains application-owned. In either case RC8's existing path/bytes are
  frozen and every new distributed bundle uses an immutable versioned path.
- Whether implementation begins on Clojure RC11 after characterization or waits
  for a stable Clojure SDK 1.0. The public local-view surface must hide that
  choice.
