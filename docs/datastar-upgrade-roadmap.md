# Datastar upgrade roadmap

Status: characterized, intentionally deferred from the first SMW migration.

## Current versions

As of 2026-07-15:

- Datastar browser: v1.0.2, the current release and already vendored by SMW.
- Datastar Clojure SDK: RC11 upstream; SMW production remains on RC8 for the
  first `datastar-live` migration.

Official sources:

- [Datastar v1.0.2 release](https://github.com/starfederation/datastar/releases/tag/v1.0.2)
- [Datastar Clojure RC11 tag](https://github.com/starfederation/datastar-clojure/tree/v1.0.0-RC11)
- [Clojure SDK changelog](https://github.com/starfederation/datastar-clojure/blob/main/CHANGELOG.md)

## Why RC11 is deferred

The new library and the SDK upgrade both touch SSE lifecycle behavior. RC10
changed close/error propagation, including exceptions raised while closing IO
resources or running `on-close`. Combining that with a new hub would make a
disconnect regression difficult to attribute.

No RC9-RC11 feature is required for the initial migration:

- RC9 adds DELETE support to `get-signals` and updates dependencies.
- RC10 changes closing behavior, makes Brotli explicitly UTF-8, and updates
  dependencies.
- RC11 adds `view-transition-selector` and updates CDN constants for browser
  v1.0.2.

The browser release is already current, so there is no browser upgrade in this
roadmap.

## Sequence

1. Run the complete `datastar-live` suite under RC8 and RC11.
2. Migrate SMW from `datastar-helpers` to a pinned `datastar-live` SHA while
   holding RC8, http-kit 2.8.0, and the browser bundle constant.
3. Prove production reconnect, multi-tab isolation, failed-write reaping,
   hot-reload, and browser-owned editor safety.
4. Soak the RC8 migration before changing dependencies.
5. In one dependency-only commit, change the Clojure SDK and Brotli artifacts
   from RC8 to RC11.
6. Re-run the same gates, with extra assertions for close exceptions,
   idempotent shutdown, `on-close` cardinality, and http-kit 2.8.0 compatibility.
7. Keep the RC8 commit deployable until the RC11 soak completes.

## Upgrade implications

- Public patch APIs used by SMW are source-compatible from RC8 through RC11.
- RC10 can surface close failures differently; code must not treat cleanup
  exceptions as successful delivery or allow them to skip registry retirement.
- The RC11 http-kit adapter declares http-kit 2.9.0-beta3, while SMW currently
  pins 2.8.0. The SDK upgrade must prove that override rather than silently
  changing the web server in the same commit.
- Brotli's UTF-8 correction is desirable but not relevant to SMW's current
  uncompressed SSE writer path.
- `view-transition-selector` is additive and unused by the first live-view API.

The upgrade proceeds only if the dependency-only diff passes. If it does not,
SMW remains on RC8 while the failing adapter behavior is isolated upstream.
