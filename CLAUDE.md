This is the easypubsub library's own source repository, distributed as a haxelib library (see `haxelib.json`).

`easypubsub` is a publish/subscribe engine built over WebSockets. `PubSubEngine` owns the WebSocket connection (with reconnection/backoff timing) and dispatches incoming messages to typed channels/events declared via `IChannel`/`IEvent` (generated with the help of `macros.ChannelMacro`); `Subscription` represents a client's subscription to a channel, unserializing incoming payloads through `jsonmodel`. All source lives under `src/easypubsub` (`easypubsub` package and its subpackages).

ANY AMBIGUITY OR MANUAL GAP SURFACING DURING IMPLEMENTATION SHOULD NOT BE RESOLVED SILENTLY. Instead, explicitly ask the question.

If told to take a dubious or possibly suboptimal approach (whether from the UI/UX or technical standpoint), also ask a question, providing details on why you're uncertain about this and what are the better practices or better ways to solve the problem.

# Code style conventions

See `code_style.md`.

# Dependencies

- `morestd` - small, universal utilities (e.g. timers, `StringSetMap`, `DateTime`, `Never`).
- `jsonmodel` - JSON unserialization of channel/event payloads.
- `hxWebSockets` - the underlying `hx.ws` WebSocket implementation.

See `haxelib.json`'s `dependencies` for the authoritative list.
