# Wire protocol

Both editions speak exactly this protocol. It is all the relay knows; anything
above it (Alma's API, end-to-end encryption) rides inside it opaquely.

```
desktop (agent)  ──WSS /agent?th=<tokenHash>──▶  relay  ◀── HTTPS/WSS /c/<token>/… ── phone (client)
```

## Addressing

- The desktop holds a 256-bit random **connect code**, generated locally.
- With end-to-end encryption on (the default), the phone addresses the desktop
  by a **routing token**, a one-way derivation of the code. With E2E off,
  the token is the code itself.
- With E2E on, the desktop serves **only** `wopen` for `/__alma_e2e__`. It
  answers any plaintext `hreq` with `403` and any other `wopen` with `werr`,
  because only the relay itself could have sent them (Alma ≥ 0.4.329).
- The desktop registers with `tokenHash = sha256_hex(token)`. The relay keeps
  only that hash, in memory, and finds the desktop for `/c/<token>/…` by
  hashing the token from the URL. Tokens are `[A-Za-z0-9_-]{16,128}`.
- `?th=<tokenHash>` on the agent URL lets path-routed relays (the Cloudflare
  edition) pick the tunnel before the register frame arrives.

## Agent socket

JSON text frames. In protocol 1, bodies and binary messages are base64 (`b`);
text WebSocket messages travel as `s`. Protocol 2 (below) moves payloads into
binary frames and streams request bodies.

| Direction | Frame | Meaning |
| --- | --- | --- |
| agent → relay | `{t:'register', tokenHash, name?, ver?, key?, proto?}` | First frame. `key` is the registration key or relay token, if the relay wants one. `proto` is the newest protocol the desktop speaks. |
| relay → agent | `{t:'registered', proto?, window?}` / `{t:'error', message}` | `proto` is the protocol for this socket; `window` comes with protocol 3. Then close codes: `4002` bad hash, `4003` bad key, `4401` bad relay token. |
| relay → agent | close `4000` | Another agent registered the same hash. Don't redial in a loop. |
| relay → agent | `{t:'hreq', id, method, path, headers, b?}` | A phone made an HTTP request. `path` includes the query. |
| agent → relay | `{t:'hres', id, status, headers}` | Response head. Send it promptly: the relay gives up after 120 s. |
| agent → relay | `{t:'hchunk', id, b}` | Response body chunk. Streamed to the phone as it arrives (SSE works). |
| agent → relay | `{t:'hend', id}` / `{t:'herr', id, message}` | Done, or failed. |
| relay → agent | `{t:'hcancel', id}` | The phone went away; stop working on `id`. |
| relay → agent | `{t:'wopen', id, path}` | A phone opened a WebSocket. |
| agent → relay | `{t:'wopened', id}` / `{t:'werr', id, message}` | The relay queues the phone's messages until `wopened`. |
| both | `{t:'wmsg', id, s}` / `{t:'wmsg', id, b}` | Text / binary message. |
| both | `{t:'wclose', id, code?, reason?}` | Close one bridged socket. |
| agent → relay | `{t:'push', id, notifications:[{token, environment, platform?, collapseId?, pushType?, priority?, payload}]}` | Ask the relay to deliver notifications. `platform: 'android'` goes to FCM (`payload: {data:{...}}`, a data-only message); anything else goes to APNs. `pushType: 'liveactivity'` sends a Live Activity update (to `<topic>.push-type.liveactivity`); `priority: 5` sends it at low priority. |
| relay → agent | `{t:'pushed', id, results:[{token, status, reason?}]}` / `{t:'pushed', id, unsupported:true}` | Per-token outcome in frame order, or "this relay has neither an APNs nor an FCM key". A notification for a platform whose key the relay lacks comes back `{status: 0, reason: 'ApnsUnsupported' | 'FcmUnsupported'}`. |

In protocol 2 the desktop may also send `hchunk` and `wmsg` as binary frames,
and the relay sends phone WebSocket messages that way; see below.

When the agent socket closes, the relay fails in-flight requests with `502`
and closes every bridged phone socket with `1012`, so phones redial instead of
talking into a dead tunnel.

## Protocol 2

Relay ≥ 1.2.0. Negotiated per agent socket, so every pairing keeps working:

| Desktop | Relay | Result |
| --- | --- | --- |
| offers nothing (older Alma) | any | protocol 1 |
| offers `proto: 2` | < 1.2.0: replies `{t:'registered'}` | protocol 1 (no `proto` in the reply means 1) |
| offers `proto: 2` | ≥ 1.2.0 | protocol 2 |
| offers `proto: 3` | 1.2.x / 1.3.x | protocol 2: the relay answers with the newest it has |
| offers `proto: 3` | ≥ 1.4.0 | protocol 3 (below) |

`RELAY_PROTOCOL=1` makes a relay answer `proto: 1` to everyone, a kill switch
if protocol 2 misbehaves; `RELAY_PROTOCOL=2` does the same for protocol 3. `/healthz` reports the newest protocol the relay speaks.

### Binary frames

```
[kind: u8][idLen: u8][id: idLen bytes, UTF-8][payload: rest of the frame]
```

| kind | Direction | Meaning |
| --- | --- | --- |
| `1` HCHUNK | agent → relay | Response body bytes for `id` (replaces `{t:'hchunk', b}`) |
| `2` HBODY | relay → agent | Request body bytes for `id` |
| `3` WBIN | both | Binary WebSocket message on `id` |
| `4` WTEXT | both | Text WebSocket message on `id`, UTF-8 payload |

The JSON forms stay valid in protocol 2; a relay accepts either.

### Streamed request bodies

A request with a body arrives as `{t:'hreq', …, stream: true}` with no `b`, as
soon as the relay has the head. Then:

| Direction | Frame | Meaning |
| --- | --- | --- |
| relay → agent | HBODY frames | The body, in pieces of at most 256 KiB |
| relay → agent | `{t:'hreqend', id}` | The body is complete |
| agent → relay | `{t:'hack', id, n}` | The desktop consumed `n` more body bytes |

The relay keeps at most 4 MiB per request un-acknowledged, so a desktop that
reads slowly slows the phone's upload down instead of filling the relay. A
desktop that acknowledges nothing for `STALL_TIMEOUT_S` while the relay waits
gets `hcancel` and the phone `504`. The 120 s wait for the response head starts
at `hreqend`. The desktop may answer before the body ends (`hres`/`hend`); the
relay then discards the rest. A body over the limit gets `413` and `hcancel`.

On Cloudflare the edge takes in a whole request body before a Worker runs, so
there the desktop sees the request only once the upload is done. Protocol 2
still lifts the 24 MB cap, keeps bodies out of Durable Object memory, and
paces them to the desktop.

## Protocol 3

Relay ≥ 1.4.0. Everything in protocol 2, plus per-stream credit for what the
desktop sends: response bodies (`HCHUNK`) and WebSocket messages (`WBIN`,
`WTEXT`).

| Direction | Frame | Meaning |
| --- | --- | --- |
| relay → agent | `{t:'registered', proto: 3, window}` | `window`: bytes of one stream the desktop may have un-acknowledged (default 1 MiB, `FLOW_STREAM_WINDOW_KB`, at least 64 KiB) |
| relay → agent | `{t:'ack', id, n}` | `n` more payload bytes of stream `id` are on their way to the phone |

The desktop counts the payload bytes it sends on each stream (the bytes after
the binary frame header; for a JSON `wmsg`, the decoded bytes) and sends more on
a stream only while fewer than `window` of them are un-acknowledged. A message
never has to be split: one may always go out while the count is under the
window, even if it then goes over. A stream's count is forgotten when it ends.

The Node edition acknowledges bytes once they are written to the phone's
socket. The Cloudflare edition acknowledges response bytes once the phone's
response has taken them from the Durable Object, and WebSocket messages as soon
as they are sent (Workers report no WebSocket backpressure). Acks are batched:
at once from 64 KiB, otherwise within a few milliseconds.

So a phone that stops reading holds back only its own stream. Before protocol
3 the Node edition had to stop reading the whole agent socket for it (below),
stalling every other phone and tunnel on that desktop; and the Cloudflare
edition had to hold whatever the desktop sent in Durable Object memory.

## HTTP endpoints

| Endpoint | Purpose |
| --- | --- |
| `GET /healthz` | `{ok, edition, version, protocol}` (+ `agents` on the Node edition) |
| `GET /agent` (WebSocket) | Desktop agents register here |
| `ANY /c/<token>/…` | Tunneled HTTP / WebSocket traffic to one desktop |
| `POST /turn` | Short-lived TURN credentials (`Authorization: Bearer <key or relay token>` when the relay is gated); `404` if TURN is not configured |

## Limits

| | Node | Cloudflare |
| --- | --- | --- |
| Request body | 128 MB (`MAX_BODY_MB`) | protocol 1: 24 MB (Durable Object WebSocket messages are capped at 32 MiB); protocol 2: 100 MB (`MAX_BODY_MB`; also your plan's upload limit) |
| Waiting for the response head | 120 s | 120 s |
| Agent frame | 32 MiB | 32 MiB |
| Phone WebSocket message | 24 MiB | 24 MiB |
| Phone bytes held before `wopened` | 4 MiB (then close `1009`) | 4 MiB (then close `1009`) |
| Request bodies buffered at once (protocol 1) | 512 MB (`MAX_BUFFERED_MB`, then `503`) | per Durable Object |
| Un-acknowledged request body (protocol 2) | 4 MiB per request | 4 MiB per request |
| Un-acknowledged response / WebSocket bytes (protocol 3) | 1 MiB per stream (`FLOW_STREAM_WINDOW_KB`) | 1 MiB per stream (`FLOW_STREAM_WINDOW_KB`) |
| Desktops | 10 000 (`MAX_AGENTS`, then close `1013`) | — |
| Concurrent requests / sockets per desktop | 256 / 256 (`MAX_REQUESTS_PER_AGENT`, `MAX_SOCKETS_PER_AGENT`, then `503`) | — |
| Registering | within 10 s of connecting | — |

## Flow control (Node edition)

The relay never buffers a whole stream. When a phone reads slower than the
desktop sends, the relay stops reading the agent socket once that phone has
8 MiB queued (`FLOW_HIGH_WATER_MB`), which pushes back on the desktop over
TCP; it resumes below 1 MiB (`FLOW_LOW_WATER_MB`). While it isn't reading, the
relay can't see the desktop's pings, so it sends the desktop an unsolicited
pong every 5 s: a desktop must count any pong (or any frame) as the relay being
alive, not only the answer to its own ping. A pause over 2 s is logged. A phone that drains nothing for `STALL_TIMEOUT_S` (60 s) is cut off
(`hcancel`, or the bridged socket is terminated). The other way round, phone
sockets are paused while the agent socket has more than 8 MiB unsent.

With a protocol-3 desktop this pause is only a safety net: the desktop stops
at its window on the slow phone's stream, so no phone gets near 8 MiB queued,
and a phone that writes out none of its stream for `STALL_TIMEOUT_S` is cut
off the same way.

Streamed request bodies (protocol 2) are paced per request by `hack` instead,
on both editions.

The pause covers the whole agent socket, so one stuck phone briefly holds up
the desktop's other streams until it drains or is cut off. Desktops should
wait for their own socket to drain before sending more (Alma does).
