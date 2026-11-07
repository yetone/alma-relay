# Alma Relay

English | [简体中文](README.zh-CN.md)

Your own relay for **[Alma](https://alma.now) Remote Access**: control the Alma
on your desktop from the Alma app on your phone, from anywhere, with no port
forwarding, no VPN, and no third party in between.

```
desktop Alma ──outbound WSS──▶  your relay  ◀──HTTPS/WSS── Alma on your phone
```

The desktop dials **out** to the relay and keeps the connection open. The phone
sends its requests to the relay, which passes them through that connection to
the desktop: HTTP, streaming (SSE) and WebSockets. With end-to-end encryption on
(the Alma default), the relay only ever sees ciphertext. It stores nothing on
disk and never sees your connect code.

Alma ships with a hosted relay (`relay.alma.now`). Run your own if you want the
traffic on infrastructure you control, a region closer to you, or no
dependency on anyone else's server.

## Deploy

Pick one. Each takes a few minutes.

### Option A: Cloudflare Workers (free, no server)

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/yetone/alma-relay)

Or from a clone:

```bash
git clone https://github.com/yetone/alma-relay && cd alma-relay
npm install
npx wrangler deploy        # prints https://alma-relay.<you>.workers.dev
```

This runs on the Workers **Free plan**, using SQLite-backed Durable Objects with
WebSocket hibernation, so an idle pairing costs next to nothing. TLS and a
`*.workers.dev` hostname come with it. To use your own domain, uncomment
`routes` in `wrangler.jsonc`.

### Option B: Docker with automatic HTTPS (any VPS)

You need a server with ports 80 and 443 open and a DNS record pointing at it.

```bash
git clone https://github.com/yetone/alma-relay && cd alma-relay
cp .env.example .env       # set RELAY_DOMAIN=relay.example.com
docker compose up -d
```

[Caddy](https://caddyserver.com) obtains and renews the Let's Encrypt
certificate on its own. Your relay is at `https://relay.example.com`.

### Option C: Behind your own reverse proxy

```bash
docker run -d --restart unless-stopped -p 127.0.0.1:8787:8787 ghcr.io/yetone/alma-relay
# or, without Docker (Node ≥ 20):
npx github:yetone/alma-relay
```

Then proxy your HTTPS hostname to `127.0.0.1:8787`. WebSockets must be allowed
and responses must not be buffered:

<details>
<summary>nginx</summary>

```nginx
location / {
    proxy_pass http://127.0.0.1:8787;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_buffering off;
    proxy_read_timeout 1h;
    client_max_body_size 128m;
}
```
</details>

<details>
<summary>Caddy</summary>

```
relay.example.com {
    reverse_proxy 127.0.0.1:8787 {
        flush_interval -1
    }
}
```
</details>

Check it with `curl https://relay.example.com/healthz`.

## Connect Alma

1. On the desktop, open **Settings → Remote Access**.
2. Click **Use a self-hosted relay**, enter your relay's `https://` address,
   and click **Use this relay**.
3. If your relay requires a key (see below), paste it under **Alma account →
   Paste a token manually**.
4. Scan the pairing QR code with the Alma app on your phone.

The phone doesn't need any setup: the pairing link carries the relay address.
Phones paired before you switched relays need to scan again.

## Configuration

Set everything through environment variables: in `.env` for Docker, or with
`wrangler secret put NAME` (secrets) or `vars` in `wrangler.jsonc` (plain
settings) on Cloudflare. All of them are optional.

| Variable | Meaning |
| --- | --- |
| `RELAY_REGISTRATION_KEY` | A shared key desktops must present before they can register. Paste it into Alma as the token. |
| `RELAY_TOKEN_SECRET` | Use per-user signed tokens instead of a shared key. Useful when you run one relay for a team (see below). Takes precedence over `RELAY_REGISTRATION_KEY`. |
| `RELAY_TOKEN_MESSAGE` | What a desktop without a valid relay token is told, e.g. how to get one (default: `A valid relay token is required`). |
| `PORT` / `HOST` | Listen address (Node: `8787` / `0.0.0.0`). |
| `MAX_BODY_MB` | Request-body cap in MB (Node: `128`. Cloudflare: `100` for desktops on protocol 2, a fixed 24 for older ones). |
| `MAX_BUFFERED_MB` | Node: cap on all request bodies held in memory at once, in MB (`512`); over it, `503`. Bodies to protocol-2 desktops are streamed and don't count. |
| `MAX_AGENTS` | Node: most desktops at once (`10000`). |
| `MAX_REQUESTS_PER_AGENT` / `MAX_SOCKETS_PER_AGENT` | Node: concurrent HTTP requests / WebSockets per desktop (`256` each). |
| `MAX_CONNECTIONS` | Node: TCP connection cap for the process (unset: no cap). |
| `STALL_TIMEOUT_S` | Cut off a phone that reads nothing for this many seconds (Node), or a protocol-2 desktop that takes in none of an upload (both editions) (`60`). See [flow control](docs/PROTOCOL.md#flow-control-node-edition). |
| `RELAY_PROTOCOL` | Set to `1` to stop offering [protocol 2](docs/PROTOCOL.md#protocol-2) (streamed uploads, binary frames) to desktops. Default `2`. |
| `LOCATION_HINT` | Cloudflare only: where tunnels live, for example `apac`, `enam` or `weur`. By default a tunnel is created near the desktop. Set this when the desktop sits behind a proxy far away from the phone. Changing it moves each desktop to a new tunnel the next time it redials; phones find desktops that haven't yet. |
| `TURN_URLS` / `TURN_SECRET` / `TURN_TTL` | Serve TURN credentials for the phone's live view of the desktop browser (WebRTC). `TURN_PROVIDER=coturn` (the default). |
| `TURN_PROVIDER` | `coturn` (default) or `cloudflare` — which TURN service `/turn` mints credentials from (see below). |
| `TURN_KEY_ID` / `TURN_KEY_API_TOKEN` | A Cloudflare TURN key (dashboard → Calls), needed when `TURN_PROVIDER=cloudflare`. |
| `TURN_API_URL` | Override the Cloudflare credentials API base. For tests; leave it unset otherwise. |
| `APNS_KEY_FILE` or `APNS_KEY`, plus `APNS_KEY_ID`, `APNS_TEAM_ID`, `APNS_TOPIC` | Phone notifications (see below). |
| `FCM_SERVICE_ACCOUNT_FILE` or `FCM_SERVICE_ACCOUNT` | Android phone notifications through Firebase (see below). |

### Who can use your relay

With no key set, anyone who knows your relay's address can open tunnels on
it. They still can't reach *your* desktop: that takes its 256-bit connect
code. For a personal relay this is usually fine. Set `RELAY_REGISTRATION_KEY`
to keep strangers from using your server at all.

To share one relay with a team and be able to cut off individual people, set
`RELAY_TOKEN_SECRET` and mint a token for each person:

```bash
RELAY_TOKEN_SECRET=… npm run mint-token -- alice 365     # user id, days valid
# Docker: docker compose exec relay node scripts/mint-token.mjs alice
```

Tokens are standard HS256 JWTs (`aud: "alma-relay"`, `sub`, `exp`), so any JWT
library can mint them too. Rotate the secret to revoke them all.

### Phone notifications

The iOS app can't keep a connection open while the phone is locked. "Reply
finished" and "Alma has a question" therefore arrive through Apple Push, which
the relay sends on the desktop's behalf. Apple only accepts these from the key
of the team that signs the app. **A self-hosted relay therefore can't notify
the official Alma iOS app.** Everything else works. If you build the iOS app
yourself, give the relay your own key:

```bash
APNS_KEY_FILE=/path/AuthKey_XXXX.p8 APNS_KEY_ID=XXXX APNS_TEAM_ID=YYYY APNS_TOPIC=your.bundle.id
# Cloudflare: wrangler secret put APNS_KEY < AuthKey_XXXX.p8  (and APNS_KEY_ID, APNS_TEAM_ID)
```

Android phones are notified through Firebase Cloud Messaging instead. Give the
relay the Firebase project's service-account key (Firebase console → Project
settings → Service accounts → Generate new private key):

```bash
FCM_SERVICE_ACCOUNT_FILE=/path/service-account.json
# Cloudflare: wrangler secret put FCM_SERVICE_ACCOUNT < service-account.json
```

Like APNs, Firebase only delivers to the app of the project that key belongs
to. A relay with only one of the two keys answers the other kind of phone as
`ApnsUnsupported` / `FcmUnsupported`.

Without either key, the relay reports `unsupported` and the desktop stops asking.

### TURN for the live browser view

The phone can show the desktop's browser live over WebRTC. On most networks
that needs a TURN server. The Docker setup includes coturn:

```bash
# in .env:
TURN_SECRET=<a long random string>
TURN_URLS=stun:relay.example.com:3478,turn:relay.example.com:3478?transport=udp,turn:relay.example.com:3478?transport=tcp

docker compose --profile turn up -d
```

Open UDP/TCP `3478` and UDP `49160-49200` in your firewall. On Cloudflare, run
coturn somewhere else and set `TURN_URLS` / `TURN_SECRET` on the Worker. The
relay only issues short-lived credentials. Media never passes through it.

#### Cloudflare's TURN

If you would rather not run coturn, the relay can mint credentials from
Cloudflare's managed TURN instead (`turn.cloudflare.com`, anycast — free
alongside the Realtime SFU, otherwise billed per GB outbound). Cloudflare does
not speak coturn's static-auth-secret scheme: a TURN key cannot be used with
`turn.cloudflare.com` directly, only through their credentials API, so a
Cloudflare relay points at the API:

1. Create a TURN key in the dashboard (Calls → TURN keys) and keep the
   **Token ID** and **API token** server-side.
2. Set on the Worker:

   ```bash
   TURN_PROVIDER=cloudflare
   TURN_KEY_ID=<Token ID>
   TURN_KEY_API_TOKEN=<API token>
   ```

The relay mints a short-lived credential per request, labels it with a
pseudonymous id for Cloudflare's analytics, and answers `/turn` with the
`iceServers` from the API plus the ttl it asked for. A failed mint is a `502`,
not the `404` an unconfigured relay gives. The Node edition speaks this
provider too, with the same variables.

## Security

- **End-to-end encryption.** With E2E on (the default for new pairings), the
  phone and desktop run an authenticated ECDH handshake over the tunnel and
  derive an AES-256-GCM session. The connect code is part of the key
  derivation, so whoever runs the relay can't intercept or read the traffic.
  Sessions have forward secrecy.
- **Use Alma 0.4.329 or later.** From that version on, a desktop with E2E on
  refuses every plaintext request, so whoever runs the relay can't call the
  desktop's API directly. Earlier versions still answered plaintext requests
  sent by the relay itself.
- **Nothing to steal.** The relay routes by `sha256(routing token)`, and the
  routing token is a one-way derivation of the code. Codes, tokens and
  traffic are never written to disk or logs.
- **Always serve it over HTTPS.** E2E protects the content either way, but TLS
  also hides the routing metadata.
- The connect code is a password for your desktop. Regenerate it in
  Remote Access settings to revoke every paired phone.

## Development

```bash
npm install
npm start                    # Node edition on :8787
npx wrangler dev             # Cloudflare edition, locally

npm test                     # end-to-end suite against the Node edition
npm run test:cloudflare      # the same suite against `wrangler dev`
node test/e2e.mjs https://relay.example.com   # smoke-test a deployment
```

| File | |
| --- | --- |
| `index.mjs` | Node edition (the `ws` package is its only dependency) |
| `cloudflare/worker.mjs` | Cloudflare Workers + Durable Objects edition |
| `shared.mjs`, `apns.mjs` | Code both editions use |
| `docs/PROTOCOL.md` | The wire protocol, if you want to write your own relay or agent |

## License

[MIT](LICENSE)
