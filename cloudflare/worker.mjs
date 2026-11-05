// Alma Relay — Cloudflare Workers + Durable Objects edition.
//
// Same reverse-tunnel wire protocol as the Node edition (index.mjs); see
// docs/PROTOCOL.md for the frame reference. One Durable Object per desktop,
// named by its token hash:
//
//   desktop agent ──WSS /agent?th=<sha256>──▶ TunnelDO ◀── /c/<token>/* ── phone
//
// Both the agent socket and bridged phone sockets are server-accepted via the
// WebSocket Hibernation API, so an idle pairing costs ~nothing: the runtime
// auto-answers the agent's keepalive pings without waking the DO.
//
// Free-plan friendly: SQLite-backed DO class, no storage writes at all.

import { DurableObject } from 'cloudflare:workers';
import { apnsSignerFromEnv, apnsTopic, sendWithFetch } from '../apns.mjs';
import { fcmSenderFromEnv, deliverPush } from '../fcm.mjs';
import {
    BIN,
    BODY_SLICE,
    BODY_WINDOW,
    PROTOCOL,
    VERSION,
    authorize,
    b64decode,
    b64encode,
    bearerOf,
    decodeBinary,
    encodeBinary,
    isTokenHash,
    negotiateProtocol,
    parseClientPath,
    sanitizeCloseCode,
    sha256hex,
    turnCredentials,
} from '../shared.mjs';

const HEAD_TIMEOUT_MS = 120_000;
const MB = 1024 * 1024;
const MAX_BODY = 24 * MB; // protocol 1: keep hreq frames well under the 32 MiB DO WebSocket message cap
// Below this a body is buffered in the Worker when it may have to be offered
// to two tunnels (see legacy tunnels); above it the Worker asks first.
const SMALL_BODY = 1 * MB;

/** A non-negative number from the environment, or the default. 0 means "no limit". */
const limit = (value, fallback) => {
    const n = Number(value);
    return value !== undefined && value !== '' && Number.isFinite(n) && n >= 0 ? n : fallback;
};
// Protocol 2 streams request bodies, so they are capped by MAX_BODY_MB (and
// by your plan's request size limit: 100 MB on Free and Pro), not by DO
// message size.
const streamMaxBody = env => limit(env.MAX_BODY_MB, 100) * MB || Infinity;
const stallMs = env => limit(env.STALL_TIMEOUT_S, 60) * 1000;
const relayProtocol = env => negotiateProtocol(env.RELAY_PROTOCOL || PROTOCOL);
const COALESCE_MS = 10;
const TIMEOUT = Symbol('timeout');
const sleep = ms => new Promise(resolve => setTimeout(() => resolve(TIMEOUT), ms));

function concatBytes(parts, length) {
    const out = new Uint8Array(length);
    let at = 0;
    for (const part of parts) {
        out.set(part, at);
        at += part.byteLength;
    }
    return out;
}

const json = (status, obj) =>
    new Response(JSON.stringify(obj), { status, headers: { 'content-type': 'application/json', 'access-control-allow-origin': '*' } });

// Marks the "no desktop in this tunnel" answer, so the worker can tell it
// from a 502 the desktop itself sent (see legacy tunnels below).
const OFFLINE_HEADER = 'x-alma-tunnel-offline';
const offline = () => {
    const res = json(502, { error: 'Desktop is offline or the connect code is invalid' });
    res.headers.set(OFFLINE_HEADER, '1');
    return res;
};

export default {
    async fetch(request, env) {
        const url = new URL(request.url);

        if (url.pathname === '/healthz') {
            return json(200, { ok: true, edition: 'cloudflare', version: VERSION, protocol: relayProtocol(env) });
        }

        if (url.pathname === '/') {
            return new Response(`Alma relay ${VERSION} is running.\nUse this address in Alma → Settings → Remote Access → Use a self-hosted relay.\n`, {
                headers: { 'content-type': 'text/plain; charset=utf-8' },
            });
        }

        if (url.pathname === '/turn') {
            if (request.method !== 'POST' && request.method !== 'GET') return json(405, { error: 'Method not allowed' });
            const bearer = bearerOf(request.headers.get('authorization'));
            const auth = await authorize(env, bearer);
            if (!auth.ok) return json(401, { error: auth.message });
            const creds = await turnCredentials(env, auth.subject || bearer);
            if (!creds) return json(404, { error: 'TURN not configured on this relay' });
            return json(200, creds);
        }

        if (url.pathname === '/agent') {
            if ((request.headers.get('upgrade') || '').toLowerCase() !== 'websocket') {
                return json(426, { error: 'WebSocket upgrade required' });
            }
            const th = url.searchParams.get('th') || request.headers.get('x-alma-token-hash') || '';
            if (!isTokenHash(th)) {
                return json(400, { error: 'Missing or invalid token hash (th)' });
            }
            const fwd = new Request('https://tunnel/agent', request);
            fwd.headers.set('x-alma-tunnel-role', 'agent');
            return tunnelFor(env, th).fetch(fwd);
        }

        const parsed = parseClientPath(url.pathname);
        if (!parsed) return json(404, { error: 'Not found' });

        const th = await sha256hex(parsed.code);
        const target = `https://tunnel${parsed.rest}${url.search}`;
        const legacy = legacyTunnelFor(env, th);
        const tunnel = tunnelFor(env, th);
        if (!legacy) return tunnel.fetch(clientRequest(target, request));

        // A large or chunked body: buffering it here would undo streaming to
        // the desktop, so ask the hinted tunnel whether the desktop is there
        // and send the body only once.
        const length = Number(request.headers.get('content-length'));
        if (request.body && !(length <= SMALL_BODY)) {
            const probe = await tunnel.fetch(new Request('https://tunnel/', { headers: { 'x-alma-tunnel-role': 'probe' } }));
            return (probe.headers.has(OFFLINE_HEADER) ? legacy : tunnel).fetch(clientRequest(target, request));
        }

        // The request may go to two tunnels, so read its body once up front
        // (the tunnel buffers it anyway). A tee'd stream does not survive the
        // first tunnel turning it down.
        let init = request;
        if (request.body) {
            const body = await request.arrayBuffer();
            if (body.byteLength > MAX_BODY) return json(413, { error: 'Request body too large' });
            init = { method: request.method, headers: request.headers, body, signal: request.signal };
        }
        const response = await tunnel.fetch(clientRequest(target, init));
        if (!response.headers.has(OFFLINE_HEADER)) return response;
        return legacy.fetch(clientRequest(target, init));
    },
};

function clientRequest(target, init) {
    const fwd = new Request(target, init);
    fwd.headers.set('x-alma-tunnel-role', 'client');
    return fwd;
}

/**
 * A Durable Object stays in the region where it was first created — by
 * default near whoever dialled in first (the desktop). If your desktop sits
 * behind a proxy far from your phone, set LOCATION_HINT (e.g. "apac", "enam",
 * "weur") to pin tunnels near the phone instead.
 *
 * Because a Durable Object never moves, tunnels are named `<hint>:<hash>`:
 * a new hint gives every desktop a fresh tunnel in the new region the next
 * time it redials. Until then it is still in its old, un-hinted tunnel
 * (hibernated sockets survive a deploy), so a phone that finds no desktop in
 * the hinted tunnel tries that one too.
 */
const locationHint = env => String(env.LOCATION_HINT || '').trim().toLowerCase();

function tunnelFor(env, th) {
    const hint = locationHint(env);
    if (!hint) return env.TUNNEL.get(env.TUNNEL.idFromName(th));
    return env.TUNNEL.get(env.TUNNEL.idFromName(`${hint}:${th}`), { locationHint: hint });
}

/** The un-hinted tunnel a desktop may still be in, or null without a hint. */
function legacyTunnelFor(env, th) {
    return locationHint(env) ? env.TUNNEL.get(env.TUNNEL.idFromName(th)) : null;
}

export class TunnelDO extends DurableObject {
    constructor(ctx, env) {
        super(ctx, env);
        // In-memory only: every entry belongs to a live request/socket, and
        // the DO cannot hibernate while one is active.
        this.pending = new Map(); // id -> {enqueue, close, fail, headResolve, timer, headersSent}
        this.preOpenQueues = new Map(); // client ws id -> frames queued until agent sends wopened
        this.counter = 0;
        // APNs signer (see ../../apns.mjs); null when the deployment has no
        // APNS_KEY secret — the agent is then told `unsupported` and the
        // desktop stops trying. The provider token is cached on the instance.
        this.apns = undefined;
        // FCM sender (see ../../fcm.mjs) from the FCM_SERVICE_ACCOUNT secret;
        // null without one. Android notifications then come back FcmUnsupported.
        this.fcm = undefined;
    }

    fcmSender() {
        if (this.fcm === undefined) {
            try {
                this.fcm = fcmSenderFromEnv(this.env);
            } catch (err) {
                console.error('[relay] FCM service account misconfigured:', err instanceof Error ? err.message : err);
                this.fcm = null;
            }
        }
        return this.fcm;
    }

    apnsSigner() {
        if (this.apns === undefined) {
            try {
                this.apns = apnsSignerFromEnv(this.env);
            } catch (err) {
                console.error('[relay] APNs key misconfigured:', err instanceof Error ? err.message : err);
                this.apns = null;
            }
        }
        return this.apns;
    }

    // Agent asked us to notify its phone(s). The desktop never sees the APNs
    // key; it only learns per-token outcomes so it can forget dead tokens.
    async handlePush(frame) {
        const id = String(frame.id || '');
        const signer = this.apnsSigner();
        const fcm = this.fcmSender();
        if (!signer && !fcm) {
            this.sendToAgent({ t: 'pushed', id, unsupported: true });
            return;
        }
        const results = await deliverPush(frame.notifications, {
            sendApns: signer ? list => sendWithFetch(signer, apnsTopic(this.env), list) : null,
            fcm,
        });
        const failed = results.filter(r => r.status !== 200).length;
        if (failed)
            console.warn(
                `[relay] push: ${failed}/${results.length} rejected`,
                results.filter(r => r.status !== 200).map(r => r.reason || r.status)
            );
        this.sendToAgent({ t: 'pushed', id, results });
    }

    nextId(prefix) {
        return `${prefix}${++this.counter}-${Math.floor(Math.random() * 1e6)}`;
    }

    agentSocket() {
        const sockets = this.ctx.getWebSockets('agent');
        for (const ws of sockets) {
            if (ws.deserializeAttachment()?.registered) return ws;
        }
        return null;
    }

    sendToAgent(frame) {
        const agent = this.agentSocket();
        if (!agent) return false;
        try {
            agent.send(JSON.stringify(frame));
            return true;
        } catch {
            return false;
        }
    }

    /** Negotiated protocol of the registered agent (1 if none). */
    agentProto() {
        return this.agentSocket()?.deserializeAttachment()?.proto || 1;
    }

    /** A binary frame (protocol 2 only). */
    sendBinaryToAgent(kind, id, payload) {
        const agent = this.agentSocket();
        if (!agent) return false;
        try {
            agent.send(encodeBinary(kind, id, payload));
            return true;
        } catch {
            return false;
        }
    }

    /** A phone WebSocket message (string or ArrayBuffer), in the form the agent speaks. */
    sendWsToAgent(id, message) {
        const isBinary = typeof message !== 'string';
        if (this.agentProto() >= 2) {
            return this.sendBinaryToAgent(isBinary ? BIN.WBIN : BIN.WTEXT, id, isBinary ? new Uint8Array(message) : message);
        }
        return this.sendToAgent(isBinary ? { t: 'wmsg', id, b: b64encode(new Uint8Array(message)) } : { t: 'wmsg', id, s: message });
    }

    async fetch(request) {
        const role = request.headers.get('x-alma-tunnel-role');
        if (role === 'agent') return this.acceptAgent();
        if (role === 'probe') return this.agentSocket() ? new Response(null, { status: 204 }) : offline();
        if (role === 'client') {
            if ((request.headers.get('upgrade') || '').toLowerCase() === 'websocket') {
                return this.acceptClientSocket(request);
            }
            return this.tunnelHttp(request);
        }
        return json(400, { error: 'Bad tunnel request' });
    }

    acceptAgent() {
        const pair = new WebSocketPair();
        const [client, server] = Object.values(pair);
        this.ctx.acceptWebSocket(server, ['agent']);
        server.serializeAttachment({ role: 'agent', registered: false });
        return new Response(null, { status: 101, webSocket: client });
    }

    acceptClientSocket(request) {
        const agent = this.agentSocket();
        if (!agent) return offline();

        const url = new URL(request.url);
        const id = this.nextId('w');
        const pair = new WebSocketPair();
        const [client, server] = Object.values(pair);
        this.ctx.acceptWebSocket(server, ['client', `c:${id}`]);
        server.serializeAttachment({ role: 'client', id });
        this.preOpenQueues.set(id, []);
        this.sendToAgent({ t: 'wopen', id, path: url.pathname + url.search });
        return new Response(null, { status: 101, webSocket: client });
    }

    async tunnelHttp(request) {
        const agent = this.agentSocket();
        if (!agent) return offline();

        const url = new URL(request.url);
        const id = this.nextId('h');
        // Protocol 2 streams the body after `hreq`; protocol 1 sends it whole.
        const streaming = this.agentProto() >= 2 && request.body !== null;

        let body = null;
        if (!streaming && request.body) {
            try {
                const buf = await request.arrayBuffer();
                if (buf.byteLength > MAX_BODY) return json(413, { error: 'Request body too large' });
                body = buf.byteLength > 0 ? new Uint8Array(buf) : null;
            } catch {
                return json(400, { error: 'Failed to read request body' });
            }
        }

        const headers = {};
        for (const [key, value] of request.headers) {
            const k = key.toLowerCase();
            if (k === 'host' || k === 'connection' || k === 'content-length' || k === 'accept-encoding' || k.startsWith('x-alma-tunnel') || k.startsWith('cf-'))
                continue;
            headers[k] = value;
        }

        const entry = {
            enqueue: null,
            close: null,
            fail: null,
            headResolve: null,
            headersSent: false,
            timer: null,
            credit: BODY_WINDOW, // request body bytes the desktop has room for
            wake: null, // resolves a body pump waiting for credit
        };
        this.pending.set(id, entry);

        const head = new Promise(resolve => {
            entry.headResolve = resolve;
        });
        // The desktop has the whole request: from here it must answer in time.
        const armHeadTimer = () => {
            entry.timer = setTimeout(() => {
                if (this.pending.get(id) === entry && !entry.headersSent) {
                    this.pending.delete(id);
                    this.sendToAgent({ t: 'hcancel', id });
                    entry.headResolve({ status: 504, error: true, message: 'Desktop did not respond in time' });
                }
            }, HEAD_TIMEOUT_MS);
        };

        const self = this;
        const stream = new ReadableStream({
            start(controller) {
                entry.enqueue = chunk => {
                    try {
                        controller.enqueue(chunk);
                    } catch {
                        /* client gone */
                    }
                };
                entry.close = () => {
                    try {
                        controller.close();
                    } catch {
                        /* already closed */
                    }
                };
                entry.fail = message => {
                    try {
                        controller.error(new Error(message));
                    } catch {
                        /* already closed */
                    }
                };
            },
            cancel() {
                cancelRequest();
            },
        });
        const cancelRequest = () => {
            if (self.pending.get(id) === entry) {
                self.pending.delete(id);
                clearTimeout(entry.timer);
                self.sendToAgent({ t: 'hcancel', id });
                entry.close?.();
                entry.wake?.();
            }
        };
        // The phone hung up (needs the enable_request_signal compatibility
        // flag; the signal survives the hop from the Worker into this DO).
        // Without it a long stream would keep the desktop busy until it ends.
        request.signal?.addEventListener?.('abort', cancelRequest);

        const sent = this.sendToAgent({
            t: 'hreq',
            id,
            method: request.method,
            path: url.pathname + url.search,
            headers,
            b: body ? b64encode(body) : undefined,
            ...(streaming ? { stream: true } : {}),
        });
        if (!sent) {
            this.pending.delete(id);
            return json(502, { error: 'Desktop disconnected from relay' });
        }
        if (streaming) this.pumpBody(id, entry, request.body, armHeadTimer);
        else armHeadTimer();

        const res = await head;
        if (res.error) {
            this.pending.delete(id);
            return json(res.status, { error: res.message });
        }
        entry.headersSent = true;

        const resHeaders = new Headers();
        for (const [key, value] of Object.entries(res.headers || {})) {
            const k = key.toLowerCase();
            if (k === 'connection' || k === 'keep-alive' || k === 'transfer-encoding' || k === 'content-length') continue;
            resHeaders.append(key, value);
        }

        if (res.status === 204 || res.status === 304) {
            this.pending.delete(id);
            clearTimeout(entry.timer);
            return new Response(null, { status: res.status, headers: resHeaders });
        }
        return new Response(stream, { status: res.status, headers: resHeaders });
    }

    /**
     * Protocol 2: stream a phone's request body to the desktop, never more
     * than BODY_WINDOW bytes ahead of its `hack`s. A desktop that reads
     * slowly slows the phone's upload down instead of filling this DO.
     */
    async pumpBody(id, entry, body, armHeadTimer) {
        const live = () => this.pending.get(id) === entry;
        const maxBody = streamMaxBody(this.env);
        const reader = body.getReader();
        const giveUp = () => reader.cancel().catch(() => {});
        const abort = (status, message) => {
            giveUp();
            if (!live()) return;
            this.pending.delete(id);
            clearTimeout(entry.timer);
            this.sendToAgent({ t: 'hcancel', id });
            if (!entry.headersSent) entry.headResolve?.({ status, error: true, message });
            else entry.fail?.(message);
        };
        // Request bodies arrive in small reads (4 KiB in workerd), and a
        // WebSocket message per read is slow. Coalesce reads into slices of
        // up to BODY_SLICE; a slice is sent early when the phone pauses, so a
        // trickling body still reaches the desktop as it comes.
        const parts = [];
        let pendingBytes = 0;
        const flush = async () => {
            if (!pendingBytes) return true;
            while (entry.credit <= 0 && live()) {
                if (!(await this.creditFor(entry))) {
                    abort(504, 'Desktop stopped reading the request body');
                    return false;
                }
            }
            if (!live()) return false;
            const slice = parts.length === 1 ? parts[0] : concatBytes(parts, pendingBytes);
            parts.length = 0;
            pendingBytes = 0;
            if (!this.sendBinaryToAgent(BIN.HBODY, id, slice)) {
                abort(502, 'Desktop disconnected from relay');
                return false;
            }
            entry.credit -= slice.byteLength;
            return true;
        };
        let size = 0;
        let read = reader.read();
        try {
            for (;;) {
                const next = pendingBytes ? await Promise.race([read, sleep(COALESCE_MS)]) : await read;
                if (next === TIMEOUT) {
                    if (!(await flush())) return giveUp();
                    continue;
                }
                if (next.done) break;
                read = reader.read();
                if (!live()) return giveUp(); // the desktop already answered, or gave up
                const value = next.value;
                size += value.byteLength;
                if (size > maxBody) return abort(413, 'Request body too large');
                for (let offset = 0; offset < value.byteLength; ) {
                    const take = Math.min(value.byteLength - offset, BODY_SLICE - pendingBytes);
                    parts.push(value.subarray(offset, offset + take));
                    pendingBytes += take;
                    offset += take;
                    if (pendingBytes >= BODY_SLICE && !(await flush())) return giveUp();
                }
            }
            if (!(await flush())) return;
        } catch {
            return abort(400, 'Failed to read request body');
        }
        if (!live()) return;
        this.sendToAgent({ t: 'hreqend', id });
        if (!entry.headersSent) armHeadTimer();
    }

    /** Resolves true when the desktop acknowledges more body, false on a stall. */
    creditFor(entry) {
        const stall = stallMs(this.env);
        return new Promise(resolve => {
            const timer = stall
                ? setTimeout(() => {
                      entry.wake = null;
                      resolve(false);
                  }, stall)
                : null;
            entry.wake = () => {
                clearTimeout(timer);
                entry.wake = null;
                resolve(true);
            };
        });
    }

    onBinaryFrame(message) {
        const frame = decodeBinary(message);
        if (!frame) return;
        if (frame.kind === BIN.HCHUNK) {
            this.pending.get(frame.id)?.enqueue?.(frame.data);
            return;
        }
        if (frame.kind !== BIN.WBIN && frame.kind !== BIN.WTEXT) return;
        const client = this.clientSocket(frame.id);
        if (!client) return;
        try {
            client.send(frame.kind === BIN.WTEXT ? new TextDecoder().decode(frame.data) : frame.data);
        } catch {
            /* client gone */
        }
    }

    async webSocketMessage(ws, message) {
        const attachment = ws.deserializeAttachment() || {};

        if (attachment.role === 'client') {
            // Phone -> desktop traffic on a bridged socket.
            const queue = this.preOpenQueues.get(attachment.id);
            if (queue) queue.push(message);
            else this.sendWsToAgent(attachment.id, message);
            return;
        }

        if (attachment.role !== 'agent') return;
        if (typeof message !== 'string' && attachment.registered && attachment.proto >= 2) {
            this.onBinaryFrame(message);
            return;
        }

        let frame;
        try {
            frame = JSON.parse(typeof message === 'string' ? message : new TextDecoder().decode(message));
        } catch {
            return;
        }

        if (frame.t === 'register') {
            // Access gate: a signed relay token (RELAY_TOKEN_SECRET), a shared
            // key (RELAY_REGISTRATION_KEY), or open — see shared.mjs.
            const auth = await authorize(this.env, frame.key);
            if (!auth.ok) {
                ws.send(JSON.stringify({ t: 'error', message: auth.message }));
                ws.close(auth.code, auth.reason);
                return;
            }
            const account = auth.subject;
            // The DO is named by the token hash (after an optional
            // `<hint>:` prefix), so a mismatched register frame means the
            // agent dialled the wrong tunnel.
            if (frame.tokenHash !== String(this.ctx.id.name || '').replace(/^[a-z0-9-]+:/, '')) {
                ws.send(JSON.stringify({ t: 'error', message: 'Token hash mismatch' }));
                ws.close(4002, 'Token hash mismatch');
                return;
            }
            let replaced = false;
            for (const other of this.ctx.getWebSockets('agent')) {
                if (other !== ws) {
                    replaced = true;
                    // Unregister first: if the old socket is half-open (the
                    // desktop's heartbeat already gave up on it), its close
                    // event may arrive minutes later or never, and must not
                    // then tear down tunnels that belong to this new agent.
                    try {
                        other.serializeAttachment({ role: 'agent', registered: false, replaced: true });
                    } catch {
                        /* ignore */
                    }
                    try {
                        other.close(4000, 'Replaced by a newer connection');
                    } catch {
                        /* ignore */
                    }
                }
            }
            // Every bridged phone socket was paired with a responder on the
            // previous agent connection, which the desktop has torn down. Left
            // open, the phone keeps sending into a socket id the new agent has
            // never heard of: the edge still answers its pings, so it never
            // redials, and every request times out (Node edition: Agent.destroy).
            if (replaced) this.dropTunnels('Desktop reconnected');
            const proto = negotiateProtocol(frame.proto, relayProtocol(this.env));
            ws.serializeAttachment({ role: 'agent', registered: true, account, proto });
            ws.send(JSON.stringify({ t: 'registered', proto }));
            if (account) console.log(`[relay] agent registered for account ${account}`);
            return;
        }

        if (!attachment.registered) return;

        switch (frame.t) {
            case 'push':
                await this.handlePush(frame);
                return;
            case 'hres': {
                const entry = this.pending.get(frame.id);
                if (!entry) {
                    this.sendToAgent({ t: 'hcancel', id: frame.id });
                    return;
                }
                clearTimeout(entry.timer);
                entry.headResolve?.({ status: frame.status || 200, headers: frame.headers || {} });
                break;
            }
            case 'hchunk': {
                const entry = this.pending.get(frame.id);
                if (!entry) return;
                entry.enqueue?.(b64decode(frame.b || ''));
                break;
            }
            case 'hack': {
                const entry = this.pending.get(frame.id);
                const n = Number(frame.n);
                if (!entry || !(n > 0)) return;
                entry.credit = Math.min(BODY_WINDOW, entry.credit + n);
                if (entry.credit > 0) entry.wake?.();
                break;
            }
            case 'hend': {
                const entry = this.pending.get(frame.id);
                if (!entry) return;
                this.pending.delete(frame.id);
                entry.close?.();
                entry.wake?.(); // a body still being pumped stops
                break;
            }
            case 'herr': {
                const entry = this.pending.get(frame.id);
                if (!entry) return;
                this.pending.delete(frame.id);
                clearTimeout(entry.timer);
                entry.wake?.();
                if (!entry.headersSent) {
                    entry.headResolve?.({ status: 502, error: true, message: String(frame.message || 'Upstream error') });
                } else {
                    entry.fail?.(String(frame.message || 'Upstream error'));
                }
                break;
            }
            case 'wopened': {
                const queue = this.preOpenQueues.get(String(frame.id));
                this.preOpenQueues.delete(String(frame.id));
                for (const queued of queue || []) this.sendWsToAgent(String(frame.id), queued);
                break;
            }
            case 'werr': {
                this.preOpenQueues.delete(String(frame.id));
                this.closeClientSocket(String(frame.id), 1011, 'Desktop could not open the socket');
                break;
            }
            case 'wmsg': {
                const client = this.clientSocket(String(frame.id));
                if (!client) return;
                try {
                    if (typeof frame.s === 'string') client.send(frame.s);
                    else if (typeof frame.b === 'string') client.send(b64decode(frame.b));
                } catch {
                    /* client gone */
                }
                break;
            }
            case 'wclose': {
                this.preOpenQueues.delete(String(frame.id));
                this.closeClientSocket(String(frame.id), sanitizeCloseCode(frame.code), String(frame.reason || ''));
                break;
            }
        }
    }

    clientSocket(id) {
        return this.ctx.getWebSockets(`c:${id}`)[0] || null;
    }

    closeClientSocket(id, code, reason) {
        const client = this.clientSocket(id);
        if (!client) return;
        try {
            client.close(code, reason);
        } catch {
            /* ignore */
        }
    }

    async webSocketClose(ws, code, reason) {
        // Finish the close handshake: the runtime does not answer the peer's
        // close frame for us, and until we close our end the peer sits in
        // CLOSING until the edge drops the TCP connection (~10 s, code 1006).
        try {
            ws.close(sanitizeCloseCode(code), String(reason || '').slice(0, 120));
        } catch {
            /* already closed */
        }
        const attachment = ws.deserializeAttachment() || {};
        if (attachment.role === 'agent' && attachment.registered) {
            // Desktop went away: fail in-flight requests, drop bridged sockets.
            this.dropTunnels('Desktop disconnected');
        } else if (attachment.role === 'client') {
            this.preOpenQueues.delete(attachment.id);
            this.sendToAgent({ t: 'wclose', id: attachment.id, code: sanitizeCloseCode(code), reason: String(reason || '').slice(0, 120) });
        }
    }

    /** Fail in-flight requests and close every bridged phone socket. */
    dropTunnels(reason) {
        for (const [id, entry] of this.pending) {
            clearTimeout(entry.timer);
            entry.wake?.();
            if (!entry.headersSent) entry.headResolve?.({ status: 502, error: true, message: 'Desktop disconnected from relay' });
            else entry.fail?.('Desktop disconnected from relay');
            this.pending.delete(id);
        }
        this.preOpenQueues.clear();
        for (const client of this.ctx.getWebSockets('client')) {
            try {
                client.close(1012, reason);
            } catch {
                /* ignore */
            }
        }
    }

    async webSocketError(ws) {
        return this.webSocketClose(ws, 1011, 'WebSocket error');
    }
}
