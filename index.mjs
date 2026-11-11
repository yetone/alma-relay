#!/usr/bin/env node
// Alma Relay — Node edition.
//
// A self-hostable reverse tunnel that lets the Alma mobile app reach a
// desktop Alma from anywhere:
//
//   desktop (agent)  --outbound WSS-->  relay  <--HTTPS/WSS--  phone (client)
//
// The desktop dials OUT to this server and registers with the SHA-256 hash of
// a routing token derived from its connect code. Phones talk plain
// HTTP/WebSocket to `https://relay.host/c/<token>/...`; the relay hashes the
// token from the URL, finds the matching agent, and tunnels the request over
// the agent socket. Nothing is written to disk; only hashes are kept, in memory.
//
// The wire protocol is documented in docs/PROTOCOL.md. Configuration is all
// environment variables — see README.md or .env.example.
//
// Run behind a TLS-terminating proxy (Caddy / nginx / Cloudflare) in production.

import { createServer } from 'node:http';
import { connect as http2Connect, constants as http2Constants } from 'node:http2';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { WebSocketServer, WebSocket } from 'ws';
import { apnsSignerFromEnv, apnsTopic, apnsHost, apnsRequestHeaders, clampBatch, validateNotification, resultFromResponse } from './apns.mjs';
import { fcmSenderFromEnv, deliverPush } from './fcm.mjs';
import {
    AckBatcher,
    BIN,
    BODY_SLICE,
    BODY_WINDOW,
    VERSION,
    authorize,
    bearerOf,
    decodeBinary,
    encodeBinary,
    isTokenHash,
    negotiateProtocol,
    parseClientPath,
    PROTOCOL,
    sanitizeCloseCode,
    streamWindow,
    turnCredentials,
} from './shared.mjs';

const env = process.env;
const PORT = Number(env.PORT || 8787);
const HOST = env.HOST || '0.0.0.0';
const MB = 1024 * 1024;
const mb = bytes => `${(bytes / MB).toFixed(1)} MB`;
/** A non-negative number from the environment, or the default. 0 means "no limit". */
const limit = (value, fallback) => {
    const n = Number(value);
    return value !== undefined && value !== '' && Number.isFinite(n) && n >= 0 ? n : fallback;
};
const MAX_BODY = limit(env.MAX_BODY_MB, 128) * MB || Infinity; // one request body
const MAX_BUFFERED = limit(env.MAX_BUFFERED_MB, 512) * MB; // all request bodies being buffered at once
const MAX_AGENTS = limit(env.MAX_AGENTS, 10_000);
const MAX_REQUESTS_PER_AGENT = limit(env.MAX_REQUESTS_PER_AGENT, 256); // concurrent HTTP requests
const MAX_SOCKETS_PER_AGENT = limit(env.MAX_SOCKETS_PER_AGENT, 256); // concurrent bridged WebSockets
const MAX_CONNECTIONS = limit(env.MAX_CONNECTIONS, 0); // TCP connections to this process
const STALL_MS = limit(env.STALL_TIMEOUT_S, 60) * 1000; // a phone that drains nothing for this long is cut off
// Highest agent protocol this relay speaks (docs/PROTOCOL.md). 1 turns off
// binary frames and request body streaming for every desktop; 2 turns off
// per-stream flow control.
const RELAY_PROTOCOL = negotiateProtocol(env.RELAY_PROTOCOL || PROTOCOL);
// Protocol 3: how far ahead of the relay's acks a desktop may send on one stream.
const STREAM_WINDOW = streamWindow(env.FLOW_STREAM_WINDOW_KB);
const HEAD_TIMEOUT_MS = 120_000; // waiting for the desktop to answer
const REGISTER_TIMEOUT_MS = 10_000; // an agent socket must register within this
const PING_INTERVAL_MS = 30_000;
// Frame caps. They match the Cloudflare edition (Durable Object messages are
// capped at 32 MiB), so a desktop that works with one works with the other.
const AGENT_MAX_FRAME = 32 * MB;
const CLIENT_MAX_MESSAGE = 24 * MB; // base64 makes it at most 32 MiB in a wmsg frame
const OPEN_QUEUE_MAX = 4 * MB; // phone bytes held while the desktop opens its side
// Flow control: above HIGH_WATER buffered bytes the sender is paused until
// the buffer drains below LOW_WATER.
const HIGH_WATER = (limit(env.FLOW_HIGH_WATER_MB, 8) || 8) * MB;
const LOW_WATER = Math.min((limit(env.FLOW_LOW_WATER_MB, 1) || 1) * MB, HIGH_WATER / 2);
const FLOW_TICK_MS = 25;
// While the agent socket is paused the relay can't read the desktop's pings,
// so it sends unsolicited pongs (RFC 6455 §5.5.3) on this interval: a desktop
// that is being pushed back on must not take the relay for gone and redial.
const PAUSED_PONG_MS = 5_000;
// A pause this long is logged (with its cause and, at the end, its length).
const PAUSE_LOG_MS = 2_000;

// ---------------------------------------------------------------------------
// APNs (optional). With no key the relay answers `unsupported` and the
// desktop stops asking. The key is read once at startup and never logged.
// ---------------------------------------------------------------------------
const apnsEnv = {
    ...env,
    APNS_KEY: env.APNS_KEY || (env.APNS_KEY_FILE ? readFileSync(env.APNS_KEY_FILE, 'utf8') : ''),
};
let apnsSigner = null;
try {
    apnsSigner = apnsSignerFromEnv(apnsEnv);
} catch (err) {
    console.error('[relay] APNs key misconfigured:', err instanceof Error ? err.message : err);
}
const APNS_TOPIC = apnsTopic(apnsEnv);

// FCM (optional, Android phones): the Firebase service-account JSON, inline
// or from a file. Read once at startup and never logged.
let fcmSender = null;
try {
    fcmSender = fcmSenderFromEnv({
        ...env,
        FCM_SERVICE_ACCOUNT: env.FCM_SERVICE_ACCOUNT || (env.FCM_SERVICE_ACCOUNT_FILE ? readFileSync(env.FCM_SERVICE_ACCOUNT_FILE, 'utf8') : ''),
    });
} catch (err) {
    console.error('[relay] FCM service account misconfigured:', err instanceof Error ? err.message : err);
}

/** One notification over a fresh HTTP/2 session (Node's fetch is HTTP/1.1 only). */
function apnsPostHttp2(host, path, headers, body) {
    return new Promise((resolve, reject) => {
        const session = http2Connect(`https://${host}`);
        const finish = fn => value => {
            session.close();
            fn(value);
        };
        session.on('error', finish(reject));
        const req = session.request({ ...headers, [http2Constants.HTTP2_HEADER_METHOD]: 'POST', [http2Constants.HTTP2_HEADER_PATH]: path });
        let status = 0;
        const chunks = [];
        req.on('response', h => {
            status = Number(h[http2Constants.HTTP2_HEADER_STATUS] || 0);
        });
        req.on('data', c => chunks.push(c));
        req.on(
            'end',
            finish(() => resolve({ status, body: Buffer.concat(chunks).toString('utf8') }))
        );
        req.on('error', finish(reject));
        req.setTimeout(15_000, () => req.close(http2Constants.NGHTTP2_CANCEL));
        req.end(body);
    });
}

async function sendPushBatch(notifications) {
    const results = [];
    for (const n of clampBatch(notifications)) {
        const invalid = validateNotification(n);
        if (invalid) {
            results.push({ token: typeof n?.token === 'string' ? n.token : '', status: 400, reason: invalid });
            continue;
        }
        try {
            const auth = await apnsSigner.token();
            const { status, body } = await apnsPostHttp2(
                apnsHost(n.environment),
                `/3/device/${n.token}`,
                apnsRequestHeaders(auth, APNS_TOPIC, n),
                JSON.stringify(n.payload)
            );
            results.push({ token: n.token, ...resultFromResponse(status, body) });
        } catch (err) {
            results.push({ token: n.token, status: 0, reason: err instanceof Error ? err.message : String(err) });
        }
    }
    return results;
}

// ---------------------------------------------------------------------------
// Agents
// ---------------------------------------------------------------------------

/** tokenHash -> Agent */
const agents = new Map();
/** Bytes of phone request bodies held in memory right now (see MAX_BUFFERED). */
let bufferedBodies = 0;

const sha256 = s => createHash('sha256').update(s).digest('hex');

class Agent {
    constructor(ws, tokenHash, name, proto = 1) {
        this.ws = ws;
        this.tokenHash = tokenHash;
        this.name = String(name || 'desktop').slice(0, 64);
        /** Negotiated protocol version (see shared.mjs) */
        this.proto = proto;
        this.nextId = 1;
        /** id -> {res, headSent, timer} for in-flight HTTP requests */
        this.http = new Map();
        /** id -> {client, opened, queue} for bridged WebSockets */
        this.sockets = new Map();
        /** HTTP requests from phones that are still uploading or waiting */
        this.inflight = 0;
        this.isAlive = true;
        // Flow control. Downstream (desktop -> phone): when a phone can't keep
        // up, stop reading the agent socket, so TCP pushes back on the desktop
        // (which waits for its socket to drain). Upstream (phone -> desktop):
        // when the agent socket's send buffer is full, stop reading the phones.
        /** key -> {level, kill, last, since}: phone sinks over HIGH_WATER */
        this.sinks = new Map();
        /** phone WebSockets and uploads paused until the agent socket drains */
        this.waiting = new Set();
        this.paused = false;
        /** when this pause began, the last unsolicited pong, the sink that caused it, and whether it was logged */
        this.pause = null;
        this.flowTimer = null;
        // Protocol 3: each stream is acknowledged as its bytes leave for the
        // phone, so the desktop holds back only the stream of a slow phone
        // and the agent socket is never paused for it (`congested` stays as
        // a safety net for a desktop that ignores its window).
        this.acks = proto >= 3 ? new AckBatcher((id, n) => this.send({ t: 'ack', id, n })) : null;
        /** key -> {n, since, gone, kill}: protocol-3 bytes handed to a phone sink, not yet written out */
        this.owed = new Map();
        this.stallTimer = null;
    }

    /**
     * Protocol 3: `n` bytes of stream `id` were handed to a phone sink.
     * Returns the write callback that acknowledges them to the desktop once
     * they are written out (undefined below protocol 3). A sink that writes
     * none of what it owes for STALL_MS is cut off with `kill()`.
     */
    deliver(id, key, n, gone, kill) {
        if (!this.acks) return undefined;
        let debt = this.owed.get(key);
        if (!debt) {
            debt = { n: 0, since: Date.now(), gone, kill };
            this.owed.set(key, debt);
            if (STALL_MS && !this.stallTimer) this.stallTimer = setInterval(() => this.checkStalls(), Math.min(1000, STALL_MS / 2));
        }
        debt.n += n;
        return err => {
            if (this.owed.get(key) !== debt) return;
            if (err) return void this.owed.delete(key);
            debt.n -= n;
            debt.since = Date.now();
            if (debt.n <= 0) this.owed.delete(key);
            this.acks.add(id, n);
        };
    }

    checkStalls() {
        const now = Date.now();
        for (const [key, debt] of this.owed) {
            if (debt.gone()) {
                this.owed.delete(key);
            } else if (now - debt.since > STALL_MS) {
                console.log(`[relay] agent ${this.tag}: ${key} wrote nothing for ${STALL_MS / 1000}s with ${mb(debt.n)} owed; cut off`);
                this.owed.delete(key);
                try {
                    debt.kill();
                } catch {}
            }
        }
        if (!this.owed.size) this.stopStallWatch();
    }

    stopStallWatch() {
        clearInterval(this.stallTimer);
        this.stallTimer = null;
    }

    /** A short, non-secret tag for logs. */
    get tag() {
        return this.tokenHash.slice(0, 8);
    }

    /** Keep the desktop's heartbeat satisfied while its socket isn't read. */
    keepAlive(now) {
        if (now - this.pause.ponged < PAUSED_PONG_MS || this.ws.readyState !== WebSocket.OPEN) return;
        this.pause.ponged = now;
        try {
            this.ws.pong();
        } catch {}
    }

    send(frame) {
        if (this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify(frame));
        }
    }

    /** A binary frame (protocol 2 only). */
    sendBinary(kind, id, payload) {
        if (this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(encodeBinary(kind, id, payload));
        }
    }

    /** A phone WebSocket message, in the form this desktop speaks. */
    sendWs(id, data, isBinary) {
        if (this.proto >= 2) this.sendBinary(isBinary ? BIN.WBIN : BIN.WTEXT, id, data);
        else this.send(isBinary ? { t: 'wmsg', id, b: Buffer.from(data).toString('base64') } : { t: 'wmsg', id, s: data.toString() });
    }

    id(prefix) {
        return `${prefix}${this.nextId++}`;
    }

    /**
     * A phone sink has more than HIGH_WATER bytes queued. `level()` returns
     * its buffered bytes, or -1 once it is gone; `kill()` cuts it off when it
     * drains nothing for STALL_MS.
     */
    congested(key, level, kill) {
        if (!this.sinks.has(key)) this.sinks.set(key, { level, kill, last: level(), since: Date.now() });
        if (!this.paused) {
            this.paused = true;
            this.ws.pause();
            this.pause = { since: Date.now(), ponged: 0, key, bytes: level(), logged: false };
            this.keepAlive(this.pause.since);
        }
        this.startFlow();
    }

    /** Pause a phone socket or upload while the agent socket is backed up. */
    throttle(source) {
        if (this.ws.bufferedAmount <= HIGH_WATER || this.waiting.has(source)) return;
        source.pause();
        this.waiting.add(source);
        this.startFlow();
    }

    startFlow() {
        if (!this.flowTimer) this.flowTimer = setInterval(() => this.flowTick(), FLOW_TICK_MS);
    }

    flowTick() {
        const now = Date.now();
        for (const [key, sink] of this.sinks) {
            const level = sink.level();
            if (level < LOW_WATER) {
                this.sinks.delete(key);
            } else if (level < sink.last) {
                sink.last = level;
                sink.since = now;
            } else if (STALL_MS && now - sink.since > STALL_MS) {
                console.log(`[relay] agent ${this.tag}: ${key} drained nothing for ${STALL_MS / 1000}s with ${mb(level)} queued; cut off`);
                this.sinks.delete(key);
                try {
                    sink.kill();
                } catch {}
            }
        }
        if (this.paused && !this.sinks.size) {
            this.paused = false;
            this.isAlive = true; // pongs were not read while paused
            if (this.pause.logged) console.log(`[relay] agent ${this.tag}: reading again after ${((now - this.pause.since) / 1000).toFixed(1)}s`);
            this.pause = null;
            if (this.ws.readyState === WebSocket.OPEN) this.ws.resume();
        } else if (this.paused) {
            this.keepAlive(now);
            if (!this.pause.logged && now - this.pause.since >= PAUSE_LOG_MS) {
                this.pause.logged = true;
                console.log(`[relay] agent ${this.tag}: not reading it, ${this.pause.key} has ${mb(this.pause.bytes)} queued for the phone (over ${mb(HIGH_WATER)})`);
            }
        }
        if (this.waiting.size && (this.ws.bufferedAmount < LOW_WATER || this.ws.readyState !== WebSocket.OPEN)) {
            for (const source of this.waiting) source.resume();
            this.waiting.clear();
        }
        if (!this.sinks.size && !this.waiting.size) this.stopFlow();
    }

    stopFlow() {
        clearInterval(this.flowTimer);
        this.flowTimer = null;
    }

    /** Fail in-flight requests and close every bridged phone socket. */
    destroy(reason = 'Desktop disconnected') {
        this.stopFlow();
        this.stopStallWatch();
        this.owed.clear();
        this.acks?.clear();
        this.sinks.clear();
        this.waiting.clear();
        for (const [id, entry] of this.http) {
            clearTimeout(entry.timer);
            entry.upload?.stop();
            if (!entry.headSent) {
                respondJson(entry.res, 502, { error: 'Desktop disconnected from relay' });
            } else {
                entry.res.destroy();
            }
            this.http.delete(id);
        }
        for (const [id, bridge] of this.sockets) {
            try {
                bridge.client.close(1012, reason);
            } catch {}
            this.sockets.delete(id);
        }
    }
}

/**
 * A phone's request body being streamed to a protocol-2 desktop. The relay
 * sends at most BODY_WINDOW bytes the desktop has not acknowledged (`hack`),
 * so a desktop that reads slowly slows the phone's upload down over TCP
 * instead of filling the relay's memory. The phone's request is paused while
 * anything holds it: no credit left, or a backed-up agent socket.
 */
class Upload {
    constructor(req, onStall) {
        this.req = req;
        this.onStall = onStall;
        this.credit = BODY_WINDOW;
        this.holds = new Set();
        this.done = false;
        this.stallTimer = null;
    }

    hold(reason) {
        if (this.done || this.holds.has(reason)) return;
        if (!this.holds.size) this.req.pause();
        this.holds.add(reason);
    }

    release(reason) {
        if (this.holds.delete(reason) && !this.holds.size && !this.done) this.req.resume();
    }

    // Agent.throttle / flowTick
    pause() {
        this.hold('socket');
    }

    resume() {
        this.release('socket');
    }

    spend(n) {
        this.credit -= n;
        if (this.credit > 0 || this.holds.has('credit')) return;
        this.hold('credit');
        if (STALL_MS) this.stallTimer = setTimeout(() => this.onStall(), STALL_MS);
    }

    refund(n) {
        if (!(n > 0)) return;
        this.credit = Math.min(BODY_WINDOW, this.credit + n);
        if (this.credit > 0 && this.holds.has('credit')) {
            clearTimeout(this.stallTimer);
            this.release('credit');
        }
    }

    /** Forward nothing more; whatever the phone still sends is read and dropped. */
    stop() {
        if (this.done) return;
        this.done = true;
        clearTimeout(this.stallTimer);
        this.holds.clear();
        this.req.resume();
    }
}

function respondJson(res, status, obj, extraHeaders = {}) {
    if (res.headersSent) {
        res.destroy();
        return;
    }
    res.writeHead(status, { 'content-type': 'application/json', 'access-control-allow-origin': '*', ...extraHeaders });
    res.end(JSON.stringify(obj));
}

/**
 * Refuse the rest of a request body without losing the reply: a socket torn
 * down the moment the reply is written can reach a phone still uploading as
 * EPIPE instead of the status. Discard what keeps coming for a moment, then
 * hang up.
 */
function dropAfterReply(req, res) {
    req.on('data', () => {});
    req.resume();
    const hangUp = () => setTimeout(() => req.destroy(), 1000).unref();
    if (res.writableFinished) hangUp();
    else res.once('finish', hangUp);
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

const server = createServer((req, res) => {
    const url = req.url || '';
    const pathname = url.split('?')[0];

    if (pathname === '/healthz') {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ ok: true, edition: 'node', version: VERSION, protocol: RELAY_PROTOCOL, agents: agents.size }));
        return;
    }

    if (pathname === '/') {
        res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
        res.end(`Alma relay ${VERSION} is running.\nUse this address in Alma → Settings → Remote Access → Use a self-hosted relay.\n`);
        return;
    }

    if (pathname === '/turn') {
        if (req.method !== 'POST' && req.method !== 'GET') {
            respondJson(res, 405, { error: 'Method not allowed' });
            return;
        }
        (async () => {
            const auth = await authorize(env, bearerOf(req.headers.authorization));
            if (!auth.ok) return respondJson(res, 401, { error: auth.message });
            const creds = await turnCredentials(env, auth.subject || bearerOf(req.headers.authorization));
            if (!creds) return respondJson(res, 404, { error: 'TURN not configured on this relay' });
            respondJson(res, 200, creds);
        })().catch(() => respondJson(res, 502, { error: 'TURN credential generation failed' }));
        return;
    }

    const parsed = parseClientPath(url);
    if (!parsed) {
        respondJson(res, 404, { error: 'Not found' });
        return;
    }

    const agent = agents.get(sha256(parsed.code));
    if (!agent) {
        respondJson(res, 502, { error: 'Desktop is offline or the connect code is invalid' });
        return;
    }
    if (Number(req.headers['content-length'] || 0) > MAX_BODY) {
        respondJson(res, 413, { error: 'Request body too large' });
        req.resume();
        return;
    }
    if (MAX_REQUESTS_PER_AGENT && agent.inflight >= MAX_REQUESTS_PER_AGENT) {
        respondJson(res, 503, { error: 'Too many concurrent requests to this desktop' });
        req.resume();
        return;
    }
    agent.inflight++;
    res.on('close', () => agent.inflight--);

    if (agent.proto >= 2) {
        streamRequest(agent, req, res, parsed.rest);
        return;
    }

    const chunks = [];
    let size = 0;
    let aborted = false;
    // Bytes of this body counted against MAX_BUFFERED, released once the
    // body has been handed to the agent socket or the request is dropped.
    let held = 0;
    const release = () => {
        bufferedBodies -= held;
        held = 0;
    };
    res.on('close', release);

    req.on('data', chunk => {
        if (aborted) return;
        size += chunk.length;
        if (size > MAX_BODY) {
            aborted = true;
            release();
            respondJson(res, 413, { error: 'Request body too large' }, { Connection: 'close' });
            dropAfterReply(req, res);
            return;
        }
        if (MAX_BUFFERED && bufferedBodies + chunk.length > MAX_BUFFERED) {
            aborted = true;
            release();
            respondJson(res, 503, { error: 'Relay is busy, try again shortly' }, { Connection: 'close' });
            dropAfterReply(req, res);
            return;
        }
        bufferedBodies += chunk.length;
        held += chunk.length;
        chunks.push(chunk);
    });

    req.on('end', () => {
        if (aborted) return;
        if (agents.get(agent.tokenHash) !== agent || agent.ws.readyState !== WebSocket.OPEN) {
            release();
            respondJson(res, 502, { error: 'Desktop disconnected from relay' });
            return;
        }
        const id = agent.id('h');
        const headers = tunneledHeaders(req);

        const timer = setTimeout(() => {
            const entry = agent.http.get(id);
            if (entry && !entry.headSent) {
                agent.http.delete(id);
                agent.send({ t: 'hcancel', id });
                respondJson(res, 504, { error: 'Desktop did not respond in time' });
            }
        }, HEAD_TIMEOUT_MS);

        agent.http.set(id, { res, headSent: false, timer });

        res.on('close', () => {
            if (agent.http.has(id)) {
                agent.http.delete(id);
                clearTimeout(timer);
                agent.send({ t: 'hcancel', id });
            }
        });

        const body = chunks.length ? Buffer.concat(chunks) : null;
        chunks.length = 0;
        agent.send({
            t: 'hreq',
            id,
            method: req.method,
            path: parsed.rest,
            headers,
            b: body ? body.toString('base64') : undefined,
        });
        release();
    });

    req.on('error', () => {});
});

function tunneledHeaders(req) {
    const headers = { ...req.headers };
    delete headers.host;
    delete headers.connection;
    delete headers['content-length'];
    delete headers['accept-encoding']; // the agent requests identity to keep lengths honest
    return headers;
}

/**
 * Protocol 2: the desktop gets `hreq` as soon as the head arrives and the
 * body follows in HBODY frames, so nothing is buffered here and the size of
 * a body is bounded only by MAX_BODY.
 */
function streamRequest(agent, req, res, path) {
    const hasBody = Number(req.headers['content-length'] || 0) > 0 || req.headers['transfer-encoding'] !== undefined;
    const id = agent.id('h');
    const entry = { res, headSent: false, timer: null, upload: null };
    agent.http.set(id, entry);
    const live = () => agent.http.get(id) === entry;

    const armHeadTimer = () => {
        entry.timer = setTimeout(() => {
            if (live() && !entry.headSent) fail(504, 'Desktop did not respond in time');
        }, HEAD_TIMEOUT_MS);
    };
    const fail = (status, message) => {
        if (!live()) return;
        agent.http.delete(id);
        clearTimeout(entry.timer);
        entry.upload?.stop();
        agent.send({ t: 'hcancel', id });
        if (entry.headSent) return res.destroy();
        respondJson(res, status, { error: message });
    };

    res.on('close', () => {
        if (!live()) return;
        agent.http.delete(id);
        clearTimeout(entry.timer);
        entry.upload?.stop();
        agent.send({ t: 'hcancel', id });
    });
    req.on('error', () => {});

    agent.send({ t: 'hreq', id, method: req.method, path, headers: tunneledHeaders(req), ...(hasBody ? { stream: true } : {}) });
    if (!hasBody) {
        armHeadTimer();
        req.resume();
        return;
    }

    const upload = new Upload(req, () => fail(504, 'Desktop stopped reading the request body'));
    entry.upload = upload;
    let size = 0;
    req.on('data', chunk => {
        if (upload.done) return;
        if (!live()) return upload.stop(); // the desktop already answered, or gave up
        size += chunk.length;
        if (size > MAX_BODY) {
            res.setHeader('Connection', 'close');
            fail(413, 'Request body too large');
            dropAfterReply(req, res);
            return;
        }
        for (let offset = 0; offset < chunk.length; offset += BODY_SLICE) {
            agent.sendBinary(BIN.HBODY, id, chunk.subarray(offset, offset + BODY_SLICE));
        }
        upload.spend(chunk.length);
        agent.throttle(upload);
    });
    req.on('end', () => {
        if (upload.done) return;
        upload.stop();
        if (!live()) return;
        agent.send({ t: 'hreqend', id });
        if (!entry.headSent) armHeadTimer();
    });
}

// ---------------------------------------------------------------------------
// WebSockets — one server in noServer mode for both /agent and /c/... paths
// ---------------------------------------------------------------------------

const agentWss = new WebSocketServer({ noServer: true, maxPayload: AGENT_MAX_FRAME });
const clientWss = new WebSocketServer({ noServer: true, maxPayload: CLIENT_MAX_MESSAGE });
if (MAX_CONNECTIONS) server.maxConnections = MAX_CONNECTIONS;

function refuseUpgrade(socket, status) {
    socket.write(`HTTP/1.1 ${status}\r\nconnection: close\r\ncontent-length: 0\r\n\r\n`);
    socket.destroy();
}

server.on('upgrade', (req, socket, head) => {
    const url = req.url || '';
    if (url === '/agent' || url.startsWith('/agent?')) {
        agentWss.handleUpgrade(req, socket, head, ws => handleAgent(ws));
        return;
    }
    const parsed = parseClientPath(url);
    if (parsed) {
        const agent = agents.get(sha256(parsed.code));
        if (!agent) return refuseUpgrade(socket, '502 Bad Gateway');
        if (MAX_SOCKETS_PER_AGENT && agent.sockets.size >= MAX_SOCKETS_PER_AGENT) return refuseUpgrade(socket, '503 Service Unavailable');
        clientWss.handleUpgrade(req, socket, head, ws => handleClientSocket(ws, agent, parsed.rest));
        return;
    }
    refuseUpgrade(socket, '404 Not Found');
});

function handleAgent(ws) {
    let agent = null;
    let registering = false;
    // Sockets that never register would otherwise sit here forever.
    const registerTimer = setTimeout(() => {
        if (!agent) ws.terminate();
    }, REGISTER_TIMEOUT_MS);

    ws.on('message', async (data, isBinary) => {
        if (isBinary && agent && agent.proto >= 2) {
            onBinaryFrame(agent, data);
            return;
        }
        let frame;
        try {
            frame = JSON.parse(data.toString());
        } catch {
            return;
        }

        if (frame.t === 'register') {
            if (agent || registering) return;
            registering = true;
            const auth = await authorize(env, frame.key);
            if (!auth.ok) {
                ws.send(JSON.stringify({ t: 'error', message: auth.message }));
                ws.close(auth.code, auth.reason);
                return;
            }
            if (!isTokenHash(frame.tokenHash)) {
                ws.send(JSON.stringify({ t: 'error', message: 'Invalid token hash' }));
                ws.close(4002, 'Invalid token hash');
                return;
            }
            if (ws.readyState !== WebSocket.OPEN) return;
            const existing = agents.get(frame.tokenHash);
            if (!existing && MAX_AGENTS && agents.size >= MAX_AGENTS) {
                ws.send(JSON.stringify({ t: 'error', message: 'This relay is full' }));
                ws.close(1013, 'Relay is full');
                return;
            }
            clearTimeout(registerTimer);
            if (existing) {
                existing.destroy('Desktop reconnected');
                try {
                    existing.ws.close(4000, 'Replaced by a newer connection');
                } catch {}
            }
            const proto = negotiateProtocol(frame.proto, RELAY_PROTOCOL);
            agent = new Agent(ws, frame.tokenHash, frame.name, proto);
            agents.set(frame.tokenHash, agent);
            ws.send(JSON.stringify({ t: 'registered', proto, ...(proto >= 3 ? { window: STREAM_WINDOW } : {}) }));
            console.log(`[relay] agent registered${auth.subject ? ` for ${auth.subject}` : ''}; agents=${agents.size}`);
            return;
        }

        if (!agent) return;

        switch (frame.t) {
            case 'push': {
                const id = String(frame.id || '');
                if (!apnsSigner && !fcmSender) {
                    agent.send({ t: 'pushed', id, unsupported: true });
                    return;
                }
                deliverPush(frame.notifications, { sendApns: apnsSigner ? sendPushBatch : null, fcm: fcmSender }).then(results => {
                    const failed = results.filter(r => r.status !== 200);
                    if (failed.length)
                        console.warn(
                            `[relay] push: ${failed.length}/${results.length} rejected`,
                            failed.map(r => r.reason || r.status)
                        );
                    agent.send({ t: 'pushed', id, results });
                });
                return;
            }
            case 'hres': {
                const entry = agent.http.get(frame.id);
                if (!entry) {
                    agent.send({ t: 'hcancel', id: frame.id });
                    return;
                }
                clearTimeout(entry.timer);
                entry.headSent = true;
                const headers = { ...(frame.headers || {}) };
                delete headers.connection;
                delete headers['keep-alive'];
                delete headers['transfer-encoding'];
                try {
                    entry.res.writeHead(frame.status || 200, headers);
                    entry.res.flushHeaders();
                } catch {
                    agent.http.delete(frame.id);
                    agent.send({ t: 'hcancel', id: frame.id });
                    try {
                        entry.res.destroy();
                    } catch {}
                }
                break;
            }
            case 'hchunk':
                writeResponseBody(agent, frame.id, Buffer.from(frame.b || '', 'base64'));
                break;
            case 'hack': {
                agent.http.get(frame.id)?.upload?.refund(Number(frame.n));
                break;
            }
            case 'hend': {
                const entry = agent.http.get(frame.id);
                if (!entry) return;
                agent.http.delete(frame.id);
                entry.upload?.stop(); // the desktop is done, whether or not it read the whole body
                try {
                    entry.res.end();
                } catch {}
                break;
            }
            case 'herr': {
                const entry = agent.http.get(frame.id);
                if (!entry) return;
                agent.http.delete(frame.id);
                clearTimeout(entry.timer);
                entry.upload?.stop();
                if (!entry.headSent) {
                    respondJson(entry.res, 502, { error: frame.message || 'Upstream error' });
                } else {
                    entry.res.destroy();
                }
                break;
            }
            case 'wopened': {
                const bridge = agent.sockets.get(frame.id);
                if (!bridge) {
                    agent.send({ t: 'wclose', id: frame.id, code: 1000 });
                    return;
                }
                bridge.opened = true;
                for (const queued of bridge.queue) agent.sendWs(frame.id, queued.data, queued.isBinary);
                bridge.queue = [];
                bridge.queued = 0;
                agent.throttle(bridge.client);
                break;
            }
            case 'werr': {
                const bridge = agent.sockets.get(frame.id);
                if (!bridge) return;
                agent.sockets.delete(frame.id);
                try {
                    bridge.client.close(1011, 'Desktop could not open the socket');
                } catch {}
                break;
            }
            case 'wmsg':
                if (typeof frame.s === 'string') sendToPhone(agent, frame.id, frame.s, false);
                else if (typeof frame.b === 'string') sendToPhone(agent, frame.id, Buffer.from(frame.b, 'base64'), true);
                break;
            case 'wclose': {
                const bridge = agent.sockets.get(frame.id);
                if (!bridge) return;
                agent.sockets.delete(frame.id);
                try {
                    bridge.client.close(sanitizeCloseCode(frame.code), String(frame.reason || '').slice(0, 120));
                } catch {}
                break;
            }
        }
    });

    ws.on('pong', () => {
        if (agent) agent.isAlive = true;
    });

    ws.on('close', () => {
        clearTimeout(registerTimer);
        if (agent) {
            agent.stopFlow();
            agent.stopStallWatch();
            agent.acks?.clear();
        }
        if (agent && agents.get(agent.tokenHash) === agent) {
            agents.delete(agent.tokenHash);
            agent.destroy();
            console.log(`[relay] agent disconnected; agents=${agents.size}`);
        }
    });

    ws.on('error', () => {});
}

function onBinaryFrame(agent, data) {
    const frame = decodeBinary(data);
    if (!frame) return;
    switch (frame.kind) {
        case BIN.HCHUNK:
            return writeResponseBody(agent, frame.id, frame.data);
        case BIN.WBIN:
            return sendToPhone(agent, frame.id, frame.data, true);
        case BIN.WTEXT:
            return sendToPhone(agent, frame.id, frame.data, false);
    }
}

function writeResponseBody(agent, id, bytes) {
    const entry = agent.http.get(id);
    if (!entry) return;
    const { res } = entry;
    const kill = () => {
        agent.http.delete(id);
        agent.send({ t: 'hcancel', id });
        res.destroy();
    };
    const written = agent.deliver(id, `h:${id}`, bytes.length, () => res.destroyed, kill);
    try {
        res.write(bytes, written);
    } catch {}
    if (res.writableLength > HIGH_WATER) {
        agent.congested(`h:${id}`, () => (res.destroyed ? -1 : res.writableLength), kill);
    }
}

/** `data` is a string or bytes; a text message may arrive as UTF-8 bytes. */
function sendToPhone(agent, id, data, isBinary) {
    const bridge = agent.sockets.get(id);
    if (!bridge || bridge.client.readyState !== WebSocket.OPEN) return;
    const { client } = bridge;
    const kill = () => client.terminate(); // its close handler tells the desktop
    const size = typeof data === 'string' ? Buffer.byteLength(data) : data.length;
    const sent = agent.deliver(id, `w:${id}`, size, () => client.readyState !== WebSocket.OPEN, kill);
    client.send(data, { binary: isBinary }, sent);
    if (client.bufferedAmount > HIGH_WATER) {
        agent.congested(`w:${id}`, () => (client.readyState === WebSocket.OPEN ? client.bufferedAmount : -1), kill);
    }
}

function handleClientSocket(clientWs, agent, path) {
    const id = agent.id('w');
    const bridge = { client: clientWs, opened: false, queue: [], queued: 0 };
    agent.sockets.set(id, bridge);
    agent.send({ t: 'wopen', id, path });

    clientWs.on('message', (data, isBinary) => {
        if (bridge.opened) {
            agent.sendWs(id, data, isBinary);
            agent.throttle(clientWs);
            return;
        }
        bridge.queued += data.length;
        if (bridge.queued > OPEN_QUEUE_MAX) {
            bridge.queue = [];
            clientWs.close(1009, 'Too much data before the desktop answered');
            return;
        }
        bridge.queue.push({ data, isBinary });
    });

    clientWs.on('close', (code, reason) => {
        if (agent.sockets.has(id)) {
            agent.sockets.delete(id);
            agent.send({ t: 'wclose', id, code: sanitizeCloseCode(code), reason: reason?.toString() });
        }
    });

    clientWs.on('error', () => {});
}

// Terminate dead agent connections.
const pinger = setInterval(() => {
    for (const agent of agents.values()) {
        if (agent.paused) continue; // pongs aren't read while paused; flow control has its own timeout
        if (!agent.isAlive) {
            try {
                agent.ws.terminate();
            } catch {}
            continue;
        }
        agent.isAlive = false;
        try {
            agent.ws.ping();
        } catch {}
    }
}, PING_INTERVAL_MS);

server.listen(PORT, HOST, () => {
    const gate = env.RELAY_TOKEN_SECRET ? 'signed relay tokens' : env.RELAY_REGISTRATION_KEY ? 'registration key' : 'open';
    const extras = [apnsSigner && 'APNs', fcmSender && 'FCM', env.TURN_URLS && env.TURN_SECRET && 'TURN'].filter(Boolean);
    console.log(`[relay] Alma relay ${VERSION} listening on ${HOST}:${PORT} (access: ${gate}${extras.length ? `; ${extras.join(', ')}` : ''})`);
});

// Docker sends SIGTERM; close cleanly so desktops reconnect right away.
for (const signal of ['SIGTERM', 'SIGINT']) {
    process.on(signal, () => {
        clearInterval(pinger);
        for (const agent of agents.values()) {
            agent.destroy('Relay shutting down');
            try {
                agent.ws.close(1012, 'Relay restarting');
            } catch {}
        }
        server.close(() => process.exit(0));
        setTimeout(() => process.exit(0), 3000).unref();
    });
}
