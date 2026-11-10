// A minimal desktop-side agent for tests: dials the relay, registers, and
// forwards tunneled HTTP/WebSocket traffic to a local backend. It speaks the
// same protocol as Alma's desktop (docs/PROTOCOL.md), minus reconnects/E2E.

import { request as httpRequest } from 'node:http';
import { createHash } from 'node:crypto';
import { WebSocket } from 'ws';
import { BIN, decodeBinary, encodeBinary } from '../shared.mjs';

export const tokenHashOf = code => createHash('sha256').update(code).digest('hex');

export class FakeAgent {
    /**
     * @param {{relay: string, code: string, backendPort: number, key?: string, wopenDelayMs?: number, agentQuery?: string, proto?: number}} opts
     *   `proto` is the protocol version offered in `register` (omitted: a
     *   protocol-1 desktop that never mentions it).
     */
    constructor(opts) {
        this.opts = opts;
        this.proto = 1; // negotiated
        this.registered = null; // the relay's `registered` frame
        this.stats = { binaryIn: 0, binaryOut: 0, jsonData: 0, streamed: 0, bodyBytes: 0, acks: 0, windowWaits: 0 };
        // Protocol 3: per-stream bytes sent and not yet acknowledged, and the
        // stream waiting for acks to resume.
        this.window = Infinity;
        this.unacked = new Map();
        this.waiting = new Map();
        this.hreqs = []; // {id, stream, at}
        // While true, request body bytes are neither written nor acknowledged.
        this.holdBodies = false;
        this.held = [];
        this.http = new Map();
        this.sockets = new Map();
        this.closed = null; // {code, reason} once the relay closes us
        this.errors = [];
        this.pushReplies = [];
        this.wcloses = []; // {id, code, reason} — wclose frames from the relay
    }

    /** Resolves once registered; rejects with the relay's error / close. */
    connect() {
        const { relay, code, key } = this.opts;
        const th = tokenHashOf(code);
        return new Promise((resolve, reject) => {
            const ws = new WebSocket(`${relay.replace(/^http/, 'ws')}/agent?th=${th}${this.opts.agentQuery || ''}`, { maxPayload: 256 * 1024 * 1024 });
            this.ws = ws;
            let settled = false;
            const settle = (fn, v) => {
                if (!settled) {
                    settled = true;
                    fn(v);
                }
            };
            ws.on('open', () =>
                ws.send(JSON.stringify({ t: 'register', tokenHash: th, name: 'test-desktop', ver: 'test', key, ...(this.opts.proto ? { proto: this.opts.proto } : {}) }))
            );
            ws.on('message', (data, isBinary) => {
                if (isBinary) {
                    this.stats.binaryIn++;
                    return this.onBinary(data);
                }
                const frame = JSON.parse(data.toString());
                if (frame.t === 'registered') {
                    this.registered = frame;
                    this.proto = this.opts.proto >= 2 && frame.proto >= 2 ? Math.min(this.opts.proto, frame.proto) : 1;
                    if (this.proto >= 3) this.window = frame.window;
                    return settle(resolve);
                }
                if (frame.t === 'error') {
                    this.errors.push(frame.message);
                    return settle(reject, new Error(frame.message));
                }
                this.onFrame(frame);
            });
            ws.on('close', (c, r) => {
                this.closed = { code: c, reason: r.toString() };
                for (const s of this.sockets.values()) s.terminate();
                settle(reject, new Error(`closed ${c} ${r}`));
            });
            ws.on('error', err => settle(reject, err));
        });
    }

    send(frame) {
        if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(frame));
    }

    sendBinary(kind, id, payload) {
        if (this.ws?.readyState !== WebSocket.OPEN) return;
        this.stats.binaryOut++;
        this.ws.send(encodeBinary(kind, id, payload));
    }

    /**
     * Response body bytes, as hchunk JSON (protocol 1) or HCHUNK (2).
     * With `corrupt` (protocol 1 only) the base64 payload gets characters
     * outside the alphabet in front of it: the relay must not fall over such
     * a frame.
     */
    sendBody(id, bytes, { corrupt = false } = {}) {
        if (this.proto >= 2) return this.sendBinary(BIN.HCHUNK, id, bytes);
        this.stats.jsonData++;
        const b64 = Buffer.from(bytes).toString('base64');
        this.send({ t: 'hchunk', id, b: corrupt ? `!!!${b64}` : b64 });
    }

    sendWs(id, data, isBinary) {
        if (this.proto >= 2) return this.sendBinary(isBinary ? BIN.WBIN : BIN.WTEXT, id, data);
        this.stats.jsonData++;
        this.send(isBinary ? { t: 'wmsg', id, b: Buffer.from(data).toString('base64') } : { t: 'wmsg', id, s: data.toString() });
    }

    /** Protocol 3: count `n` bytes sent on stream `id`; pause it with `hold()` once its window is full. */
    spent(id, n, hold, release) {
        if (this.proto < 3) return;
        const unacked = (this.unacked.get(id) || 0) + n;
        this.unacked.set(id, unacked);
        if (unacked >= this.window && !this.waiting.has(id)) {
            this.stats.windowWaits++;
            hold();
            this.waiting.set(id, release);
        }
    }

    acked(id, n) {
        this.stats.acks++;
        const unacked = Math.max(0, (this.unacked.get(id) || 0) - n);
        this.unacked.set(id, unacked);
        const release = this.waiting.get(id);
        if (release && unacked < this.window) {
            this.waiting.delete(id);
            release();
        }
    }

    forget(id) {
        this.unacked.delete(id);
        this.waiting.delete(id);
    }

    onBinary(data) {
        const frame = decodeBinary(data);
        if (!frame) return;
        if (frame.kind === BIN.HBODY) {
            const req = this.http.get(frame.id);
            if (!req) return;
            const bytes = Buffer.from(frame.data);
            this.stats.bodyBytes += bytes.length;
            // Acknowledge once the backend has taken the bytes, like a real desktop.
            this.whenReleased(() => req.write(bytes, () => this.send({ t: 'hack', id: frame.id, n: bytes.length })));
            return;
        }
        if (frame.kind === BIN.WBIN || frame.kind === BIN.WTEXT) {
            const ws = this.sockets.get(frame.id);
            if (ws?.readyState === WebSocket.OPEN) ws.send(Buffer.from(frame.data), { binary: frame.kind === BIN.WBIN });
        }
    }

    whenReleased(fn) {
        if (this.holdBodies) this.held.push(fn);
        else fn();
    }

    /** Stop holding request bodies back and deliver what was held. */
    releaseBodies() {
        this.holdBodies = false;
        const held = this.held;
        this.held = [];
        for (const fn of held) fn();
    }

    onFrame(frame) {
        const { backendPort } = this.opts;
        switch (frame.t) {
            case 'hreq': {
                // A test path: end the response before ever sending a head.
                if (frame.path.startsWith('/api/no-head')) {
                    this.sendBody(frame.id, Buffer.from('no-head-body'));
                    this.send({ t: 'hend', id: frame.id });
                    break;
                }
                const req = httpRequest(
                    {
                        host: '127.0.0.1',
                        port: backendPort,
                        method: frame.method,
                        path: frame.path,
                        headers: { ...frame.headers, 'accept-encoding': 'identity' },
                    },
                    res => {
                        // A backend that asks for it gets its body sent with a
                        // base64 payload that is not valid base64.
                        const corrupt = res.headers['x-alma-corrupt-base64'] !== undefined;
                        const headers = { ...res.headers };
                        delete headers['x-alma-corrupt-base64'];
                        this.send({ t: 'hres', id: frame.id, status: res.statusCode, headers });
                        // Reading stops while anything holds it: the stream's window or the relay socket.
                        const holds = new Set();
                        const hold = why => (holds.add(why), res.pause());
                        const release = why => holds.delete(why) && !holds.size && res.resume();
                        res.on('data', chunk => {
                            this.sendBody(frame.id, chunk, { corrupt });
                            this.spent(frame.id, chunk.length, () => hold('window'), () => release('window'));
                            // Like Alma's desktop, wait for the relay socket to drain.
                            if (this.ws.bufferedAmount > 1024 * 1024 && !holds.has('socket')) {
                                hold('socket');
                                const timer = setInterval(() => {
                                    if (this.ws.bufferedAmount < 256 * 1024 || this.ws.readyState !== WebSocket.OPEN) {
                                        clearInterval(timer);
                                        release('socket');
                                    }
                                }, 10);
                            }
                        });
                        res.on('end', () => {
                            this.http.delete(frame.id);
                            this.forget(frame.id);
                            this.send({ t: 'hend', id: frame.id });
                        });
                    }
                );
                req.on('error', err => {
                    this.http.delete(frame.id);
                    this.send({ t: 'herr', id: frame.id, message: err.message });
                });
                this.http.set(frame.id, req);
                this.hreqs.push({ id: frame.id, stream: !!frame.stream, at: Date.now() });
                if (frame.stream) this.stats.streamed++;
                // A streamed body follows in HBODY frames and ends with hreqend.
                else req.end(frame.b ? Buffer.from(frame.b, 'base64') : undefined);
                break;
            }
            case 'hreqend': {
                const req = this.http.get(frame.id);
                if (req) this.whenReleased(() => req.end());
                break;
            }
            case 'ack':
                this.acked(frame.id, Number(frame.n));
                break;
            case 'hcancel':
                this.forget(frame.id);
                this.http.get(frame.id)?.destroy();
                this.http.delete(frame.id);
                break;
            case 'wopen': {
                const ws = new WebSocket(`ws://127.0.0.1:${backendPort}${frame.path}`);
                this.sockets.set(frame.id, ws);
                ws.on('open', () => setTimeout(() => this.send({ t: 'wopened', id: frame.id }), this.opts.wopenDelayMs || 0));
                ws.on('message', (data, isBinary) => {
                    this.sendWs(frame.id, data, isBinary);
                    this.spent(frame.id, data.length, () => ws.pause(), () => ws.resume());
                });
                ws.on('close', (code, reason) => {
                    this.forget(frame.id);
                    if (this.sockets.delete(frame.id)) this.send({ t: 'wclose', id: frame.id, code, reason: reason.toString() });
                });
                ws.on('error', err => {
                    if (this.sockets.delete(frame.id)) this.send({ t: 'werr', id: frame.id, message: err.message });
                });
                break;
            }
            case 'wmsg': {
                const ws = this.sockets.get(frame.id);
                if (!ws || ws.readyState !== WebSocket.OPEN) return;
                if (typeof frame.s === 'string') ws.send(frame.s);
                else ws.send(Buffer.from(frame.b, 'base64'));
                break;
            }
            case 'wclose': {
                this.wcloses.push({ id: frame.id, code: frame.code, reason: frame.reason });
                const ws = this.sockets.get(frame.id);
                this.sockets.delete(frame.id);
                ws?.close();
                break;
            }
            case 'pushed':
                this.pushReplies.push(frame);
                break;
        }
    }

    close() {
        this.ws?.close();
    }
}
