#!/usr/bin/env node
// End-to-end tests for both relay editions.
//
//   fake backend (http+ws)  <--  FakeAgent (desktop)  -->  relay  <--  this script (phone)
//
//   node test/e2e.mjs                          # Node edition (spawns index.mjs)
//   node test/e2e.mjs --cloudflare             # Cloudflare edition (spawns `wrangler dev --local`)
//   node test/e2e.mjs https://relay.example.com  # smoke-test a deployed, open relay
//   node test/e2e.mjs https://relay.example.com --protocol2 --stall-timeout=3 --max-body-mb=48
//       # ...plus the protocol-2 suite, for a deployment with those settings
//
// Exit status is the number of failed checks (0 = all pass).

import { spawn } from 'node:child_process';
import { createServer, get as httpGet } from 'node:http';
import { createHash, createHmac, randomBytes } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { WebSocketServer, WebSocket } from 'ws';
import { FakeAgent } from './fake-agent.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const ARGS = process.argv.slice(2);
const EXTERNAL = ARGS.find(a => !a.startsWith('--'))?.replace(/\/+$/, '') || null;
const EDITION = EXTERNAL ? 'external' : ARGS.includes('--cloudflare') ? 'cloudflare' : 'node';
const SLACK = EDITION === 'node' ? 1 : 4; // timing tolerance multiplier
const BACKEND_PORT = 23000 + Math.floor(Math.random() * 500);
let nextRelayPort = 18800 + Math.floor(Math.random() * 500);

const argValue = name => ARGS.find(a => a.startsWith(`--${name}=`))?.split('=')[1];
// wrangler dev's local workerd drops a request whose body is left half-read
// (a paused or abandoned upload), which deployed Workers handle. Those checks
// run against a deployment instead: --protocol2 with the settings it has.
const HALF_READ_OK = EDITION !== 'cloudflare';
const skip = (name, why) => console.log(`  - ${name} (skipped: ${why})`);
const WRANGLER_DEV = 'wrangler dev drops half-read request bodies; run --protocol2 against a deployment';

let failures = 0;
const check = (name, ok, detail = '') => {
    console.log(`${ok ? '  ✓' : '  ✗'} ${name}${ok || !detail ? '' : ` — ${detail}`}`);
    if (!ok) failures++;
};
const newCode = () => randomBytes(32).toString('base64url');
const sha = b => createHash('sha256').update(b).digest('hex');

// ---------------------------------------------------------------------------
// Fake desktop backend
// ---------------------------------------------------------------------------
const bigPayload = randomBytes(5 * 1024 * 1024);
let lastSlowClosed = false;
const firehose = { written: 0, closed: false };

const backend = createServer((req, res) => {
    if (req.url === '/api/health') {
        res.writeHead(200, { 'content-type': 'application/json' });
        return res.end(JSON.stringify({ ok: true }));
    }
    if (req.url.startsWith('/api/echo')) {
        const chunks = [];
        req.on('data', c => chunks.push(c));
        req.on('end', () => {
            res.writeHead(200, { 'content-type': 'application/json', 'x-echo-header': req.headers['x-test-header'] || '' });
            res.end(JSON.stringify({ body: Buffer.concat(chunks).toString(), method: req.method, url: req.url }));
        });
        return;
    }
    if (req.url === '/api/upload') {
        const hash = createHash('sha256');
        let n = 0;
        req.on('data', c => {
            hash.update(c);
            n += c.length;
        });
        req.on('end', () => {
            res.writeHead(200, { 'content-type': 'application/json' });
            res.end(JSON.stringify({ sha: hash.digest('hex'), n }));
        });
        return;
    }
    if (req.url === '/api/sse') {
        res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' });
        let n = 0;
        const timer = setInterval(() => {
            res.write(`data: event-${++n}\n\n`);
            if (n >= 3) {
                clearInterval(timer);
                res.end();
            }
        }, 300);
        res.on('close', () => clearInterval(timer));
        return;
    }
    if (req.url === '/api/slow') {
        lastSlowClosed = false;
        res.writeHead(200, { 'content-type': 'text/event-stream' });
        res.write('data: first\n\n');
        res.on('close', () => (lastSlowClosed = true));
        return;
    }
    if (req.url.startsWith('/api/firehose')) {
        // Streams `mb` MB as fast as the reader takes it, honouring backpressure.
        const total = Number(new URL(req.url, 'http://x').searchParams.get('mb') || 256) * 1024 * 1024;
        const chunk = Buffer.alloc(64 * 1024, 7);
        firehose.written = 0;
        firehose.closed = false;
        res.writeHead(200, { 'content-type': 'application/octet-stream', 'content-length': String(total) });
        res.on('close', () => (firehose.closed = true));
        const pump = () => {
            while (firehose.written < total && !res.destroyed) {
                firehose.written += chunk.length;
                if (!res.write(chunk)) return res.once('drain', pump);
            }
            res.end();
        };
        return pump();
    }
    if (req.url === '/api/big') {
        res.writeHead(200, { 'content-type': 'application/octet-stream', 'content-length': String(bigPayload.length) });
        return res.end(bigPayload);
    }
    if (req.url === '/api/early') {
        // Answers without reading the request body.
        res.writeHead(200, { 'content-type': 'text/plain' });
        return res.end('early');
    }
    if (req.url === '/api/empty') {
        res.writeHead(204);
        return res.end();
    }
    if (req.url === '/api/drop') {
        // The desktop's own request fails before any response head → herr.
        return req.socket.destroy();
    }
    res.writeHead(404).end();
});
const backendWss = new WebSocketServer({ server: backend });
backendWss.on('connection', (ws, req) => {
    if (req.url === '/ws/hello') ws.send('hello-from-desktop');
    ws.on('message', (data, isBinary) => ws.send(data, { binary: isBinary }));
});
await new Promise(r => backend.listen(BACKEND_PORT, '127.0.0.1', r));

// ---------------------------------------------------------------------------
// Relay launchers
// ---------------------------------------------------------------------------
async function startRelay(vars = {}, { main } = {}) {
    if (EXTERNAL) return { origin: EXTERNAL, stop() {} };
    const port = nextRelayPort++;
    const origin = `http://127.0.0.1:${port}`;
    let child;
    if (EDITION === 'cloudflare') {
        const args = ['wrangler', 'dev', ...(main ? [main] : []), '--local', '--ip', '127.0.0.1', '--port', String(port), '--inspector-port', String(port + 1000)];
        for (const [k, v] of Object.entries(vars)) args.push('--var', `${k}:${v}`);
        child = spawn('npx', args, { cwd: ROOT, env: { ...process.env, CI: '1', WRANGLER_SEND_METRICS: 'false' }, stdio: ['ignore', 'pipe', 'pipe'] });
    } else {
        child = spawn(process.execPath, [path.join(ROOT, 'index.mjs')], {
            env: { PATH: process.env.PATH, PORT: String(port), HOST: '127.0.0.1', ...vars },
            stdio: ['ignore', 'pipe', 'pipe'],
        });
    }
    let log = '';
    child.stdout.on('data', d => (log += d));
    child.stderr.on('data', d => (log += d));
    for (let i = 0; i < (EDITION === 'cloudflare' ? 600 : 100); i++) {
        try {
            if ((await fetch(`${origin}/healthz`)).ok) return { origin, log: () => log, stop: () => child.kill('SIGTERM') };
        } catch {}
        await sleep(100);
    }
    child.kill('SIGKILL');
    throw new Error(`relay did not start:\n${log}`);
}

async function connected(opts) {
    const agent = new FakeAgent({ backendPort: BACKEND_PORT, ...opts });
    await agent.connect();
    return agent;
}

async function rejectedWith(opts) {
    const agent = new FakeAgent({ backendPort: BACKEND_PORT, ...opts });
    try {
        await agent.connect();
        agent.close();
        return 'registered';
    } catch {
        for (let i = 0; i < 30 && !agent.closed; i++) await sleep(50);
        return agent.closed?.code ?? 'no-close';
    }
}

function openPhoneSocket(url) {
    return new Promise((resolve, reject) => {
        const ws = new WebSocket(url);
        const messages = [];
        ws.messages = messages;
        ws.closedWith = new Promise(r => ws.on('close', code => r(code)));
        ws.on('message', (data, isBinary) => messages.push(isBinary ? Buffer.from(data) : data.toString()));
        ws.on('open', () => resolve(ws));
        ws.on('error', reject);
    });
}

async function waitFor(fn, ms = 3000) {
    const end = Date.now() + ms * SLACK;
    while (Date.now() < end) {
        if (fn()) return true;
        await sleep(25);
    }
    return false;
}

/**
 * A phone upload whose body is produced on demand, so the test can see how
 * much of it the relay has taken. Optionally pauses after the first chunk.
 */
function streamingUpload(url, total, { chunk = 256 * 1024, pauseAfterFirstMs = 0 } = {}) {
    const block = randomBytes(chunk);
    const hash = createHash('sha256');
    const state = { pushed: 0, sha: null };
    const body = new ReadableStream(
        {
            async pull(controller) {
                if (state.pushed >= total) {
                    state.sha = hash.digest('hex');
                    return controller.close();
                }
                if (state.pushed === chunk && pauseAfterFirstMs) await sleep(pauseAfterFirstMs);
                const piece = block.subarray(0, Math.min(chunk, total - state.pushed));
                hash.update(piece);
                state.pushed += piece.length;
                controller.enqueue(new Uint8Array(piece));
            },
        },
        { highWaterMark: 0 }
    );
    state.response = fetch(url, { method: 'POST', body, duplex: 'half' }).then(
        async res => ({ status: res.status, body: await res.text() }),
        err => ({ status: 0, body: String(err?.cause?.code || err) })
    );
    return state;
}

function mintToken(secret, claims) {
    const b64 = o => Buffer.from(JSON.stringify(o)).toString('base64url');
    const input = `${b64({ alg: 'HS256', typ: 'JWT' })}.${b64(claims)}`;
    return `${input}.${createHmac('sha256', secret).update(input).digest('base64url')}`;
}

// ---------------------------------------------------------------------------
// Suite 1: an open relay — the tunnel itself, once with a protocol-1 desktop
// (one that never mentions `proto`) and once with a protocol-2 desktop
// ---------------------------------------------------------------------------
console.log(`[e2e] edition: ${EDITION}`);
for (const PROTO of [1, 2]) {
    console.log(`  — protocol ${PROTO} desktop`);
    const offer = PROTO === 2 ? 2 : undefined;
    const relay = await startRelay();
    const CODE = newCode();
    const base = `${relay.origin}/c/${CODE}`;
    const wsBase = base.replace(/^http/, 'ws');
    try {
        const health = await (await fetch(`${relay.origin}/healthz`)).json();
        check('GET /healthz', health.ok === true, JSON.stringify(health));

        const agent = await connected({ relay: relay.origin, code: CODE, wopenDelayMs: 150, proto: offer });
        check('desktop registers', true);
        check(`…and speaks protocol ${PROTO}`, agent.proto === PROTO && (agent.registered.proto ?? 1) === PROTO, JSON.stringify(agent.registered));

        {
            const res = await fetch(`${base}/api/health`);
            const body = await res.json().catch(() => null);
            check('GET through the tunnel', res.status === 200 && body?.ok === true, `status=${res.status}`);
        }
        {
            const res = await fetch(`${base}/api/echo?x=1&y=two`, {
                method: 'POST',
                headers: { 'content-type': 'application/json', 'x-test-header': 'hello-relay' },
                body: JSON.stringify({ ping: 'pong' }),
            });
            const body = await res.json().catch(() => null);
            check(
                'method, body, headers and query pass through',
                res.status === 200 && body?.method === 'POST' && body?.body === '{"ping":"pong"}' && body?.url === '/api/echo?x=1&y=two' && res.headers.get('x-echo-header') === 'hello-relay',
                JSON.stringify(body)
            );
        }
        {
            const res = await fetch(`${base}/api/empty`, { method: 'DELETE' });
            check('204 with no body', res.status === 204, `status=${res.status}`);
        }
        {
            const res = await fetch(`${base}/api/drop`);
            check('desktop error before the head → 502', res.status === 502, `status=${res.status}`);
        }
        {
            const t0 = Date.now();
            const reader = (await fetch(`${base}/api/sse`)).body.getReader();
            let first = 0;
            let text = '';
            for (;;) {
                const { done, value } = await reader.read();
                if (done) break;
                if (!first) first = Date.now();
                text += Buffer.from(value).toString();
            }
            const ok = text.includes('event-1') && text.includes('event-3') && first - t0 < 700 * SLACK && Date.now() - first > 400;
            check('SSE streams incrementally', ok, `first=${first - t0}ms total=${Date.now() - t0}ms`);
        }
        {
            const buf = Buffer.from(await (await fetch(`${base}/api/big`)).arrayBuffer());
            check('5 MB download intact', buf.length === bigPayload.length && sha(buf) === sha(bigPayload), `len=${buf.length}`);
        }
        {
            const up = randomBytes(3 * 1024 * 1024);
            const body = await (await fetch(`${base}/api/upload`, { method: 'POST', body: up })).json();
            check('3 MB upload intact', body.n === up.length && body.sha === sha(up), JSON.stringify(body));
        }
        {
            const ws = await openPhoneSocket(`${wsBase}/ws/echo`);
            ws.send('queued-before-open'); // the desktop answers wopened 150 ms later
            ws.send(Buffer.from([1, 2, 3, 4, 5]));
            const ok = await waitFor(() => ws.messages.length >= 2);
            check(
                'WebSocket text + binary, queued until the desktop is ready',
                ok && ws.messages[0] === 'queued-before-open' && Buffer.isBuffer(ws.messages[1]) && ws.messages[1].equals(Buffer.from([1, 2, 3, 4, 5])),
                JSON.stringify(ws.messages.map(String))
            );
            ws.close();
        }
        {
            const ws = await openPhoneSocket(`${wsBase}/ws/hello`);
            check('desktop-initiated WebSocket message', await waitFor(() => ws.messages[0] === 'hello-from-desktop'));
            const t0 = Date.now();
            ws.close();
            check('phone close reaches the desktop', await waitFor(() => agent.sockets.size === 0));
            const closeCode = await Promise.race([ws.closedWith, sleep(2000 * SLACK).then(() => 'open')]);
            check('…and the close handshake completes', closeCode === 1000 || closeCode === 1005, `code=${closeCode} after ${Date.now() - t0}ms`);
        }
        if (EDITION === 'cloudflare') {
            // `wrangler dev` fronts workerd with its own proxy, which keeps the
            // upstream request alive after the phone hangs up, so neither the
            // stream cancel nor request.signal fires locally.
            console.log('  - phone abort cancels the desktop request (skipped: wrangler dev does not forward client disconnects)');
        } else {
            const controller = new AbortController();
            const res = await fetch(`${base}/api/slow`, { signal: controller.signal });
            await res.body.getReader().read();
            controller.abort();
            check('phone abort cancels the desktop request', await waitFor(() => lastSlowClosed));
        }
        {
            const res = await fetch(`${relay.origin}/c/${'x'.repeat(43)}/api/health`);
            check('unknown code → 502', res.status === 502, `status=${res.status}`);
            const res2 = await fetch(`${relay.origin}/nope`);
            check('unknown path → 404', res2.status === 404, `status=${res2.status}`);
            const err = await openPhoneSocket(`${relay.origin.replace(/^http/, 'ws')}/c/${'x'.repeat(43)}/ws/echo`).then(
                ws => (ws.close(), 'opened'),
                e => e.message
            );
            check('unknown code WebSocket refused', err !== 'opened', err);
        }
        if (!EXTERNAL) {
            const res = await fetch(`${relay.origin}/turn`, { method: 'POST' });
            check('/turn → 404 when TURN is not configured', res.status === 404, `status=${res.status}`);
            agent.send({ t: 'push', id: 'p1', notifications: [] });
            check('push → unsupported without an APNs key', await waitFor(() => agent.pushReplies[0]?.unsupported === true), JSON.stringify(agent.pushReplies));
        }
        if (PROTO === 2) {
            check(
                'protocol 2: payloads travel in binary frames and bodies are streamed',
                agent.stats.binaryIn > 0 && agent.stats.binaryOut > 0 && agent.stats.jsonData === 0 && agent.stats.streamed >= 2,
                JSON.stringify(agent.stats)
            );
        } else {
            check('protocol 1: no binary frames, no streamed bodies', agent.stats.binaryIn === 0 && agent.stats.binaryOut === 0 && agent.stats.streamed === 0, JSON.stringify(agent.stats));
        }
        {
            // A newer desktop with the same code takes over; the old one is
            // told so and phone sockets bridged through it are dropped.
            const ws = await openPhoneSocket(`${wsBase}/ws/echo`);
            await waitFor(() => agent.sockets.size === 1);
            const agent2 = await connected({ relay: relay.origin, code: CODE, proto: offer });
            const oldClosed = await waitFor(() => agent.closed);
            const phoneCode = await Promise.race([ws.closedWith, sleep(3000 * SLACK).then(() => 'open')]);
            const res = await fetch(`${base}/api/health`);
            check('newer desktop replaces the older one', oldClosed && agent.closed.code === 4000 && res.status === 200, JSON.stringify(agent.closed));
            check('…and bridged phone sockets are dropped', phoneCode === 1012, String(phoneCode));

            const ws2 = await openPhoneSocket(`${wsBase}/ws/echo`);
            agent2.close();
            const code2 = await Promise.race([ws2.closedWith, sleep(3000 * SLACK).then(() => 'open')]);
            check('desktop going away closes phone sockets', code2 === 1012, String(code2));
            await sleep(100);
            const after = await fetch(`${base}/api/health`);
            check('…and later requests get 502', after.status === 502, `status=${after.status}`);
        }
    } catch (e) {
        check(`suite: open relay (protocol ${PROTO})`, false, e.stack);
        if (relay.log) console.log(relay.log());
    } finally {
        relay.stop();
    }
}

// ---------------------------------------------------------------------------
// Suite 2: a relay gated by a shared registration key, with TURN
// ---------------------------------------------------------------------------
if (!EXTERNAL) {
    const KEY = 'k-' + randomBytes(12).toString('hex');
    const TURN_SECRET = 's-' + randomBytes(12).toString('hex');
    const relay = await startRelay({ RELAY_REGISTRATION_KEY: KEY, TURN_URLS: 'stun:turn.test:3478,turn:turn.test:3478?transport=udp', TURN_SECRET });
    try {
        check('wrong registration key → 4003', (await rejectedWith({ relay: relay.origin, code: newCode(), key: 'nope' })) === 4003);
        check('missing registration key → 4003', (await rejectedWith({ relay: relay.origin, code: newCode() })) === 4003);
        const agent = await connected({ relay: relay.origin, code: newCode(), key: KEY });
        check('right registration key registers', true);
        agent.close();

        const denied = await fetch(`${relay.origin}/turn`, { method: 'POST' });
        check('/turn without the key → 401', denied.status === 401, `status=${denied.status}`);
        const res = await fetch(`${relay.origin}/turn`, { method: 'POST', headers: { authorization: `Bearer ${KEY}` } });
        const body = await res.json().catch(() => null);
        const ice = body?.iceServers?.[0];
        const expected = ice && createHmac('sha1', TURN_SECRET).update(ice.username).digest('base64');
        check('/turn mints coturn credentials', res.status === 200 && ice.urls.length === 2 && ice.credential === expected, JSON.stringify(body));
    } catch (e) {
        check('suite: registration key', false, e.stack);
        if (relay.log) console.log(relay.log());
    } finally {
        relay.stop();
    }
}

// ---------------------------------------------------------------------------
// Suite 3: a relay gated by signed per-user tokens
// ---------------------------------------------------------------------------
if (!EXTERNAL) {
    const SECRET = randomBytes(24).toString('hex');
    const now = Math.floor(Date.now() / 1000);
    const TOKEN_MESSAGE = 'Sign in to use this relay';
    // LOCATION_HINT puts Cloudflare tunnels under a `<hint>:` name; the Node edition ignores it.
    const relay = await startRelay({ RELAY_TOKEN_SECRET: SECRET, RELAY_TOKEN_MESSAGE: TOKEN_MESSAGE, LOCATION_HINT: 'apac' });
    try {
        check('no token → 4401', (await rejectedWith({ relay: relay.origin, code: newCode() })) === 4401);
        const unsigned = new FakeAgent({ relay: relay.origin, code: newCode(), backendPort: BACKEND_PORT });
        await unsigned.connect().catch(() => {});
        check('RELAY_TOKEN_MESSAGE is what the desktop is told', unsigned.errors[0] === TOKEN_MESSAGE, JSON.stringify(unsigned.errors));
        check(
            'expired token → 4401',
            (await rejectedWith({ relay: relay.origin, code: newCode(), key: mintToken(SECRET, { sub: 'u1', aud: 'alma-relay', exp: now - 10 }) })) === 4401
        );
        check(
            'token for another audience → 4401',
            (await rejectedWith({ relay: relay.origin, code: newCode(), key: mintToken(SECRET, { sub: 'u1', aud: 'other', exp: now + 600 }) })) === 4401
        );
        check(
            'token signed with another secret → 4401',
            (await rejectedWith({ relay: relay.origin, code: newCode(), key: mintToken('wrong', { sub: 'u1', aud: 'alma-relay', exp: now + 600 }) })) === 4401
        );
        const minted = await new Promise((resolve, reject) => {
            const child = spawn(process.execPath, [path.join(ROOT, 'scripts', 'mint-token.mjs'), 'alice', '1'], { env: { ...process.env, RELAY_TOKEN_SECRET: SECRET } });
            let out = '';
            child.stdout.on('data', d => (out += d));
            child.on('exit', c => (c === 0 ? resolve(out.trim()) : reject(new Error(`mint-token exited ${c}`))));
        });
        const CODE = newCode();
        const agent = await connected({ relay: relay.origin, code: CODE, key: minted });
        const res = await fetch(`${relay.origin}/c/${CODE}/api/health`);
        check('token from scripts/mint-token.mjs registers and tunnels', res.status === 200, `status=${res.status}`);
        const drop = await fetch(`${relay.origin}/c/${CODE}/api/drop`);
        const dropBody = await drop.json().catch(() => ({}));
        check('with a location hint, a desktop error still reaches the phone', drop.status === 502 && !/offline/.test(dropBody.error || ''), `${drop.status} ${JSON.stringify(dropBody)}`);
        const gone = await fetch(`${relay.origin}/c/${newCode()}/api/health`);
        check('with a location hint, an unknown code → 502', gone.status === 502, `status=${gone.status}`);
        agent.close();
    } catch (e) {
        check('suite: relay tokens', false, e.stack);
        if (relay.log) console.log(relay.log());
    } finally {
        relay.stop();
    }
}

// ---------------------------------------------------------------------------
// Suite 4 (Cloudflare edition): a desktop still in its un-hinted tunnel
// ---------------------------------------------------------------------------
if (EDITION === 'cloudflare') {
    const relay = await startRelay({ LOCATION_HINT: 'apac' }, { main: 'test/legacy-worker.mjs' });
    try {
        const CODE = newCode();
        const legacy = await connected({ relay: relay.origin, code: CODE, agentQuery: '&legacy=1' });
        const res = await fetch(`${relay.origin}/c/${CODE}/api/echo`, { method: 'POST', body: 'hello-legacy' });
        const body = await res.json().catch(() => ({}));
        check('phone reaches a desktop still in its un-hinted tunnel', res.status === 200 && body.body === 'hello-legacy', `${res.status} ${JSON.stringify(body)}`);
        const ws = await openPhoneSocket(`${relay.origin.replace(/^http/, 'ws')}/c/${CODE}/ws/hello`);
        check('…over WebSocket too', await waitFor(() => ws.messages.includes('hello-from-desktop')));
        ws.close();
        legacy.close();
        await sleep(200);

        // Protocol 2 with a hint: a large body is streamed, not buffered in
        // the Worker, to whichever tunnel the desktop is in.
        const legacy2 = await connected({ relay: relay.origin, code: CODE, agentQuery: '&legacy=1', proto: 2 });
        const up = streamingUpload(`${relay.origin}/c/${CODE}/api/upload`, 8 * 1024 * 1024);
        const r1 = await up.response;
        check('a streamed body reaches a desktop in its un-hinted tunnel', r1.status === 200 && JSON.parse(r1.body).sha === up.sha && legacy2.stats.streamed === 1, `${r1.status} ${r1.body}`);
        legacy2.close();
        await sleep(200);

        const moved = await connected({ relay: relay.origin, code: CODE, proto: 2 });
        const again = await fetch(`${relay.origin}/c/${CODE}/api/health`);
        check('…and in the hinted tunnel once it redials', again.status === 200, `status=${again.status}`);
        const up2 = streamingUpload(`${relay.origin}/c/${CODE}/api/upload`, 8 * 1024 * 1024);
        const r2 = await up2.response;
        check('…streamed there too', r2.status === 200 && JSON.parse(r2.body).sha === up2.sha && moved.stats.streamed === 1, `${r2.status} ${r2.body}`);
        // A chunked body carries no content-length: it has to take the probe
        // path too, or the Worker buffers it and fails it at MAX_BODY (24 MB)
        // instead of streaming it up to MAX_BODY_MB.
        const big = streamingUpload(`${relay.origin}/c/${CODE}/api/upload`, 30 * 1024 * 1024);
        const bigRes = await big.response;
        check('a chunked body over 24 MB streams under a hint', bigRes.status === 200 && JSON.parse(bigRes.body).sha === big.sha, `${bigRes.status} ${bigRes.body}`);
        moved.close();
    } catch (e) {
        check('suite: legacy tunnels', false, e.stack);
        if (relay.log) console.log(relay.log());
    } finally {
        relay.stop();
    }
}

// ---------------------------------------------------------------------------
// Suite 5 (Node edition): flow control and limits
// ---------------------------------------------------------------------------
if (EDITION === 'node') {
    const relay = await startRelay({
        STALL_TIMEOUT_S: '2',
        MAX_BODY_MB: '1',
        MAX_AGENTS: '1',
        MAX_REQUESTS_PER_AGENT: '3',
        MAX_SOCKETS_PER_AGENT: '2',
    });
    const CODE = newCode();
    const base = `${relay.origin}/c/${CODE}`;
    const wsBase = base.replace(/^http/, 'ws');
    /** A phone GET whose body it reads only when told to. */
    const phoneGet = url =>
        new Promise((resolve, reject) => {
            httpGet(url, { agent: false }, res => {
                res.pause();
                res.received = 0;
                res.ended = new Promise(r => {
                    res.on('data', c => (res.received += c.length));
                    res.on('end', () => r('end'));
                    res.on('aborted', () => r('aborted'));
                    res.on('error', () => r('error'));
                    res.on('close', () => r('close'));
                });
                resolve(res);
            }).on('error', reject);
        });
    try {
        const agent = await connected({ relay: relay.origin, code: CODE });

        // A phone that stops reading: the relay stops reading the desktop
        // instead of buffering the whole stream in memory...
        const stuck = await phoneGet(`${base}/api/firehose?mb=256`);
        await sleep(1000);
        const writtenWhileStuck = firehose.written;
        check('slow phone: desktop is held back (backpressure)', writtenWhileStuck < 64 * 1024 * 1024, `${(writtenWhileStuck / 1048576).toFixed(1)} MB written`);
        // ...and cuts it off once it has drained nothing for STALL_TIMEOUT_S.
        check('stalled phone is cut off and the desktop told to stop', (await waitFor(() => firehose.closed, 5000)) && firehose.written < 128 * 1024 * 1024, `closed=${firehose.closed} written=${firehose.written}`);
        stuck.destroy();
        const after = await fetch(`${base}/api/health`);
        check('tunnel works again after a stalled phone', after.status === 200, `status=${after.status}`);

        // A phone that pauses briefly and then reads gets every byte.
        const paused = await phoneGet(`${base}/api/firehose?mb=32`);
        await sleep(800);
        paused.resume();
        const how = await paused.ended;
        check('paused phone resumes and gets the whole body', how === 'end' && paused.received === 32 * 1024 * 1024, `${how} ${paused.received}`);

        // Limits.
        const big = await fetch(`${base}/api/echo`, { method: 'POST', body: Buffer.alloc(2 * 1024 * 1024) });
        check('body over MAX_BODY_MB → 413', big.status === 413, `status=${big.status}`);

        const hanging = await Promise.all([0, 1, 2].map(() => phoneGet(`${base}/api/slow`)));
        const fourth = await fetch(`${base}/api/health`);
        check('MAX_REQUESTS_PER_AGENT → 503', fourth.status === 503, `status=${fourth.status}`);
        for (const r of hanging) r.destroy();
        await sleep(300);
        const freed = await fetch(`${base}/api/health`);
        check('request slots free up again', freed.status === 200, `status=${freed.status}`);

        const s1 = await openPhoneSocket(`${wsBase}/ws/echo`);
        const s2 = await openPhoneSocket(`${wsBase}/ws/echo`);
        const third = await openPhoneSocket(`${wsBase}/ws/echo`).then(
            ws => (ws.close(), 'opened'),
            e => e.message
        );
        check('MAX_SOCKETS_PER_AGENT → upgrade refused', /503/.test(third), third);

        s1.send(Buffer.alloc(25 * 1024 * 1024), { binary: true });
        check('phone message over 24 MB closes the socket with 1009', (await Promise.race([s1.closedWith, sleep(5000 * SLACK).then(() => 'still open')])) === 1009);
        s2.close();

        const other = await rejectedWith({ relay: relay.origin, code: newCode() });
        check('MAX_AGENTS → a new desktop is refused with 1013', other === 1013, String(other));
        const same = await connected({ relay: relay.origin, code: CODE });
        check('MAX_AGENTS still lets a desktop replace itself', !!same);
        same.close();
        agent.close();
    } catch (e) {
        check('suite: flow control and limits', false, e.stack);
        if (relay.log) console.log(relay.log());
    } finally {
        relay.stop();
    }
}

// ---------------------------------------------------------------------------
// Suite 6: protocol 2 — streamed request bodies with flow control
// ---------------------------------------------------------------------------
if (!EXTERNAL || ARGS.includes('--protocol2')) {
    // MAX_BUFFERED_MB is far below the uploads below: a relay that buffered
    // them (protocol 1) would answer 503.
    const STALL_S = EXTERNAL ? Number(argValue('stall-timeout')) || 0 : 2;
    const MAX_MB = EXTERNAL ? Number(argValue('max-body-mb')) || 0 : 1; // for the 413 check
    const relay = await startRelay({ STALL_TIMEOUT_S: String(STALL_S), MAX_BUFFERED_MB: '8' });
    const CODE = newCode();
    const base = `${relay.origin}/c/${CODE}`;
    try {
        const agent = await connected({ relay: relay.origin, code: CODE, proto: 2 });
        const offered3 = await connected({ relay: relay.origin, code: newCode(), proto: 3 });
        check('a desktop offering a newer protocol gets the newest the relay has', offered3.proto === 2 && offered3.registered.proto === 2, JSON.stringify(offered3.registered));
        offered3.close();

        {
            const up = streamingUpload(`${base}/api/upload`, 40 * 1024 * 1024);
            const res = await up.response;
            const body = res.status === 200 ? JSON.parse(res.body) : {};
            check('40 MB upload streamed intact (over the protocol-1 body limit)', res.status === 200 && body.sha === up.sha && body.n === 40 * 1024 * 1024, `${res.status} ${res.body}`);
        }
        // Cloudflare's edge takes in the whole request body before a Worker
        // runs, so there the desktop can only see it once the phone is done.
        const edition = EXTERNAL ? (await (await fetch(`${relay.origin}/healthz`)).json()).edition : null; // wrangler dev streams
        if (edition === 'cloudflare') skip('hreq and the first body bytes reach the desktop before the upload ends', "Cloudflare's edge buffers request bodies");
        else {
            // The desktop sees the request while the phone is still uploading.
            const [requests, bytes] = [agent.hreqs.length, agent.stats.bodyBytes];
            const up = streamingUpload(`${base}/api/upload`, 4 * 1024 * 1024, { pauseAfterFirstMs: 1500 * SLACK });
            const early = await waitFor(() => agent.hreqs.length > requests && agent.stats.bodyBytes > bytes, 1200);
            const midway = up.pushed;
            const res = await up.response;
            check(
                'hreq and the first body bytes reach the desktop before the upload ends',
                early && midway < 4 * 1024 * 1024 && res.status === 200 && JSON.parse(res.body).sha === up.sha,
                `early=${early} pushed=${midway} ${res.status}`
            );
        }
        if (!HALF_READ_OK) {
            skip('desktop not reading: the phone is held back, then completes', WRANGLER_DEV);
            skip('desktop that stops reading for STALL_TIMEOUT_S → 504', WRANGLER_DEV);
            skip('desktop answering before reading the body', WRANGLER_DEV);
        } else {
        {
            // A desktop that stops reading holds the phone back...
            agent.holdBodies = true;
            // Bigger than the window and the socket buffers, within MAX_BODY_MB.
            const up = streamingUpload(`${base}/api/upload`, Math.min(96, MAX_MB > 2 ? MAX_MB - 1 : 96) * 1024 * 1024);
            await sleep(1000);
            const pushed = up.pushed;
            check('desktop not reading: the phone is held back (upload backpressure)', pushed < 40 * 1024 * 1024, `${(pushed / 1048576).toFixed(1)} MB pushed`);
            // ...and gets every byte once it reads again (before the stall timeout).
            agent.releaseBodies();
            const res = await up.response;
            check('…and the upload completes once it reads again', res.status === 200 && JSON.parse(res.body).sha === up.sha, `${res.status} ${res.body}`);
        }
        if (!STALL_S) skip('desktop that stops reading for STALL_TIMEOUT_S → 504', 'pass --stall-timeout=S for this deployment');
        else {
            agent.holdBodies = true;
            const up = streamingUpload(`${base}/api/upload`, 32 * 1024 * 1024);
            const res = await Promise.race([up.response, sleep((STALL_S * 1000 + 6000) * SLACK).then(() => ({ status: 'hung' }))]);
            check('desktop that stops reading for STALL_TIMEOUT_S → 504', res.status === 504, `${res.status} ${res.body || ''}`);
            agent.holdBodies = false;
            agent.held = [];
        }
        {
            const up = streamingUpload(`${base}/api/early`, 32 * 1024 * 1024);
            const res = await Promise.race([up.response, sleep(8000 * SLACK).then(() => ({ status: 'hung' }))]);
            check('desktop answering before reading the body', res.status === 200 && res.body === 'early', `${res.status} ${res.body || ''}`);
            const after = await fetch(`${base}/api/health`);
            check('…leaves the tunnel working', after.status === 200, `status=${after.status}`);
        }
        }
        agent.close();
    } catch (e) {
        check('suite: protocol 2', false, e.stack);
        if (relay.log) console.log(relay.log());
    } finally {
        relay.stop();
    }

    // Limits still apply to streamed bodies, and RELAY_PROTOCOL=1 makes a
    // relay act like one that predates protocol 2.
    const small = !HALF_READ_OK || !MAX_MB ? null : await startRelay({ MAX_BODY_MB: String(MAX_MB) });
    if (!small) skip('streamed body over MAX_BODY_MB → 413', HALF_READ_OK ? 'pass --max-body-mb=N for this deployment' : WRANGLER_DEV);
    else try {
        const CODE2 = newCode();
        const agent = await connected({ relay: small.origin, code: CODE2, proto: 2 });
        const up = streamingUpload(`${small.origin}/c/${CODE2}/api/upload`, (MAX_MB + 2) * 1024 * 1024);
        const res = await up.response;
        check('streamed body over MAX_BODY_MB → 413', res.status === 413, `${res.status} ${res.body}`);
        const after = await fetch(`${small.origin}/c/${CODE2}/api/health`);
        check('…and the tunnel still works', after.status === 200, `status=${after.status}`);
        agent.close();
    } catch (e) {
        check('suite: protocol 2 limits', false, e.stack);
        if (small.log) console.log(small.log());
    } finally {
        small.stop();
    }

    const v1 = EXTERNAL ? null : await startRelay({ RELAY_PROTOCOL: '1' });
    if (v1) try {
        const CODE3 = newCode();
        const agent = await connected({ relay: v1.origin, code: CODE3, proto: 2 });
        check('RELAY_PROTOCOL=1: a protocol-2 desktop falls back to 1', agent.proto === 1, JSON.stringify(agent.registered));
        const up = randomBytes(2 * 1024 * 1024);
        const body = await (await fetch(`${v1.origin}/c/${CODE3}/api/upload`, { method: 'POST', body: up })).json();
        const ws = await openPhoneSocket(`${v1.origin.replace(/^http/, 'ws')}/c/${CODE3}/ws/echo`);
        ws.send(Buffer.from([9, 8, 7]));
        ws.send('text');
        const echoed = await waitFor(() => ws.messages.length >= 2);
        ws.close();
        check(
            '…and still tunnels uploads and WebSockets, all as JSON',
            body.sha === sha(up) && echoed && Buffer.isBuffer(ws.messages[0]) && ws.messages[1] === 'text' && agent.stats.binaryIn === 0 && agent.stats.binaryOut === 0,
            JSON.stringify(agent.stats)
        );
        agent.close();
    } catch (e) {
        check('suite: RELAY_PROTOCOL=1', false, e.stack);
        if (v1.log) console.log(v1.log());
    } finally {
        v1.stop();
    }
}

backend.close();
for (const client of backendWss.clients) client.terminate();
console.log(failures === 0 ? '\n[e2e] ALL PASS' : `\n[e2e] ${failures} FAILURE(S)`);
process.exit(failures);
