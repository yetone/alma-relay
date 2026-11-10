// Pieces shared by both relay editions (Node and Cloudflare Workers).
// Everything here uses WebCrypto + standard globals, so it runs unchanged in
// Node >= 20 and inside a Worker.

export const VERSION = '1.4.0';

// ---------------------------------------------------------------------------
// Agent protocol versions (docs/PROTOCOL.md).
//
//   1  JSON text frames only; request bodies arrive whole, base64, in `hreq`.
//   2  Payload bytes travel in binary frames, and request bodies are streamed
//      to the desktop with a per-request credit window.
//   3  Per-stream downstream credit: the relay acknowledges (`ack`) the
//      response body / WebSocket bytes of each stream once they are on their
//      way to the phone, and the desktop keeps at most `window` bytes of a
//      stream unacknowledged. One slow phone then holds back only its own
//      stream, never the agent socket that every other stream shares.
//
// The desktop offers `proto` in `register`; the relay answers the version it
// will speak in `registered`. Either side that never mentions it speaks 1, so
// old desktops on new relays and new desktops on old relays keep working.
// ---------------------------------------------------------------------------
export const PROTOCOL = 3;

/** Negotiated version: the lower of what the desktop offers and the relay allows. */
export function negotiateProtocol(offered, relayMax = PROTOCOL) {
    const n = Math.floor(Number(offered));
    const max = Math.max(1, Math.min(PROTOCOL, Math.floor(Number(relayMax)) || PROTOCOL));
    return Number.isFinite(n) && n >= 2 ? Math.min(n, max) : 1;
}

/** Binary frame kinds (protocol 2). */
export const BIN = Object.freeze({
    HCHUNK: 1, // agent -> relay: response body bytes
    HBODY: 2, // relay -> agent: request body bytes
    WBIN: 3, // both: a binary WebSocket message
    WTEXT: 4, // both: a text WebSocket message, UTF-8
});

/** Request body bytes the relay may send ahead of the desktop's `hack`s. */
export const BODY_WINDOW = 4 * 1024 * 1024;
/** Largest request body slice the relay puts in one frame. */
export const BODY_SLICE = 256 * 1024;

/** Protocol 3: bytes of one stream the desktop may send ahead of the relay's `ack`s. */
export const STREAM_WINDOW = 1024 * 1024;
/** Smallest window a relay may advertise. */
export const MIN_STREAM_WINDOW = 64 * 1024;
const ACK_EAGER = 64 * 1024; // acknowledged at once from this many bytes
const ACK_DELAY_MS = 5; // smaller amounts are batched for this long

/** The window a relay advertises: `kb` from its configuration, or the default. */
export function streamWindow(kb) {
    const n = Math.floor(Number(kb) * 1024);
    return Number.isFinite(n) && n > 0 ? Math.max(MIN_STREAM_WINDOW, n) : STREAM_WINDOW;
}

/**
 * Protocol 3 acknowledgements, coalesced per stream: `send(id, n)` goes out
 * once a stream has ACK_EAGER bytes owed, or ACK_DELAY_MS after the first.
 */
export class AckBatcher {
    constructor(send) {
        this.send = send;
        this.owed = new Map();
        this.timer = null;
    }

    add(id, n) {
        if (!(n > 0)) return;
        const total = (this.owed.get(id) || 0) + n;
        if (total >= ACK_EAGER) {
            this.owed.delete(id);
            this.send(id, total);
            return;
        }
        this.owed.set(id, total);
        this.timer ??= setTimeout(() => this.flush(), ACK_DELAY_MS);
    }

    flush() {
        clearTimeout(this.timer);
        this.timer = null;
        const owed = [...this.owed];
        this.owed.clear();
        for (const [id, n] of owed) this.send(id, n);
    }

    clear() {
        clearTimeout(this.timer);
        this.timer = null;
        this.owed.clear();
    }
}

const textEncoder = new TextEncoder();

/** `[kind:u8][idLength:u8][id:utf8][payload]` */
export function encodeBinary(kind, id, payload) {
    const idBytes = textEncoder.encode(String(id));
    if (idBytes.length > 255) throw new Error('frame id too long');
    const bytes = typeof payload === 'string' ? textEncoder.encode(payload) : payload;
    const out = new Uint8Array(2 + idBytes.length + bytes.length);
    out[0] = kind;
    out[1] = idBytes.length;
    out.set(idBytes, 2);
    out.set(bytes, 2 + idBytes.length);
    return out;
}

/** -> {kind, id, data} (data is a view into `bytes`), or null if malformed. */
export function decodeBinary(bytes) {
    const view = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
    if (view.length < 2 || view.length < 2 + view[1]) return null;
    const id = new TextDecoder().decode(view.subarray(2, 2 + view[1]));
    return { kind: view[0], id, data: view.subarray(2 + view[1]) };
}

/** Phone-facing paths look like /c/<code>/rest?query. Never log the code. */
const CLIENT_PATH = /^\/c\/([A-Za-z0-9_-]{16,128})(\/.*)?$/;

/** Parse `/c/<code>/rest?query` -> {code, rest} or null. */
export function parseClientPath(pathWithQuery) {
    const m = CLIENT_PATH.exec(pathWithQuery || '');
    if (!m) return null;
    return { code: m[1], rest: m[2] || '/' };
}

export const isTokenHash = s => typeof s === 'string' && /^[a-f0-9]{64}$/.test(s);

const enc = new TextEncoder();

export async function sha256hex(text) {
    const digest = await crypto.subtle.digest('SHA-256', enc.encode(text));
    return [...new Uint8Array(digest)].map(b => b.toString(16).padStart(2, '0')).join('');
}

export function b64encode(bytes) {
    let binary = '';
    const chunk = 0x8000;
    for (let i = 0; i < bytes.length; i += chunk) {
        binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
    }
    return btoa(binary);
}

export function b64decode(text) {
    const binary = atob(text);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    return bytes;
}

function b64urlDecode(s) {
    const p = s.replace(/-/g, '+').replace(/_/g, '/');
    return b64decode(p + '='.repeat((4 - (p.length % 4)) % 4));
}

/** Constant-time string compare (length leaks, content doesn't). */
export function safeEqual(a, b) {
    const x = enc.encode(String(a));
    const y = enc.encode(String(b));
    if (x.length !== y.length) return false;
    let diff = 0;
    for (let i = 0; i < x.length; i++) diff |= x[i] ^ y[i];
    return diff === 0;
}

/** WebSocket close codes a server may legally send on. */
export function sanitizeCloseCode(code) {
    const n = Number(code);
    if (n === 1000 || (n >= 3000 && n <= 4999)) return n;
    return 1000;
}

// ---------------------------------------------------------------------------
// Signed relay tokens (optional, for running one relay for many users).
//
// An HS256 JWT signed with RELAY_TOKEN_SECRET, audience "alma-relay", with a
// `sub` (the user id) and an optional `exp`. Mint them with any JWT library,
// or with `node scripts/mint-token.mjs <user-id>`.
// ---------------------------------------------------------------------------
export const RELAY_AUDIENCE = 'alma-relay';

/** Returns the token's claims if it is valid, unexpired and for this relay; else null. */
export async function verifyRelayToken(token, secret) {
    try {
        const parts = String(token || '').split('.');
        if (parts.length !== 3) return null;
        const key = await crypto.subtle.importKey('raw', enc.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['verify']);
        const ok = await crypto.subtle.verify('HMAC', key, b64urlDecode(parts[2]), enc.encode(`${parts[0]}.${parts[1]}`));
        if (!ok) return null;
        const header = JSON.parse(new TextDecoder().decode(b64urlDecode(parts[0])));
        if (header.alg !== 'HS256') return null;
        const claims = JSON.parse(new TextDecoder().decode(b64urlDecode(parts[1])));
        if (claims.aud !== RELAY_AUDIENCE) return null;
        // RFC 7519: the current time must be strictly before exp.
        if (typeof claims.exp === 'number' && claims.exp <= Math.floor(Date.now() / 1000)) return null;
        if (!claims.sub) return null;
        return claims;
    } catch {
        return null;
    }
}

/**
 * Who may register a desktop (and mint TURN credentials).
 *   RELAY_TOKEN_SECRET set     -> a valid signed relay token is required
 *   RELAY_REGISTRATION_KEY set -> that shared key is required
 *   neither                    -> open relay (fine for personal use: a desktop
 *                                 is only reachable with its 256-bit code)
 * Returns {ok:true, subject} or {ok:false, message, code}.
 */
export async function authorize(env, presented) {
    if (env.RELAY_TOKEN_SECRET) {
        const claims = await verifyRelayToken(presented, env.RELAY_TOKEN_SECRET);
        if (!claims) return { ok: false, message: env.RELAY_TOKEN_MESSAGE || 'A valid relay token is required', reason: 'Relay token required', code: 4401 };
        return { ok: true, subject: String(claims.sub) };
    }
    if (env.RELAY_REGISTRATION_KEY && !safeEqual(presented || '', env.RELAY_REGISTRATION_KEY)) {
        return { ok: false, message: 'Invalid registration key', reason: 'Invalid registration key', code: 4003 };
    }
    return { ok: true, subject: null };
}

// ---------------------------------------------------------------------------
// TURN credentials for the phone<->desktop WebRTC stream (TURN REST API, as
// spoken by coturn's `use-auth-secret`): username "<expiry>:<id>", credential
// = base64(HMAC-SHA1(secret, username)). The relay only mints them; media
// never passes through it. Unset TURN_URLS / TURN_SECRET -> null (404).
// ---------------------------------------------------------------------------
export async function turnCredentials(env, subject) {
    const urls = String(env.TURN_URLS || '')
        .split(',')
        .map(s => s.trim())
        .filter(Boolean);
    if (urls.length === 0 || !env.TURN_SECRET) return null;
    const ttl = Math.min(86400, Math.max(300, Number(env.TURN_TTL) || 43200));
    const expiresAt = Math.floor(Date.now() / 1000) + ttl;
    // The id only feeds coturn's per-user quota; it is never the account itself.
    const id = (await sha256hex(String(subject || 'anon'))).slice(0, 12);
    const username = `${expiresAt}:${id}`;
    const key = await crypto.subtle.importKey('raw', enc.encode(env.TURN_SECRET), { name: 'HMAC', hash: 'SHA-1' }, false, ['sign']);
    const mac = new Uint8Array(await crypto.subtle.sign('HMAC', key, enc.encode(username)));
    return { iceServers: [{ urls, username, credential: b64encode(mac) }], ttl, expiresAt };
}

/** Bearer value of an Authorization header. */
export const bearerOf = header => String(header || '').replace(/^Bearer\s+/i, '').trim();
