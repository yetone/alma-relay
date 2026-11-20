// FCM delivery (Android), shared by both relay editions (Node and Cloudflare).
//
// The desktop marks notifications for Android phones `platform: 'android'`
// and shapes their payload for FCM already, as a data-only message:
//
//   {token, environment, platform:'android', collapseId?, payload:{data:{title, body, kind, threadId, ...}}}
//
// The relay holds the Firebase service-account key (FCM_SERVICE_ACCOUNT, the
// JSON Google hands out), trades it for an OAuth access token, and posts each
// one to the FCM HTTP v1 API. Results come back in the same
// `{token, status, reason?}` shape as APNs, so the desktop prunes dead tokens
// the same way (FCM says 404 UNREGISTERED).
//
// WebCrypto + fetch only, so it runs unchanged inside a Worker; FCM speaks
// HTTP/1.1, so Node's fetch is fine here too.

import { clampBatch } from './apns.mjs';

const SCOPE = 'https://www.googleapis.com/auth/firebase.messaging';
const TOKEN_URL = 'https://oauth2.googleapis.com/token';
/** Google issues 1-hour access tokens; renew well before. */
const EARLY_RENEW_MS = 5 * 60 * 1000;
/** FCM's data payload limit is 4096 bytes. */
const MAX_DATA_BYTES = 4000;

const b64url = bytes =>
    btoa(String.fromCharCode(...new Uint8Array(bytes)))
        .replace(/\+/g, '-')
        .replace(/\//g, '_')
        .replace(/=+$/, '');
const b64urlText = text => b64url(new TextEncoder().encode(text));

function pemToPkcs8(pem) {
    const body = String(pem)
        .replace(/\\n/g, '\n')
        .replace(/-----[A-Z ]+-----/g, '')
        .replace(/\s+/g, '');
    const raw = atob(body);
    const out = new Uint8Array(raw.length);
    for (let i = 0; i < raw.length; i++) out[i] = raw.charCodeAt(i);
    return out.buffer;
}

/** Mints and caches the OAuth access token FCM wants in `authorization`. */
export class FcmSender {
    constructor({ clientEmail, privateKey, projectId }) {
        if (!clientEmail || !privateKey || !projectId) throw new Error('FCM sender needs client_email, private_key and project_id');
        this.clientEmail = clientEmail;
        this.pem = privateKey;
        this.projectId = projectId;
        this.cryptoKey = null;
        this.cached = null; // {token, expiresAt}
        this.pending = null;
    }

    async assertion(now = Date.now()) {
        if (!this.cryptoKey) {
            this.cryptoKey = await crypto.subtle.importKey('pkcs8', pemToPkcs8(this.pem), { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' }, false, ['sign']);
        }
        const iat = Math.floor(now / 1000);
        const header = b64urlText(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
        const claims = b64urlText(JSON.stringify({ iss: this.clientEmail, scope: SCOPE, aud: TOKEN_URL, iat, exp: iat + 3600 }));
        const signingInput = `${header}.${claims}`;
        const sig = await crypto.subtle.sign('RSASSA-PKCS1-v1_5', this.cryptoKey, new TextEncoder().encode(signingInput));
        return `${signingInput}.${b64url(sig)}`;
    }

    async token(fetchImpl = fetch) {
        const now = Date.now();
        if (this.cached && now < this.cached.expiresAt - EARLY_RENEW_MS) return this.cached.token;
        // One exchange at a time: a burst of pushes shares it.
        if (!this.pending) {
            this.pending = (async () => {
                const res = await fetchImpl(TOKEN_URL, {
                    method: 'POST',
                    headers: { 'content-type': 'application/x-www-form-urlencoded' },
                    body: new URLSearchParams({ grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer', assertion: await this.assertion(now) }).toString(),
                });
                const json = await res.json().catch(() => ({}));
                if (!res.ok || typeof json.access_token !== 'string') {
                    throw new Error(`FCM auth failed: ${res.status} ${json.error || ''}`.trim());
                }
                this.cached = { token: json.access_token, expiresAt: now + (Number(json.expires_in) || 3600) * 1000 };
                return json.access_token;
            })().finally(() => {
                this.pending = null;
            });
        }
        return this.pending;
    }

    get endpoint() {
        return `https://fcm.googleapis.com/v1/projects/${this.projectId}/messages:send`;
    }
}

/**
 * Build a sender from an env-like object, or null when no key is configured.
 * FCM_SERVICE_ACCOUNT is the service-account JSON (Firebase console →
 * Project settings → Service accounts → Generate new private key).
 */
export function fcmSenderFromEnv(env) {
    const raw = env.FCM_SERVICE_ACCOUNT;
    if (!raw) return null;
    const sa = typeof raw === 'string' ? JSON.parse(raw) : raw;
    return new FcmSender({ clientEmail: sa.client_email, privateKey: sa.private_key, projectId: env.FCM_PROJECT_ID || sa.project_id });
}

export function isAndroid(n) {
    return !!n && typeof n === 'object' && n.platform === 'android';
}

/** Sanity-check one Android notification; returns a reason or null. */
export function validateFcmNotification(n) {
    if (!n || typeof n !== 'object') return 'BadNotification';
    if (typeof n.token !== 'string' || !/^[A-Za-z0-9_:-]{100,4096}$/.test(n.token)) return 'BadDeviceToken';
    const data = n.payload?.data;
    if (!data || typeof data !== 'object') return 'PayloadEmpty';
    if (Object.values(data).some(v => typeof v !== 'string')) return 'BadPayload';
    if (new TextEncoder().encode(JSON.stringify(data)).length > MAX_DATA_BYTES) return 'PayloadTooLarge';
    return null;
}

/** The FCM v1 request body: data-only, high priority (it is a reply or a question). */
export function fcmMessage(n) {
    const android = { priority: 'HIGH', ttl: '86400s' };
    if (typeof n.collapseId === 'string' && n.collapseId) android.collapse_key = n.collapseId.slice(0, 64);
    return { message: { token: n.token, data: n.payload.data, android } };
}

/**
 * FCM's error body is `{error:{status, details:[{'@type': ...FcmError, errorCode}]}}`;
 * the errorCode (UNREGISTERED, SENDER_ID_MISMATCH, ...) is the useful part.
 */
export function fcmResult(status, bodyText) {
    if (status === 200) return { status };
    let reason;
    try {
        const err = JSON.parse(bodyText || '{}').error || {};
        const detail = (err.details || []).find(d => typeof d?.errorCode === 'string');
        reason = detail?.errorCode || err.status;
    } catch {
        /* not JSON */
    }
    return { status, reason: typeof reason === 'string' ? reason : undefined };
}

/** Deliver Android notifications. Never throws: a failure becomes `{status: 0, reason}`. */
export async function sendFcm(sender, notifications, fetchImpl = fetch) {
    const results = [];
    for (const n of notifications) {
        const invalid = validateFcmNotification(n);
        if (invalid) {
            results.push({ token: typeof n?.token === 'string' ? n.token : '', status: 400, reason: invalid });
            continue;
        }
        try {
            const auth = await sender.token(fetchImpl);
            const res = await fetchImpl(sender.endpoint, {
                method: 'POST',
                headers: { authorization: `Bearer ${auth}`, 'content-type': 'application/json' },
                body: JSON.stringify(fcmMessage(n)),
            });
            results.push({ token: n.token, ...fcmResult(res.status, await res.text()) });
        } catch (err) {
            results.push({ token: n.token, status: 0, reason: err instanceof Error ? err.message : String(err) });
        }
    }
    return results;
}

/**
 * One `push` frame, either kind of phone. `sendApns(list)` delivers the iOS
 * part (null when the relay has no APNs key); `fcm` the Android part. A
 * phone whose service this relay cannot reach gets `{status: 0, reason:
 * 'ApnsUnsupported' | 'FcmUnsupported'}`, which the desktop logs but does not
 * treat as a dead token. Results keep the frame's order.
 */
export async function deliverPush(notifications, { sendApns, fcm, fetchImpl = fetch }) {
    const all = Array.isArray(notifications) ? notifications : [];
    const batch = clampBatch(all);
    const ios = batch.filter(n => !isAndroid(n));
    const android = batch.filter(isAndroid);
    const unsupported = (list, reason) => list.map(n => ({ token: typeof n?.token === 'string' ? n.token : '', status: 0, reason }));
    const [iosResults, androidResults] = await Promise.all([
        ios.length ? (sendApns ? sendApns(ios) : unsupported(ios, 'ApnsUnsupported')) : [],
        android.length ? (fcm ? sendFcm(fcm, android, fetchImpl) : unsupported(android, 'FcmUnsupported')) : [],
    ]);
    let i = 0;
    let a = 0;
    const results = batch.map(n => (isAndroid(n) ? androidResults[a++] : iosResults[i++]));
    // A frame may carry more notifications than one batch holds. The rest
    // were never sent, and the desktop still has to hear about them.
    for (const n of all.slice(batch.length)) {
        results.push({ token: typeof n?.token === 'string' ? n.token : '', status: 0, reason: 'BatchTooLarge' });
    }
    return results;
}
