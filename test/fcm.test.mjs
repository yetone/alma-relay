// node --test test
import assert from 'node:assert/strict';
import test from 'node:test';

import { FcmSender, deliverPush, fcmMessage, fcmResult, validateFcmNotification } from '../fcm.mjs';

const FCM_TOKEN = 'cXyZ12_-:APA91b' + 'Ab9'.repeat(45);
const IOS_TOKEN = 'a'.repeat(64);

async function testSender() {
    const pair = await crypto.subtle.generateKey(
        { name: 'RSASSA-PKCS1-v1_5', modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: 'SHA-256' },
        true,
        ['sign', 'verify']
    );
    const der = Buffer.from(await crypto.subtle.exportKey('pkcs8', pair.privateKey)).toString('base64');
    const pem = `-----BEGIN PRIVATE KEY-----\n${der.match(/.{1,64}/g).join('\n')}\n-----END PRIVATE KEY-----\n`;
    return { sender: new FcmSender({ clientEmail: 'push@alma-test.iam.gserviceaccount.com', privateKey: pem, projectId: 'alma-test' }), publicKey: pair.publicKey };
}

/** A fake Google: one OAuth exchange, then FCM answers per token. */
function fakeGoogle(answers = {}) {
    const calls = [];
    const fetchImpl = async (url, init) => {
        calls.push({ url, init });
        if (url === 'https://oauth2.googleapis.com/token') {
            return new Response(JSON.stringify({ access_token: 'ya29.test', expires_in: 3599 }), { status: 200 });
        }
        const token = JSON.parse(init.body).message.token;
        const [status, body] = answers[token] ?? [200, '{"name":"projects/alma-test/messages/1"}'];
        return new Response(body, { status });
    };
    return { calls, fetchImpl };
}

const android = (extra = {}) => ({
    token: FCM_TOKEN,
    environment: 'production',
    platform: 'android',
    collapseId: 'reply:t1',
    payload: { data: { title: 'Alma', body: 'Done', kind: 'reply', threadId: 't1' } },
    ...extra,
});

test('the OAuth assertion is an RS256 JWT for the messaging scope, signed by the key', async () => {
    const { sender, publicKey } = await testSender();
    const jwt = await sender.assertion(Date.UTC(2026, 10, 2));
    const [h, c, s] = jwt.split('.');
    const claims = JSON.parse(Buffer.from(c, 'base64url').toString());
    assert.equal(JSON.parse(Buffer.from(h, 'base64url').toString()).alg, 'RS256');
    assert.equal(claims.iss, 'push@alma-test.iam.gserviceaccount.com');
    assert.equal(claims.scope, 'https://www.googleapis.com/auth/firebase.messaging');
    assert.equal(claims.exp - claims.iat, 3600);
    const ok = await crypto.subtle.verify('RSASSA-PKCS1-v1_5', publicKey, Buffer.from(s, 'base64url'), new TextEncoder().encode(`${h}.${c}`));
    assert.ok(ok);
});

test('a message is data-only, high priority, collapsed like the APNs one', () => {
    const m = fcmMessage(android());
    assert.deepEqual(m, {
        message: {
            token: FCM_TOKEN,
            data: { title: 'Alma', body: 'Done', kind: 'reply', threadId: 't1' },
            android: { priority: 'HIGH', ttl: '86400s', collapse_key: 'reply:t1' },
        },
    });
    assert.equal(m.message.notification, undefined);
});

test('bad Android notifications are refused before reaching Google', () => {
    assert.equal(validateFcmNotification(android({ token: IOS_TOKEN })), 'BadDeviceToken');
    assert.equal(validateFcmNotification(android({ payload: {} })), 'PayloadEmpty');
    assert.equal(validateFcmNotification(android({ payload: { data: { n: 1 } } })), 'BadPayload');
    assert.equal(validateFcmNotification(android({ payload: { data: { body: 'x'.repeat(5000) } } })), 'PayloadTooLarge');
    assert.equal(validateFcmNotification(android()), null);
});

test('FCM errors surface their errorCode, so the desktop can drop dead tokens', () => {
    const body = JSON.stringify({
        error: { code: 404, status: 'NOT_FOUND', details: [{ '@type': 'type.googleapis.com/google.firebase.fcm.v1.FcmError', errorCode: 'UNREGISTERED' }] },
    });
    assert.deepEqual(fcmResult(404, body), { status: 404, reason: 'UNREGISTERED' });
    assert.deepEqual(fcmResult(503, '{"error":{"status":"UNAVAILABLE"}}'), { status: 503, reason: 'UNAVAILABLE' });
    assert.deepEqual(fcmResult(200, ''), { status: 200 });
});

test('a mixed frame goes to APNs and FCM, results in frame order, one OAuth exchange', async () => {
    const { sender } = await testSender();
    const { calls, fetchImpl } = fakeGoogle();
    const ios = { token: IOS_TOKEN, environment: 'production', collapseId: 'reply:t1', payload: { aps: {} } };
    const second = android({ token: FCM_TOKEN.replace('cXyZ', 'dQwE') });
    const seen = [];
    const results = await deliverPush([android(), ios, second], {
        sendApns: async list => {
            seen.push(...list);
            return list.map(n => ({ token: n.token, status: 200 }));
        },
        fcm: sender,
        fetchImpl,
    });
    assert.deepEqual(
        results.map(r => [r.token.slice(0, 4), r.status]),
        [
            ['cXyZ', 200],
            ['aaaa', 200],
            ['dQwE', 200],
        ]
    );
    assert.deepEqual(seen, [ios]);
    assert.equal(calls.filter(c => c.url.includes('oauth2')).length, 1);
    const send = calls.find(c => c.url.includes('fcm.googleapis.com'));
    assert.equal(send.url, 'https://fcm.googleapis.com/v1/projects/alma-test/messages:send');
    assert.equal(send.init.headers.authorization, 'Bearer ya29.test');
});

test('a relay with only one key answers the other platform as unsupported, not dead', async () => {
    const ios = { token: IOS_TOKEN, environment: 'production', payload: { aps: {} } };
    const noFcm = await deliverPush([android(), ios], { sendApns: async list => list.map(n => ({ token: n.token, status: 200 })), fcm: null });
    assert.deepEqual(noFcm, [
        { token: FCM_TOKEN, status: 0, reason: 'FcmUnsupported' },
        { token: IOS_TOKEN, status: 200 },
    ]);
    const { sender } = await testSender();
    const { fetchImpl } = fakeGoogle();
    const noApns = await deliverPush([ios, android()], { sendApns: null, fcm: sender, fetchImpl });
    assert.deepEqual(noApns, [
        { token: IOS_TOKEN, status: 0, reason: 'ApnsUnsupported' },
        { token: FCM_TOKEN, status: 200 },
    ]);
});

test('a frame with more notifications than one batch holds still answers for every token', async () => {
    const many = Array.from({ length: 70 }, (_, i) => ({
        token: 'a'.repeat(60) + i.toString(16).padStart(4, '0'),
        environment: 'production',
        payload: { aps: {} },
    }));
    const sent = [];
    const results = await deliverPush(many, {
        sendApns: async list => {
            sent.push(...list);
            return list.map(n => ({ token: n.token, status: 200 }));
        },
        fcm: null,
    });
    assert.equal(sent.length, 64); // one batch
    assert.equal(results.length, 70); // every token of the frame gets an outcome
    assert.deepEqual(
        results.slice(0, 64).map(r => r.status),
        Array(64).fill(200)
    );
    assert.deepEqual(
        results.slice(64),
        many.slice(64).map(n => ({ token: n.token, status: 0, reason: 'BatchTooLarge' }))
    );
});
