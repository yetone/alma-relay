// node --test test
import assert from 'node:assert/strict';
import test from 'node:test';

import { apnsRequestHeaders, validateNotification } from '../apns.mjs';

test('an alert goes to the app topic', () => {
    const h = apnsRequestHeaders('jwt', 'me.yetone.alma', { collapseId: 'x' });
    assert.equal(h['apns-topic'], 'me.yetone.alma');
    assert.equal(h['apns-push-type'], 'alert');
    assert.equal(h['apns-priority'], '10');
    assert.equal(h['apns-collapse-id'], 'x');
});

test('a Live Activity update goes to its own topic', () => {
    const h = apnsRequestHeaders('jwt', 'me.yetone.alma', { pushType: 'liveactivity', priority: 5 });
    assert.equal(h['apns-topic'], 'me.yetone.alma.push-type.liveactivity');
    assert.equal(h['apns-push-type'], 'liveactivity');
    assert.equal(h['apns-priority'], '5');
});

test('the payload limit is bytes, the way Apple counts it', () => {
    const n = payload => ({ token: 'a'.repeat(64), environment: 'production', payload });
    // 1400 CJK characters: 1420 UTF-16 units, 4220 UTF-8 bytes.
    const cjk = { aps: { alert: '中'.repeat(1400) } };
    assert.ok(JSON.stringify(cjk).length < 4096, 'the old check counted units, not bytes');
    assert.equal(validateNotification(n(cjk)), 'PayloadTooLarge');
    assert.equal(validateNotification(n({ aps: { alert: 'x'.repeat(4080) } })), 'PayloadTooLarge');
    assert.equal(validateNotification(n({ aps: { alert: '中'.repeat(1000) } })), null);
});
