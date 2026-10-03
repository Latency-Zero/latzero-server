'use strict';

const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const { performance } = require('node:perf_hooks');
const readline = require('node:readline');
const { StringDecoder } = require('node:string_decoder');
const { pathToFileURL } = require('node:url');
const vm = require('node:vm');

const config = JSON.parse(process.argv[2]);
const plain = value => JSON.parse(JSON.stringify(value));
const clients = new Map();
const sockets = [];
const gates = new Map();
const errors = [];
let sequence = 0;

function deferred() {
    let resolve;
    const promise = new Promise(res => { resolve = res; });
    return { promise, resolve };
}

async function bounded(promise, label, timeout = 6000) {
    let timer;
    try {
        return await Promise.race([promise, new Promise((_, reject) => {
            timer = setTimeout(() => reject(new Error(`${label} timed out`)), timeout);
        })]);
    } finally { clearTimeout(timer); }
}

class Feed {
    constructor() { this.values = []; this.waiters = new Set(); }
    push(value) {
        assert.ok(this.values.length < 2048, 'Smoke observation budget exceeded');
        this.values.push(value);
        for (const waiter of this.waiters) waiter(value);
    }
    async wait(predicate, label) {
        const found = this.values.find(predicate);
        if (found) return found;
        let listener;
        try {
            return await bounded(new Promise(resolve => {
                listener = value => { if (predicate(value)) resolve(value); };
                this.waiters.add(listener);
            }), label);
        } finally { this.waiters.delete(listener); }
    }
}

function browserClass() {
    assert.equal(typeof WebSocket, 'function', 'A Node runtime with native WebSocket is required');
    class BrowserCustomEvent extends Event {
        constructor(type, options = {}) { super(type); this.detail = options.detail; }
    }
    // Only inject a real handshake header; all framing and I/O remain native.
    class BrowserWebSocket extends WebSocket {
        constructor(url) {
            super(url, config.origin == null ? [] : { headers: { Origin: config.origin } });
            sockets.push(this);
        }
    }
    const context = vm.createContext({
        window: {}, WebSocket: BrowserWebSocket, URL, EventTarget,
        CustomEvent: BrowserCustomEvent, TextEncoder, performance,
        setTimeout, clearTimeout, console
    });
    const filename = join(config.root, 'web-client', 'latzero-client.js');
    vm.runInContext(readFileSync(filename, 'utf8'), context, { filename });
    return context.window.LatZeroWebClient;
}

function record(client, browser) {
    const item = { client, browser, wire: new Feed(), hooks: new Feed(), updates: new Feed(), notices: new Feed() };
    clients.set(client.clientId, item);
    client.on('interop-notice', data => item.notices.push(data));
    if (browser) {
        client.addEventListener('error', event => errors.push({ client: client.clientId, message: event.detail.message }));
        client.addEventListener('app_result', event => item.hooks.push(event.detail));
        client.addEventListener('bufferUpdate', event => item.updates.push(event.detail));
    } else {
        client.on('error', error => errors.push({ client: client.clientId, message: error.message }));
        client.on('app_result', message => item.hooks.push(message));
        client.on('bufferUpdate', payload => item.updates.push(payload));
    }
    return item;
}

async function connect(item) {
    await bounded(item.client.connect(), `${item.client.clientId} hello/join`);
    if (item.browser) {
        item.client.ws.addEventListener('message', event => item.wire.push(JSON.parse(event.data)));
    } else {
        const decoder = new StringDecoder('utf8');
        let buffer = '';
        item.client.socket.on('data', chunk => {
            buffer += decoder.write(chunk);
            let end;
            while ((end = buffer.indexOf('\n')) !== -1) {
                const line = buffer.slice(0, end);
                buffer = buffer.slice(end + 1);
                if (line.trim()) item.wire.push(JSON.parse(line));
            }
        });
    }
}

function value(owner, kind, data) { return { owner, kind, value: data.a + data.b }; }

function handler(owner, kind) {
    return async data => {
        if (data.gate) {
            const gate = gates.get(data.gate);
            assert.ok(gate, 'Incoming work retained the original data');
            gate.entered.resolve();
            await gate.release.promise;
        }
        if (data.fail) throw new Error('interop application failure');
        return value(owner, kind, data);
    };
}

function invoke(item, kind, target, data, options = {}) {
    return kind === 'app' ? item.client.callEvent('calculate', {
        targetClientId: target, data, ...options
    }) : item.client.process.call(`${target}:compute`, data, options);
}

function terminal(message, originId, origin, target, kind, data) {
    assert.equal(message.type, 'app_result');
    assert.equal(message.request_id, originId);
    assert.equal(message.payload.request_id, originId);
    assert.equal(message.payload.error, null);
    assert.equal(message.payload.source_client_id, origin);
    assert.equal(message.payload.target_client_id, target);
    assert.equal(message.payload.event, kind === 'app' ? 'calculate' : `${target}:compute`);
    assert.deepEqual(plain(message.payload.value), value(target, kind, data));
}

async function rpcSmoke() {
    const receipts = [];
    for (const origin of ['node-main', 'browser-main']) {
        const source = clients.get(origin);
        const other = origin === 'node-main' ? 'browser-main' : 'node-main';
        for (const kind of ['app', 'process']) {
            for (const [target, explicitSelf] of [[origin, false], [origin, true], [other, false], [other, true]]) {
                const gateId = `terminal-${++sequence}`;
                const gate = { entered: deferred(), release: deferred() };
                gates.set(gateId, gate);
                const data = { a: 6, b: 7, gate: gateId };
                const before = new Set(source.client.pending.keys());
                const call = invoke(source, kind, target, data, explicitSelf ? { responseTo: origin } : {});
                let settled = false;
                call.then(() => { settled = true; }, () => { settled = true; });
                const ids = [...source.client.pending.keys()].filter(id => !before.has(id));
                assert.equal(ids.length, 1);
                const id = ids[0];
                try {
                    await bounded(gate.entered.promise, `${kind} handler entry`);
                    await source.wire.wait(message => message.type === 'ack' && message.request_id === id &&
                        message.payload.queued, 'Actual daemon acceptance');
                    assert.equal(settled, false, 'Acceptance is not execution/completion');
                    const incoming = await clients.get(target).wire.wait(message => message.type === 'call_app' &&
                        message.payload.data.gate === gateId, 'Actual callee invocation');
                    assert.notEqual(incoming.request_id, id, 'Daemon uses an opaque per-hop ID');
                    assert.equal(incoming.payload.event, kind === 'app' ? 'calculate' : `${target}:compute`);
                    gate.release.resolve();
                    const result = await bounded(call, 'Terminal RPC result');
                    terminal(result, id, origin, target, kind, data);
                    receipts.push({ origin, target, kind, explicitSelf, id, hop: incoming.request_id });
                } finally { gate.release.resolve(); gates.delete(gateId); }
            }
        }
    }
    const routed = [];
    for (const [origin, target, recipient] of [
        ['node-main', 'browser-main', 'node-peer'], ['node-main', 'node-peer', 'browser-main'],
        ['browser-main', 'node-main', 'node-peer'], ['browser-main', 'node-peer', 'node-main'],
        ['node-main', 'node-main', 'browser-main'], ['browser-main', 'browser-main', 'node-main']
    ]) {
        for (const kind of ['app', 'process']) {
            const gateId = `third-party-${++sequence}`;
            const gate = { entered: deferred(), release: deferred() };
            gates.set(gateId, gate);
            const data = { a: 2, b: 3, gate: gateId };
            try {
                const acceptance = await bounded(invoke(clients.get(origin), kind, target, data,
                    { responseTo: recipient }), 'Third-party acceptance');
                assert.equal(acceptance.type, 'ack');
                assert.equal(acceptance.payload.queued, true);
                assert.equal(acceptance.payload.request_id, acceptance.request_id);
                await bounded(gate.entered.promise, 'Third-party handler entry');
                assert.equal(clients.get(recipient).hooks.values.some(message =>
                    message.request_id === acceptance.request_id), false);
                gate.release.resolve();
                const result = await clients.get(recipient).hooks.wait(message =>
                    message.request_id === acceptance.request_id, 'Unsolicited full-envelope result hook');
                terminal(result, acceptance.request_id, origin, target, kind, data);
                assert.equal(result.payload.response_to, recipient);
                routed.push({ origin, target, recipient, kind, id: acceptance.request_id });
            } finally { gate.release.resolve(); gates.delete(gateId); }
        }
    }
    const failures = [];
    const shortNames = [];
    const events = [];
    for (const origin of ['node-main', 'browser-main']) {
        const source = clients.get(origin);
        const target = origin === 'node-main' ? 'browser-main' : 'node-main';
        for (const kind of ['app', 'process']) {
            const error = await invoke(source, kind, target, { a: 1, b: 2, fail: true });
            assert.equal(error.type, 'app_result');
            assert.equal(error.payload.request_id, error.request_id);
            assert.equal(error.payload.value, null);
            assert.equal(error.payload.error.message, 'interop application failure');
            const recovery = await invoke(source, kind, target, { a: 1, b: 2 });
            terminal(recovery, recovery.request_id, origin, target, kind, { a: 1, b: 2 });
            failures.push(error.request_id);
        }
        const listed = await source.client.process.list();
        assert.deepEqual(Object.keys(listed).sort(), [...clients.keys()].map(id => `${id}:compute`).sort());
        const owners = [];
        for (let index = 0; index < clients.size; index++) {
            const data = { a: 1, b: index, marker: `short-name-${++sequence}` };
            const result = await source.client.process.call('compute', data);
            const owner = result.payload.target_client_id;
            terminal(result, result.request_id, origin, owner, 'process', data);
            const incoming = await clients.get(owner).wire.wait(message => message.type === 'call_app' &&
                message.payload.data.marker === data.marker, 'Short-name canonical invocation');
            assert.equal(incoming.payload.event, `${owner}:compute`);
            owners.push(owner);
            shortNames.push(result.request_id);
        }
        assert.deepEqual(owners.sort(), [...clients.keys()].sort());
        for (const destination of [origin, target]) {
            const data = { marker: `event-${++sequence}` };
            await source.client.emitEvent('interop-notice', { targetClientId: destination, data });
            assert.deepEqual(plain(await clients.get(destination).notices.wait(notice =>
                notice.marker === data.marker, 'Actual self/cross notification')), data);
            const push = await clients.get(destination).wire.wait(message => message.type === 'emit_event' &&
                message.payload.data.marker === data.marker, 'Actual notification envelope');
            assert.equal(push.payload.source_client_id, origin);
            events.push(data.marker);
        }
        const broadcast = { marker: `broadcast-${++sequence}` };
        await source.client.emitEvent('interop-notice', { data: broadcast });
        for (const item of clients.values()) {
            if (item.client.clientId === origin) continue;
            assert.deepEqual(plain(await item.notices.wait(notice => notice.marker === broadcast.marker,
                'Actual broadcast notification')), broadcast);
        }
        assert.equal(source.notices.values.some(notice => notice.marker === broadcast.marker), false);
        events.push(broadcast.marker);
    }
    return { receipts, routed, failures, shortNames, events };
}

async function subscription(item, key, subscribe) {
    if (item.browser) await item.client.sendRequest(subscribe ? 'subscribe_buffer' : 'unsubscribe_buffer', { key });
    else await item.client[subscribe ? 'subscribe' : 'unsubscribe'](key);
}

async function bufferSmoke() {
    const key = 'interop-shared';
    for (const item of clients.values()) await subscription(item, key, true);
    let version = 0;
    for (const writer of ['node-main', 'browser-main', 'node-peer']) {
        version++;
        const data = { writer, values: [null, false, 1.25, { nested: 'value' }] };
        await clients.get(writer).client.set(key, data);
        for (const item of clients.values()) {
            const update = await item.updates.wait(payload => payload.key === key &&
                payload.operation === 'set' && payload.entry.version === version, 'Subscribed set delivery');
            assert.deepEqual(plain(update.entry.value), data);
            assert.equal(update.entry.updated_by, writer);
            assert.deepEqual(plain(await item.client.get(key)), data);
        }
    }
    assert.equal(await clients.get('browser-main').client.delete(key), true);
    for (const item of clients.values()) {
        await item.updates.wait(payload => payload.key === key && payload.operation === 'delete', 'Subscribed delete delivery');
        assert.equal(await item.client.get(key, 'absent'), 'absent');
        await subscription(item, key, false);
    }
    await clients.get('node-main').client.set(key, 'after-unsubscribe');
    for (const item of clients.values()) {
        assert.equal(await item.client.get(key), 'after-unsubscribe');
        assert.equal(item.updates.values.filter(payload => payload.key === key).length, 4);
    }
    for (const scalar of [false, 0, '']) {
        await clients.get('node-peer').client.set('interop-scalar', scalar);
        for (const item of clients.values()) {
            assert.equal(await item.client.get('interop-scalar', 'absent'), scalar,
                `${item.client.clientId} preserves an existing ${JSON.stringify(scalar)} value`);
        }
    }
    await clients.get('node-peer').client.set('interop-scalar', null);
    for (const item of clients.values()) {
        const response = await item.client.sendRequest('get_buffer', { key: 'interop-scalar' });
        assert.equal(response.payload.exists, true);
        assert.equal(response.payload.entry.value, null);
        assert.equal(await item.client.get('interop-scalar'), null);
        assert.equal(await item.client.get('interop-scalar', 'absent'), null,
            `${item.client.clientId} does not replace a stored null with the fallback`);
    }
    return { writers: 3, subscribers: 3, updatesPerSubscriber: 4, scalarValues: 4 };
}

async function prepareTtl() {
    await clients.get('node-main').client.set('node-ttl', { source: 'node' }, { autoClean: 1250 });
    await clients.get('browser-main').client.set('browser-ttl', { source: 'browser' }, { autoClean: 1750 });
    const entries = {};
    for (const [key, observer] of [['node-ttl', 'browser-main'], ['browser-ttl', 'node-peer']]) {
        const response = await clients.get(observer).client.sendRequest('get_buffer', { key });
        assert.equal(response.payload.exists, true);
        entries[key] = response.payload.entry;
    }
    return { entries };
}

async function expireTtl() {
    for (const item of clients.values()) {
        for (const key of ['node-ttl', 'browser-ttl']) {
            assert.equal(await item.client.exists(key), false);
            assert.equal(await item.client.get(key, 'expired'), 'expired');
            assert.equal((await item.client.keys(key)).includes(key), false);
        }
    }
    return { expired: ['node-ttl', 'browser-ttl'] };
}

async function mixedSmoke(pythonId) {
    for (const item of clients.values()) {
        assert.deepEqual(plain(await item.client.get('from-python')), { source: 'python', values: [null, 1.5] });
    }
    await clients.get('node-main').client.set('mixed-shared', { source: 'node' });
    await clients.get('browser-main').client.set('mixed-shared', { source: 'browser' });
    const results = [];
    const acceptances = [];
    for (const origin of ['node-main', 'browser-main']) {
        for (const kind of ['app', 'process']) {
            for (const explicitSelf of [false, true]) {
                const data = { a: 8, b: 9 };
                const result = await invoke(clients.get(origin), kind, pythonId, data,
                    explicitSelf ? { responseTo: origin } : {});
                terminal(result, result.request_id, origin, pythonId, kind, data);
                results.push(result.request_id);
            }
            const target = origin === 'node-main' ? 'browser-main' : 'node-main';
            const acceptance = await invoke(clients.get(origin), kind, target, { a: 4, b: 5 },
                { responseTo: pythonId });
            assert.equal(acceptance.type, 'ack');
            assert.equal(acceptance.payload.queued, true);
            acceptances.push({ id: acceptance.request_id, origin, target, kind });
        }
    }
    return { results, acceptances };
}

async function command(message) {
    switch (message.operation) {
        case 'rpc': return rpcSmoke();
        case 'buffers': return bufferSmoke();
        case 'prepare_ttl': return prepareTtl();
        case 'expire_ttl': return expireTtl();
        case 'mixed': return mixedSmoke(message.pythonId);
        case 'hook': return clients.get(message.recipient).hooks.wait(result =>
            result.request_id === message.requestId, 'Python-origin result hook');
        case 'legacy_call': return invoke(clients.get(message.origin), message.kind, message.target, { a: 10, b: 11 });
        case 'report': return {
            wire: Object.fromEntries([...clients].map(([id, item]) => [id, item.wire.values])), errors
        };
        default: throw new Error(`Unknown interop command: ${message.operation}`);
    }
}

async function cleanup() {
    for (const gate of gates.values()) gate.release.resolve();
    const closing = sockets.map(socket => socket.readyState === WebSocket.CLOSED ? Promise.resolve() :
        new Promise(resolve => socket.addEventListener('close', resolve, { once: true })));
    for (const item of clients.values()) item.client.disconnect();
    for (const socket of sockets) {
        if (socket.readyState < WebSocket.CLOSING) socket.close();
    }
    await bounded(Promise.all(closing), 'Native WebSocket cleanup', 3000);
    for (const item of clients.values()) {
        assert.equal(item.client.pending.size, 0, 'No client pending-request leaks');
        assert.equal(item.client._outbox.length, 0, 'No client outbox leaks');
        assert.equal(item.client._queuedBytes, 0, 'No queued byte leaks');
        if (item.browser) {
            assert.equal(item.client._activeHandlers.size, 0);
            assert.equal(item.client._reconnectTimer, null);
            assert.equal(item.client._sendTimer, null);
        } else {
            assert.equal(item.client._activeHandlers, 0);
            assert.equal(item.client._connectTimer, null);
            assert.equal(item.client._drainTimer, null);
        }
    }
}

function output(message) { process.stdout.write(JSON.stringify(message) + '\n'); }

async function main() {
    let input;
    try {
        const Browser = browserClass();
        const browser = record(new Browser('latzero://browser-main', config.pool, {
            // The browser's public port option is the base for its WS port + 1.
            host: '127.0.0.1', port: config.wsPort - 1, autoConnect: false,
            timeout: 5000, maxReconnectAttempts: 0
        }), true);
        if (config.mode === 'origin') {
            let outcome;
            try {
                await connect(browser);
                await browser.client.set('origin-probe', { origin: config.origin ?? null });
                outcome = { connected: true, value: await browser.client.get('origin-probe') };
            } catch (error) { outcome = { connected: false, code: error.code, message: error.message }; }
            output({ ok: true, ready: true, outcome });
        } else {
            const exports = config.format === 'esm' ? await import(pathToFileURL(join(config.root, 'node-client', 'index.js')).href) :
                require(join(config.root, 'node-client', 'index.cjs'));
            const Client = config.api === 'async' ? exports.LatZeroAsyncClient :
                (config.format === 'esm' ? exports.default : exports);
            for (const id of ['node-main', 'node-peer']) {
                const item = record(new Client(`latzero://${id}`, config.pool, {
                    host: '127.0.0.1', port: config.port, autoConnect: false, timeout: 5000
                }), false);
                await connect(item);
            }
            await connect(browser);
            for (const [id, item] of clients) {
                item.client.on('calculate', handler(id, 'app'));
                await item.client.process.register(handler(id, 'process'), 'compute', { minWorkers: 1, maxWorkers: 1 });
            }
            output({ ok: true, ready: true, clients: [...clients.keys()], format: config.format, api: config.api });
        }
        input = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
        for await (const line of input) {
            const request = JSON.parse(line);
            if (request.operation === 'shutdown') {
                output({ ok: true, id: request.id, result: { closed: true } });
                break;
            }
            const result = await bounded(command(request), `Command ${request.operation}`, 12000);
            output({ ok: true, id: request.id, result });
        }
    } catch (error) {
        output({ ok: false, error: error.message, stack: error.stack });
        process.exitCode = 1;
    } finally {
        input?.close();
        process.stdin.destroy();
        try { await cleanup(); }
        catch (error) { process.stderr.write(error.stack + '\n'); process.exitCode = 1; }
    }
}

main();
