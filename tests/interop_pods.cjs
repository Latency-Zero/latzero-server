'use strict';

const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const { performance } = require('node:perf_hooks');
const readline = require('node:readline');
const { pathToFileURL } = require('node:url');
const vm = require('node:vm');

const config = JSON.parse(process.argv[2]);
const clients = new Map();
const sockets = [];
const errors = [];
const plain = value => JSON.parse(JSON.stringify(value));

async function bounded(promise, label, timeout = 7000) {
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
        assert.ok(this.values.length < 128, 'Pod helper observation budget exceeded');
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
    assert.equal(typeof WebSocket, 'function', 'Pod interop requires native Node WebSocket');
    class BrowserCustomEvent extends Event {
        constructor(type, options = {}) { super(type); this.detail = options.detail; }
    }
    class BrowserWebSocket extends WebSocket {
        constructor(url) { super(url); sockets.push(this); }
    }
    const filename = join(config.root, 'web-client', 'latzero-client.js');
    const context = vm.createContext({
        window: {}, WebSocket: BrowserWebSocket, URL, EventTarget,
        CustomEvent: BrowserCustomEvent, TextEncoder, performance,
        setTimeout, clearTimeout, console
    });
    vm.runInContext(readFileSync(filename, 'utf8'), context, { filename });
    return context.window.LatZeroWebClient;
}

function record(client, browser) {
    const item = { client, browser, hooks: new Feed(), updates: new Feed(), generation: 0 };
    clients.set(client.clientId, item);
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

function endpoints() {
    return Object.fromEntries([...clients].map(([id, item]) => [id, {
        port: item.browser ? Number(new URL(item.client.ws.url).port) : item.client.socket.remotePort,
        host: item.client.host, configuredPort: item.client.port,
        configuredWsPort: item.browser ? item.client.wsPort : null,
        pool: item.client.poolName, clientId: item.client.clientId,
    }]));
}

function value(id, kind, data) { return { owner: id, kind, value: data.a + data.b }; }

async function register(item) {
    const id = item.client.clientId;
    item.client.on('calculate', data => value(id, 'app', data));
    await bounded(item.client.process.register(
        data => value(id, 'process', data), 'compute', { minWorkers: 1, maxWorkers: 1 }
    ), `${id} process registration`);
    const listed = await bounded(item.client.process.list(), `${id} process listing`);
    assert.equal(Object.hasOwn(listed, `${id}:compute`), true);
}

function call(item, kind, target, data, responseTo = null) {
    return kind === 'app' ? item.client.callEvent('calculate', {
        targetClientId: target, data, timeout: 5000, responseTo
    }) : item.client.process.call(`${target}:compute`, data, { timeout: 5000, responseTo });
}

function assertTerminal(message, origin, target, kind, data) {
    assert.equal(message.type, 'app_result');
    assert.equal(message.payload.request_id, message.request_id);
    assert.equal(message.payload.source_client_id, origin);
    assert.equal(message.payload.target_client_id, target);
    assert.equal(message.payload.error, null);
    assert.equal(message.payload.event, kind === 'app' ? 'calculate' : `${target}:compute`);
    assert.deepEqual(plain(message.payload.value), value(target, kind, data));
}

async function subscription(item, key, subscribe) {
    await item.client.sendRequest(subscribe ? 'subscribe_buffer' : 'unsubscribe_buffer', { key });
}

async function rpc() {
    const results = [];
    const routed = [];
    for (const [origin, target] of [['node-main', 'browser-main'], ['browser-main', 'node-peer']]) {
        for (const kind of ['app', 'process']) {
            for (const responseTo of [null, origin]) {
                const data = { a: 4, b: 5 };
                const result = await bounded(call(clients.get(origin), kind, target, data, responseTo), 'Owner-local RPC');
                assertTerminal(result, origin, target, kind, data);
                results.push(result.request_id);
            }
        }
    }
    for (const [origin, target, recipient] of [
        ['node-main', 'node-peer', 'browser-main'], ['browser-main', 'node-main', 'node-peer'],
        ['node-peer', 'browser-main', 'node-main']
    ]) {
        for (const kind of ['app', 'process']) {
            const data = { a: 1, b: 2 };
            const acceptance = await bounded(call(clients.get(origin), kind, target, data, recipient), 'Routed acceptance');
            assert.equal(acceptance.type, 'ack');
            assert.equal(acceptance.payload.queued, true);
            assert.equal(acceptance.payload.request_id, acceptance.request_id);
            const result = await clients.get(recipient).hooks.wait(message =>
                message.request_id === acceptance.request_id, 'Owner-local A to B return C');
            assertTerminal(result, origin, target, kind, data);
            assert.equal(result.payload.response_to, recipient);
            routed.push({ origin, target, recipient, request_id: result.request_id });
        }
    }
    assert.deepEqual(errors, []);
    return { results, routed, endpoints: endpoints() };
}

async function buffers() {
    const key = 'pod-shared';
    for (const item of clients.values()) await subscription(item, key, true);
    let version = 0;
    for (const [id, item] of clients) {
        const data = { owner: id, values: [null, false, 0, '', '\u00e9\u96ea'] };
        version++;
        await bounded(item.client.set(key, data), 'Pod buffer write');
        for (const observer of clients.values()) {
            const update = await observer.updates.wait(payload => payload.key === key &&
                payload.entry.version === version, 'Pod subscription delivery');
            assert.deepEqual(plain(update.entry.value), data);
            assert.equal(update.entry.updated_by, id);
            assert.deepEqual(plain(await observer.client.get(key)), data);
        }
    }
    for (const item of clients.values()) await subscription(item, key, false);
    assert.deepEqual(errors, []);
    return { writers: 3, subscribers: 3, updates: 9 };
}

async function mixed(pythonId) {
    const results = [];
    const acceptances = [];
    for (const origin of ['node-main', 'browser-main']) {
        for (const kind of ['app', 'process']) {
            for (const responseTo of [null, origin]) {
                const data = { a: 3, b: 4 };
                const result = await bounded(call(clients.get(origin), kind, pythonId, data, responseTo), 'Actual Python callee');
                assertTerminal(result, origin, pythonId, kind, data);
                results.push(result.request_id);
            }
            const target = origin === 'node-main' ? 'browser-main' : 'node-peer';
            const acceptance = await bounded(call(clients.get(origin), kind, target, { a: 6, b: 7 }, pythonId), 'Python result recipient');
            assert.equal(acceptance.type, 'ack');
            assert.equal(acceptance.payload.queued, true);
            acceptances.push({ origin, target, kind, request_id: acceptance.request_id });
        }
    }
    for (const item of clients.values()) {
        assert.deepEqual(plain(await item.client.get('from-python')), { source: 'python', values: [null, false, '\u96ea'] });
    }
    assert.deepEqual(errors, []);
    return { results, acceptances };
}

async function command(request) {
    switch (request.operation) {
        case 'rpc': return rpc();
        case 'buffers': return buffers();
        case 'mixed': return mixed(request.pythonId);
        case 'hook': return clients.get(request.recipient).hooks.wait(result =>
            result.request_id === request.requestId, 'Python-origin routed result');
        case 'switch': {
            const item = clients.get(request.clientId);
            const original = { host: item.client.host, port: item.client.port, wsPort: item.client.wsPort };
            await bounded(item.client.switchPool(request.pool, request.authToken), 'Owner-changing pool switch');
            assert.equal(item.client.host, original.host);
            assert.equal(item.client.port, original.port);
            if (item.browser) assert.equal(item.client.wsPort, original.wsPort);
            const processes = await item.client.process.list();
            assert.equal(Object.hasOwn(processes, `${request.clientId}:compute`), false);
            return { endpoints: endpoints(), processes: plain(processes) };
        }
        case 'register': {
            await register(clients.get(request.clientId));
            return { registered: true };
        }
        case 'report': return { endpoints: endpoints(), errors };
        default: throw new Error(`Unknown pod interop command: ${request.operation}`);
    }
}

async function cleanup() {
    const closed = sockets.map(socket => socket.readyState === WebSocket.CLOSED ? Promise.resolve() :
        new Promise(resolve => socket.addEventListener('close', resolve, { once: true })));
    for (const item of clients.values()) {
        if (!item.browser) {
            const socket = item.client.socket;
            if (socket && !socket.closed) closed.push(new Promise(resolve => socket.once('close', resolve)));
        }
        item.client.disconnect();
    }
    for (const socket of sockets) if (socket.readyState < WebSocket.CLOSING) socket.close();
    await bounded(Promise.all(closed), 'Pod SDK transport cleanup', 4000);
    for (const item of clients.values()) assert.equal(item.client.pending.size, 0);
}

function output(message) { process.stdout.write(JSON.stringify(message) + '\n'); }

async function main() {
    let input;
    try {
        const exports = config.format === 'esm'
            ? await import(pathToFileURL(join(config.root, 'node-client', 'index.js')).href)
            : require(join(config.root, 'node-client', 'index.cjs'));
        const Client = config.api === 'async' ? exports.LatZeroAsyncClient :
            (config.format === 'esm' ? exports.default : exports);
        for (const id of ['node-main', 'node-peer']) {
            const item = record(new Client(`latzero://${id}`, config.pool, {
                host: '127.0.0.1', port: config.port, autoConnect: false, timeout: 6000
            }), false);
            await bounded(item.client.connect(), `${id} public router hello/join`);
        }
        const Browser = browserClass();
        const browser = record(new Browser('latzero://browser-main', config.pool, {
            host: '127.0.0.1', port: config.port, wsPort: config.wsPort,
            autoConnect: false, timeout: 6000, maxReconnectAttempts: 0
        }), true);
        await bounded(browser.client.connect(), 'Browser public router hello/join');
        for (const item of clients.values()) await register(item);
        assert.deepEqual(errors, []);
        output({ ok: true, ready: true, endpoints: endpoints(), clients: [...clients.keys()] });
        input = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
        for await (const line of input) {
            const request = JSON.parse(line);
            if (request.operation === 'shutdown') {
                output({ ok: true, id: request.id, result: { stopped: true } });
                break;
            }
            const result = await bounded(command(request), `Pod command ${request.operation}`, 20000);
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
