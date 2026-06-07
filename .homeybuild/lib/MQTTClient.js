"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.discoverFrigateCameras = exports.FrigateMQTTClient = void 0;
const mqtt_1 = __importDefault(require("mqtt"));
// ---------- Per-device MQTT client ----------
class FrigateMQTTClient {
    constructor(brokerUrl, username, password) {
        this.brokerUrl = brokerUrl;
        this.username = username;
        this.password = password;
        this.client = null;
        this.subscriptions = new Map();
        this.binarySubscriptions = new Map();
        this.reconnectDelay = 1000;
        this.destroyed = false;
    }
    connect(onConnect, onDisconnect) {
        this.onConnectCb = onConnect;
        this.onDisconnectCb = onDisconnect;
        this.doConnect();
    }
    doConnect() {
        if (this.destroyed)
            return;
        const options = {
            clientId: `homey-frigate-${Math.random().toString(16).slice(2, 10)}`,
            clean: true,
            reconnectPeriod: 0, // we manage reconnect ourselves for backoff control
        };
        if (this.username)
            options.username = this.username;
        if (this.password)
            options.password = this.password;
        this.client = mqtt_1.default.connect(this.brokerUrl, options);
        this.client.on('connect', () => {
            var _a;
            this.reconnectDelay = 1000;
            for (const topic of this.subscriptions.keys())
                this.client.subscribe(topic);
            for (const topic of this.binarySubscriptions.keys())
                this.client.subscribe(topic);
            (_a = this.onConnectCb) === null || _a === void 0 ? void 0 : _a.call(this);
        });
        this.client.on('message', (topic, payload) => {
            var _a;
            (_a = this.onRawMessage) === null || _a === void 0 ? void 0 : _a.call(this, topic, payload);
            // String handlers — for JSON/text topics
            const msg = payload.toString();
            for (const [pattern, handlers] of this.subscriptions.entries()) {
                if (this.matches(pattern, topic))
                    handlers.forEach((h) => h(topic, msg));
            }
            // Binary handlers — for JPEG snapshot topics
            for (const [pattern, handlers] of this.binarySubscriptions.entries()) {
                if (this.matches(pattern, topic))
                    handlers.forEach((h) => h(topic, payload));
            }
        });
        this.client.on('error', () => {
            // Handled via 'close' to avoid unhandled EventEmitter error
        });
        this.client.on('close', () => {
            var _a;
            if (this.destroyed)
                return;
            this.client = null;
            (_a = this.onDisconnectCb) === null || _a === void 0 ? void 0 : _a.call(this);
            this.scheduleReconnect();
        });
    }
    scheduleReconnect() {
        if (this.destroyed)
            return;
        setTimeout(() => this.doConnect(), this.reconnectDelay);
        this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30000);
    }
    subscribe(topic, handler) {
        var _a;
        if (!this.subscriptions.has(topic)) {
            this.subscriptions.set(topic, new Set());
            (_a = this.client) === null || _a === void 0 ? void 0 : _a.subscribe(topic);
        }
        this.subscriptions.get(topic).add(handler);
    }
    subscribeBinary(topic, handler) {
        var _a;
        if (!this.binarySubscriptions.has(topic)) {
            this.binarySubscriptions.set(topic, new Set());
            (_a = this.client) === null || _a === void 0 ? void 0 : _a.subscribe(topic);
        }
        this.binarySubscriptions.get(topic).add(handler);
    }
    publish(topic, payload = '') {
        var _a;
        (_a = this.client) === null || _a === void 0 ? void 0 : _a.publish(topic, payload);
    }
    disconnect() {
        var _a;
        this.destroyed = true;
        (_a = this.client) === null || _a === void 0 ? void 0 : _a.end(true);
        this.client = null;
    }
    // MQTT wildcard matching: + = one level, # = rest of path
    matches(pattern, topic) {
        const pp = pattern.split('/');
        const tp = topic.split('/');
        for (let i = 0; i < pp.length; i++) {
            if (pp[i] === '#')
                return true;
            if (pp[i] !== '+' && pp[i] !== tp[i])
                return false;
        }
        return pp.length === tp.length;
    }
}
exports.FrigateMQTTClient = FrigateMQTTClient;
// ---------- One-shot camera discovery ----------
//
// Strategy (in order of reliability):
//   1. Subscribe to `{prefix}/stats` — Frigate publishes this every few seconds
//      automatically. The JSON has a `cameras` key with camera names.
//   2. Subscribe to `{prefix}/camera_activity` — published in response to
//      `{prefix}/onConnect`. Handles both flat {cam:{}} and nested {cameras:{cam:{}}} formats.
//
// Returns an empty array on timeout so callers can fall back to manual entry.
// Only rejects on a hard connection failure (broker unreachable, auth error).
function discoverFrigateCameras(brokerUrl, username, password, topicPrefix, timeoutMs = 12000) {
    return new Promise((resolve, reject) => {
        const options = {
            clientId: `homey-frigate-discover-${Date.now()}`,
            clean: true,
            connectTimeout: 8000,
        };
        if (username)
            options.username = username;
        if (password)
            options.password = password;
        const client = mqtt_1.default.connect(brokerUrl, options);
        let settled = false;
        let connectionFailed = false;
        const done = (result) => {
            if (settled)
                return;
            settled = true;
            client.end(true);
            if (result instanceof Error)
                reject(result);
            else
                resolve(result);
        };
        // On timeout: resolve with empty array so pairing continues with manual entry
        const timer = setTimeout(() => done([]), timeoutMs);
        client.on('connect', () => {
            client.subscribe(`${topicPrefix}/stats`);
            client.subscribe(`${topicPrefix}/camera_activity`);
            // Prompt Frigate to publish camera_activity after a short delay
            setTimeout(() => client.publish(`${topicPrefix}/onConnect`, ''), 400);
        });
        client.on('message', (topic, payload) => {
            try {
                const data = JSON.parse(payload.toString());
                let cameras = [];
                if (topic === `${topicPrefix}/stats`) {
                    // stats payload: { cameras: { cam_name: {...}, ... }, ... }
                    if ((data === null || data === void 0 ? void 0 : data.cameras) && typeof data.cameras === 'object') {
                        cameras = Object.keys(data.cameras);
                    }
                }
                else if (topic === `${topicPrefix}/camera_activity`) {
                    // Two possible formats Frigate may use:
                    //   Flat:   { cam_name: { motion: false, objects: [] }, ... }
                    //   Nested: { cameras: { cam_name: { ... }, ... } }
                    if ((data === null || data === void 0 ? void 0 : data.cameras) && typeof data.cameras === 'object') {
                        cameras = Object.keys(data.cameras);
                    }
                    else if (data && typeof data === 'object') {
                        cameras = Object.keys(data).filter((k) => typeof data[k] === 'object');
                    }
                }
                if (cameras.length > 0) {
                    clearTimeout(timer);
                    done(cameras);
                }
                // If cameras is still empty, keep waiting for the next message
            }
            catch { /* ignore malformed payloads */ }
        });
        client.on('error', (err) => {
            // Only reject on connection errors (wrong host, auth failure, etc.)
            // Suppress subsequent errors after we've already settled
            if (settled || !connectionFailed) {
                connectionFailed = true;
                clearTimeout(timer);
                done(err instanceof Error ? err : new Error(String(err)));
            }
        });
    });
}
exports.discoverFrigateCameras = discoverFrigateCameras;
