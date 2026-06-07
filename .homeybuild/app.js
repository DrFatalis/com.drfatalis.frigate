"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const homey_1 = __importDefault(require("homey"));
class FrigateApp extends homey_1.default.App {
    constructor() {
        super(...arguments);
        this.debugLogs = [];
        this.MAX_DEBUG_LOGS = 100;
        this.debugFlushTimer = null;
    }
    async onInit() {
        this.log('Frigate NVR app has started');
        if (this.homey.settings.get('debug_mqtt') === null) {
            this.homey.settings.set('debug_mqtt', false);
        }
        this.log(`MQTT debug logging: ${this.isDebugMqtt() ? 'enabled' : 'disabled'}`);
        // When the settings page clears debug_log, sync the in-memory buffer too.
        this.homey.settings.on('set', (key) => {
            if (key === 'debug_log' && this.homey.settings.get('debug_log') === '') {
                this.debugLogs = [];
            }
        });
    }
    isDebugMqtt() {
        return this.homey.settings.get('debug_mqtt') === true;
    }
    addDebugLog(line) {
        const ts = new Date().toISOString().replace('T', ' ').slice(0, 23);
        this.debugLogs.push(`[${ts}] ${line}`);
        if (this.debugLogs.length > this.MAX_DEBUG_LOGS) {
            this.debugLogs = this.debugLogs.slice(-this.MAX_DEBUG_LOGS);
        }
        // Debounce: flush to homey.settings at most once every 2 s.
        // The native settings page reads this value when opened.
        if (this.debugFlushTimer !== null)
            clearTimeout(this.debugFlushTimer);
        this.debugFlushTimer = setTimeout(() => {
            this.homey.settings.set('debug_log', this.debugLogs.join('\n'));
            this.debugFlushTimer = null;
        }, 2000);
    }
}
module.exports = FrigateApp;
