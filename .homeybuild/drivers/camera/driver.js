"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const homey_1 = __importDefault(require("homey"));
const MQTTClient_1 = require("../../lib/MQTTClient");
class CameraDriver extends homey_1.default.Driver {
    constructor() {
        super(...arguments);
        this.pairSettings = {
            cameras: [],
            mqtt_url: '',
            mqtt_username: '',
            mqtt_password: '',
            mqtt_topic_prefix: 'frigate',
            frigate_url: '',
        };
    }
    async onInit() {
        this.log('CameraDriver initialized');
        this.homey.flow
            .getActionCard('restart-frigate')
            .registerRunListener(async (args) => {
            args.device.restart();
        });
        this.homey.flow
            .getActionCard('enable-detection')
            .registerRunListener(async (args) => {
            args.device.setDetection(true);
        });
        this.homey.flow
            .getActionCard('disable-detection')
            .registerRunListener(async (args) => {
            args.device.setDetection(false);
        });
        this.homey.flow
            .getActionCard('enable-recording')
            .registerRunListener(async (args) => {
            args.device.setRecording(true);
        });
        this.homey.flow
            .getActionCard('disable-recording')
            .registerRunListener(async (args) => {
            args.device.setRecording(false);
        });
        this.homey.flow
            .getActionCard('enable-snapshots')
            .registerRunListener(async (args) => {
            args.device.setSnapshots(true);
        });
        this.homey.flow
            .getActionCard('disable-snapshots')
            .registerRunListener(async (args) => {
            args.device.setSnapshots(false);
        });
        this.homey.flow
            .getActionCard('suspend-notifications')
            .registerRunListener(async (args) => {
            args.device.suspendNotifications(args.minutes);
        });
        this.homey.flow
            .getConditionCard('label-detected-recently')
            .registerRunListener((args) => {
            return args.device.wasLabelDetectedRecently(args.label, args.minutes);
        });
        this.homey.flow
            .getConditionCard('camera-has-unreviewed-alerts')
            .registerRunListener((args) => {
            return args.device.hasUnreviewedAlerts();
        });
    }
    async onPair(session) {
        // Step 1: validate broker connection + attempt camera discovery
        session.setHandler('validate_credentials', async (data) => {
            var _a, _b, _c, _d, _e;
            const prefix = ((_a = data.mqtt_topic_prefix) === null || _a === void 0 ? void 0 : _a.trim()) || 'frigate';
            // discoverFrigateCameras rejects only on hard broker errors;
            // returns [] if Frigate is not yet publishing / discovery timed out.
            const cameras = await (0, MQTTClient_1.discoverFrigateCameras)(data.mqtt_url, data.mqtt_username || undefined, data.mqtt_password || undefined, prefix);
            this.pairSettings = {
                cameras,
                mqtt_url: data.mqtt_url,
                mqtt_username: (_b = data.mqtt_username) !== null && _b !== void 0 ? _b : '',
                mqtt_password: (_c = data.mqtt_password) !== null && _c !== void 0 ? _c : '',
                mqtt_topic_prefix: prefix,
                frigate_url: (_e = (_d = data.frigate_url) === null || _d === void 0 ? void 0 : _d.replace(/\/+$/, '')) !== null && _e !== void 0 ? _e : '',
            };
        });
        // Step 2: camera view requests stored settings + discovered camera list
        session.setHandler('get_pair_settings', async () => {
            return this.pairSettings;
        });
    }
}
module.exports = CameraDriver;
