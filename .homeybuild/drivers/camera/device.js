"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const homey_1 = __importDefault(require("homey"));
const http_1 = __importDefault(require("http"));
const https_1 = __importDefault(require("https"));
const MQTTClient_1 = require("../../lib/MQTTClient");
class CameraDevice extends homey_1.default.Device {
    constructor() {
        super(...arguments);
        this.settings = {
            mqtt_url: '',
            mqtt_username: '',
            mqtt_password: '',
            mqtt_topic_prefix: 'frigate',
            camera_name: '',
            frigate_url: '',
            frigate_local_url: '',
            label_filter: '',
            live_stream_interval: 500,
        };
        this.mqtt = null;
        this.destroyed = false;
        // Latest JPEG from frigate/{camera}/+/snapshot (binary MQTT payload)
        this.latestSnapshot = null;
        // Homey image object — serves the buffered JPEG and shows on device card
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        this.cameraImage = null;
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        this.liveStreamImage = null;
        this.liveStreamTimer = null;
        // In-memory rolling window: label → array of unix timestamps (seconds)
        // Used to answer the label-detected-recently condition without any HTTP call
        this.recentDetections = new Map();
        // Per-label snapshot images for the device-card image picker
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        this.labelImages = new Map();
        this.labelBuffers = new Map();
        // Unreviewed alert counter (tracked from MQTT; reset when review_status=NONE)
        this.unreviewedCount = 0;
        // Daily event counter
        this.dailyEventCount = 0;
        this.dailyCountDate = new Date().getDate();
        // Dedup guards
        this.seenEventIds = new Set();
        this.seenReviewIds = new Set();
    }
    // ---------- Helpers ----------
    loadSettings() {
        var _a, _b, _c, _d, _e;
        this.settings = this.getSettings();
        if (!((_a = this.settings.mqtt_topic_prefix) === null || _a === void 0 ? void 0 : _a.trim()))
            this.settings.mqtt_topic_prefix = 'frigate';
        this.settings.frigate_url = (_c = (_b = this.settings.frigate_url) === null || _b === void 0 ? void 0 : _b.replace(/\/+$/, '')) !== null && _c !== void 0 ? _c : '';
        this.settings.frigate_local_url = (_e = (_d = this.settings.frigate_local_url) === null || _d === void 0 ? void 0 : _d.replace(/\/+$/, '')) !== null && _e !== void 0 ? _e : '';
    }
    get p() { return this.settings.mqtt_topic_prefix; }
    get cam() { return this.settings.camera_name; }
    get labelFilter() {
        var _a;
        if (!((_a = this.settings.label_filter) === null || _a === void 0 ? void 0 : _a.trim()))
            return [];
        return this.settings.label_filter.split(',').map((l) => l.trim().toLowerCase()).filter(Boolean);
    }
    snapshotUrl(eventId) {
        const base = this.settings.frigate_url;
        return base ? `${base}/api/events/${eventId}/snapshot.jpg?bbox=1` : '';
    }
    clipUrl(eventId) {
        const base = this.settings.frigate_url;
        return base ? `${base}/api/events/${eventId}/clip.mp4` : '';
    }
    fetchBuffer(url) {
        return new Promise((resolve, reject) => {
            const get = url.startsWith('https://') ? https_1.default.get : http_1.default.get;
            get(url, (res) => {
                if (res.statusCode !== 200) {
                    res.resume();
                    reject(new Error(`HTTP ${res.statusCode}`));
                    return;
                }
                const chunks = [];
                res.on('data', (chunk) => chunks.push(chunk));
                res.on('end', () => resolve(Buffer.concat(chunks)));
                res.on('error', reject);
            }).on('error', reject);
        });
    }
    // ---------- Init / teardown ----------
    async onInit() {
        this.loadSettings();
        this.log(`Camera "${this.cam}" initializing`);
        for (const cap of ['alarm_motion', 'detection_fps', 'event_count', 'unreviewed_alerts']) {
            if (!this.hasCapability(cap))
                await this.addCapability(cap);
        }
        await this.setCapabilityValue('alarm_motion', false);
        await this.setCapabilityValue('event_count', 0);
        await this.setCapabilityValue('unreviewed_alerts', 0);
        await this.initCameraImage();
        await this.initLiveStreamImage();
        this.startMQTT();
    }
    async initCameraImage() {
        try {
            this.cameraImage = await this.homey.images.createImage();
            this.cameraImage.setStream(async (imageStream) => {
                const base = this.settings.frigate_local_url || this.settings.frigate_url;
                if (base) {
                    const url = `${base}/api/${this.cam}/latest.jpg`;
                    try {
                        const buffer = await this.fetchBuffer(url);
                        imageStream.end(buffer);
                    }
                    catch (err) {
                        this.error('Camera image fetch failed:', err);
                        imageStream.end();
                    }
                }
                else if (this.latestSnapshot) {
                    imageStream.end(this.latestSnapshot);
                }
                else {
                    imageStream.end();
                }
            });
            // Attach image to the device card (visible in the Homey app device tile)
            await this.setCameraImage('snapshot', 'Latest Snapshot', this.cameraImage);
        }
        catch (err) {
            this.error('Failed to create camera image:', err);
        }
    }
    async initLiveStreamImage() {
        const base = this.settings.frigate_local_url;
        if (!base)
            return;
        try {
            if (!this.liveStreamImage) {
                this.liveStreamImage = await this.homey.images.createImage();
                await this.setCameraImage('live_stream', 'Live Stream', this.liveStreamImage);
            }
            if (base.startsWith('https://')) {
                // HTTPS: Homey app fetches the MJPEG URL directly — true live stream.
                this.stopLiveStreamTimer();
                this.liveStreamImage.setUrl(`${base}/api/${this.cam}/stream`);
            }
            else {
                // HTTP: setUrl is rejected by the SDK; poll latest.jpg on a timer instead.
                this.liveStreamImage.setStream(async (imageStream) => {
                    const url = `${base}/api/${this.cam}/latest.jpg`;
                    try {
                        const buffer = await this.fetchBuffer(url);
                        imageStream.end(buffer);
                    }
                    catch (err) {
                        this.error('Live stream fetch failed:', err);
                        imageStream.end();
                    }
                });
                this.startLiveStreamTimer();
            }
            this.liveStreamImage.update().catch(() => { });
        }
        catch (err) {
            this.error('Failed to create live stream image:', err);
        }
    }
    startLiveStreamTimer() {
        var _a;
        this.stopLiveStreamTimer();
        const interval = Math.max(200, (_a = this.settings.live_stream_interval) !== null && _a !== void 0 ? _a : 500);
        this.liveStreamTimer = setInterval(() => {
            var _a;
            (_a = this.liveStreamImage) === null || _a === void 0 ? void 0 : _a.update().catch(() => { });
        }, interval);
    }
    stopLiveStreamTimer() {
        if (this.liveStreamTimer !== null) {
            clearInterval(this.liveStreamTimer);
            this.liveStreamTimer = null;
        }
    }
    async updateLabelImage(label) {
        if (this.labelImages.has(label)) {
            this.labelImages.get(label).update().catch(() => { });
            return;
        }
        const img = await this.homey.images.createImage();
        img.setStream(async (imageStream) => {
            var _a;
            imageStream.end((_a = this.labelBuffers.get(label)) !== null && _a !== void 0 ? _a : Buffer.alloc(0));
        });
        this.labelImages.set(label, img);
        const title = `${label.charAt(0).toUpperCase()}${label.slice(1)}`;
        await this.setCameraImage(`label_${label}`, `Latest ${title}`, img);
    }
    startMQTT() {
        this.mqtt = new MQTTClient_1.FrigateMQTTClient(this.settings.mqtt_url, this.settings.mqtt_username || undefined, this.settings.mqtt_password || undefined);
        this.mqtt.connect(() => this.onMQTTConnected(), () => this.onMQTTDisconnected());
    }
    stopMQTT() {
        var _a;
        (_a = this.mqtt) === null || _a === void 0 ? void 0 : _a.disconnect();
        this.mqtt = null;
    }
    // ---------- MQTT connection ----------
    async onMQTTConnected() {
        this.log(`"${this.cam}" MQTT connected`);
        await this.setAvailable();
        // Frigate server availability
        this.mqtt.subscribe(`${this.p}/available`, (_t, payload) => {
            if (payload === 'online')
                this.setAvailable().catch(() => { });
            else
                this.setUnavailable(this.homey.__('device.unavailable')).catch(() => { });
        });
        // Camera motion state
        this.mqtt.subscribe(`${this.p}/${this.cam}/motion`, (_t, payload) => {
            this.setCapabilityValue('alarm_motion', payload === 'ON').catch(() => { });
        });
        // Per-camera review status: NONE means all reviews viewed → reset counter
        this.mqtt.subscribe(`${this.p}/${this.cam}/review_status`, (_t, payload) => {
            if (payload === 'NONE') {
                this.unreviewedCount = 0;
                this.setCapabilityValue('unreviewed_alerts', 0).catch(() => { });
            }
        });
        // Detection FPS from stats broadcast
        this.mqtt.subscribe(`${this.p}/stats`, (_t, payload) => {
            var _a, _b;
            try {
                const stats = JSON.parse(payload);
                const fps = (_b = (_a = stats === null || stats === void 0 ? void 0 : stats.cameras) === null || _a === void 0 ? void 0 : _a[this.cam]) === null || _b === void 0 ? void 0 : _b.detection_fps;
                if (typeof fps === 'number') {
                    this.setCapabilityValue('detection_fps', fps).catch(() => { });
                }
            }
            catch { /* ignore */ }
        });
        // New detection events
        this.mqtt.subscribe(`${this.p}/events`, (_t, payload) => {
            this.handleEvent(payload).catch((err) => this.error('handleEvent error:', err));
        });
        // New review items
        this.mqtt.subscribe(`${this.p}/reviews`, (_t, payload) => this.handleReview(payload));
        // Snapshot images: frigate/{camera}/{label}/snapshot publishes raw JPEG bytes.
        // Buffer the latest one for the live card image, and maintain a per-label
        // image so the device card offers a picker ("Latest Person", "Latest Car", …).
        this.mqtt.subscribeBinary(`${this.p}/${this.cam}/+/snapshot`, (topic, payload) => {
            var _a;
            this.latestSnapshot = payload;
            (_a = this.cameraImage) === null || _a === void 0 ? void 0 : _a.update().catch(() => { });
            // topic format: {prefix}/{cam}/{label}/snapshot
            const parts = topic.split('/');
            const label = parts[parts.length - 2];
            if (label) {
                this.labelBuffers.set(label, payload);
                this.updateLabelImage(label).catch((err) => this.error('Label image update failed:', err));
            }
        });
    }
    async onMQTTDisconnected() {
        this.log(`"${this.cam}" MQTT disconnected`);
        await this.setUnavailable(this.homey.__('device.unavailable'));
        await this.setCapabilityValue('alarm_motion', false);
    }
    // ---------- MQTT message handlers ----------
    async handleEvent(payload) {
        var _a, _b, _c, _d, _e, _f, _g;
        let msg;
        try {
            msg = JSON.parse(payload);
        }
        catch {
            return;
        }
        if (msg.type !== 'new')
            return;
        const ev = msg.after;
        if (ev.camera !== this.cam)
            return;
        if (this.seenEventIds.has(ev.id))
            return;
        this.seenEventIds.add(ev.id);
        // Rotate daily counter at midnight
        const today = new Date().getDate();
        if (today !== this.dailyCountDate) {
            this.dailyEventCount = 0;
            this.dailyCountDate = today;
            this.seenEventIds.clear();
            this.recentDetections.clear();
        }
        this.dailyEventCount++;
        this.setCapabilityValue('event_count', this.dailyEventCount).catch(() => { });
        // Track label in rolling window for label-detected-recently condition
        const labelKey = ((_a = ev.label) !== null && _a !== void 0 ? _a : '').toLowerCase();
        const nowSec = Date.now() / 1000;
        if (!this.recentDetections.has(labelKey))
            this.recentDetections.set(labelKey, []);
        this.recentDetections.get(labelKey).push(nowSec);
        // Apply optional label filter before firing flow trigger
        if (this.labelFilter.length > 0 && !this.labelFilter.includes(labelKey))
            return;
        const zones = [...new Set([...((_b = ev.current_zones) !== null && _b !== void 0 ? _b : []), ...((_c = ev.entered_zones) !== null && _c !== void 0 ? _c : [])])].join(', ');
        // Build a per-event snapshot image. The stream is fetched lazily (when Homey
        // requests it, e.g. to attach to an email), by which time Frigate has the snapshot ready.
        // Falls back to the latest buffered MQTT snapshot if the HTTP fetch fails.
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        let eventSnapshot = this.cameraImage;
        const snapUrl = this.snapshotUrl(ev.id);
        if (snapUrl) {
            try {
                const img = await this.homey.images.createImage();
                img.setStream(async (imageStream) => {
                    var _a;
                    try {
                        const buffer = await this.fetchBuffer(snapUrl);
                        imageStream.end(buffer);
                    }
                    catch (err) {
                        this.error('Event snapshot fetch failed:', err);
                        imageStream.end((_a = this.latestSnapshot) !== null && _a !== void 0 ? _a : Buffer.alloc(0));
                    }
                });
                eventSnapshot = img;
            }
            catch (err) {
                this.error('Failed to create event image:', err);
            }
        }
        this.homey.flow.getDeviceTriggerCard('object-detected').trigger(this, {
            label: (_d = ev.label) !== null && _d !== void 0 ? _d : '',
            sub_label: (_e = ev.sub_label) !== null && _e !== void 0 ? _e : '',
            zones,
            score: Math.round(((_g = (_f = ev.top_score) !== null && _f !== void 0 ? _f : ev.score) !== null && _g !== void 0 ? _g : 0) * 100),
            event_id: ev.id,
            snapshot_url: snapUrl,
            clip_url: this.clipUrl(ev.id),
            snapshot: eventSnapshot,
            device_name: this.getName(),
        }).catch((err) => this.error('object-detected trigger error:', err));
        if (this.seenEventIds.size > 500) {
            this.seenEventIds = new Set([...this.seenEventIds].slice(-250));
        }
    }
    handleReview(payload) {
        var _a, _b, _c, _d;
        let msg;
        try {
            msg = JSON.parse(payload);
        }
        catch {
            return;
        }
        if (msg.type !== 'new')
            return;
        const rev = msg.after;
        if (rev.camera !== this.cam)
            return;
        if (this.seenReviewIds.has(rev.id))
            return;
        this.seenReviewIds.add(rev.id);
        if (!rev.has_been_reviewed) {
            this.unreviewedCount++;
            this.setCapabilityValue('unreviewed_alerts', this.unreviewedCount).catch(() => { });
        }
        if (rev.severity === 'alert') {
            this.homey.flow.getDeviceTriggerCard('review-alert').trigger(this, {
                review_id: rev.id,
                severity: rev.severity,
                objects: ((_b = (_a = rev.data) === null || _a === void 0 ? void 0 : _a.objects) !== null && _b !== void 0 ? _b : []).join(', '),
                zones: ((_d = (_c = rev.data) === null || _c === void 0 ? void 0 : _c.zones) !== null && _d !== void 0 ? _d : []).join(', '),
            }).catch((err) => this.error('review-alert trigger error:', err));
        }
        if (this.seenReviewIds.size > 500) {
            this.seenReviewIds = new Set([...this.seenReviewIds].slice(-250));
        }
    }
    // ---------- Flow condition handlers ----------
    wasLabelDetectedRecently(label, minutes) {
        var _a;
        const cutoff = Date.now() / 1000 - minutes * 60;
        const key = label.trim().toLowerCase();
        const timestamps = (_a = this.recentDetections.get(key)) !== null && _a !== void 0 ? _a : [];
        const fresh = timestamps.filter((t) => t >= cutoff);
        // Prune stale entries while we're here
        this.recentDetections.set(key, fresh);
        return fresh.length > 0;
    }
    hasUnreviewedAlerts() {
        return this.unreviewedCount > 0;
    }
    // ---------- Flow action handlers (all via MQTT publish) ----------
    pub(topic, payload = '') {
        if (!this.mqtt)
            throw new Error('MQTT not connected');
        this.mqtt.publish(topic, payload);
    }
    restart() { this.pub(`${this.p}/restart`); }
    setDetection(on) { this.pub(`${this.p}/${this.cam}/detect/set`, on ? 'ON' : 'OFF'); }
    setRecording(on) { this.pub(`${this.p}/${this.cam}/recordings/set`, on ? 'ON' : 'OFF'); }
    setSnapshots(on) { this.pub(`${this.p}/${this.cam}/snapshots/set`, on ? 'ON' : 'OFF'); }
    suspendNotifications(minutes) {
        this.pub(`${this.p}/${this.cam}/notifications/suspend`, String(Math.round(minutes)));
    }
    // ---------- Lifecycle ----------
    async onAdded() {
        this.log(`Camera "${this.cam}" added`);
    }
    async onSettings({}) {
        var _a;
        this.log('Settings changed — reconnecting MQTT');
        this.stopMQTT();
        this.stopLiveStreamTimer();
        this.loadSettings();
        this.seenEventIds.clear();
        this.seenReviewIds.clear();
        this.recentDetections.clear();
        this.labelImages.clear();
        this.labelBuffers.clear();
        this.unreviewedCount = 0;
        this.startMQTT();
        (_a = this.cameraImage) === null || _a === void 0 ? void 0 : _a.update().catch(() => { });
        await this.initLiveStreamImage();
    }
    async onRenamed(name) {
        this.log(`Renamed to "${name}"`);
    }
    async onDeleted() {
        this.log(`Camera "${this.cam}" deleted`);
        this.destroyed = true;
        this.stopMQTT();
        this.stopLiveStreamTimer();
    }
}
module.exports = CameraDevice;
