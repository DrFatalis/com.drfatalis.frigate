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
            go2rtc_http_port: 1984,
            label_filter: '',
            event_trigger_delay: 5,
        };
        this.mqtt = null;
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        this.cameraImage = null;
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        this.liveVideo = null;
        // In-memory rolling window: label → array of unix timestamps (seconds)
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
        this.seenReviewIds = new Set(); // type:"new"
        this.seenEndReviewIds = new Set(); // type:"end"
        // Cooldown: unix ms timestamp of the last fired object-detected trigger
        this.lastTriggerFiredAt = 0;
        // Prevents re-registering MQTT handlers on every reconnect
        this.subscriptionsSetup = false;
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
    isDebugMqtt() {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return this.homey.app.isDebugMqtt();
    }
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
        this.log(`[${this.cam}] Initializing — broker: ${this.settings.mqtt_url}, prefix: "${this.p}", filter: "${this.settings.label_filter || 'none'}", cooldown: ${this.settings.event_trigger_delay}s`);
        for (const cap of ['alarm_motion', 'detection_fps', 'event_count', 'unreviewed_alerts']) {
            if (!this.hasCapability(cap)) {
                await this.addCapability(cap);
                this.log(`[${this.cam}] Added missing capability: ${cap}`);
            }
        }
        await this.setCapabilityValue('alarm_motion', false);
        await this.setCapabilityValue('event_count', 0);
        await this.setCapabilityValue('unreviewed_alerts', 0);
        await this.initCameraImage();
        await this.initLiveVideo();
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
                        this.log(`[${this.cam}] Camera image fetch succeeded (${url})`);
                    }
                    catch (err) {
                        this.error(`[${this.cam}] Camera image fetch failed (${url}):`, err);
                        imageStream.end();
                    }
                }
                else {
                    imageStream.end();
                }
            });
            await this.setCameraImage('snapshot', 'Latest Snapshot', this.cameraImage);
            this.log(`[${this.cam}] Camera snapshot image registered`);
        }
        catch (err) {
            this.error(`[${this.cam}] Failed to create camera image:`, err);
        }
    }
    async initLiveVideo() {
        var _a;
        const base = this.settings.frigate_local_url;
        if (!base) {
            this.log(`[${this.cam}] No local URL configured — live video skipped`);
            return;
        }
        let hostname;
        try {
            hostname = new URL(base).hostname;
        }
        catch {
            this.error(`[${this.cam}] Cannot parse frigate_local_url: ${base}`);
            return;
        }
        const port = (_a = this.settings.go2rtc_http_port) !== null && _a !== void 0 ? _a : 1984;
        const hlsUrl = `http://${hostname}:${port}/api/stream.m3u8?src=${this.cam}`;
        try {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            this.liveVideo = await this.homey.videos.createVideoHLS();
            this.liveVideo.registerVideoUrlListener(async () => ({ url: hlsUrl }));
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            await this.setCameraVideo('live', 'Live Stream', this.liveVideo);
            this.log(`[${this.cam}] Live HLS video registered → ${hlsUrl}`);
        }
        catch (err) {
            this.error(`[${this.cam}] Failed to register live video:`, err);
        }
    }
    async stopLiveVideo() {
        if (this.liveVideo) {
            try {
                await this.liveVideo.unregister();
            }
            catch { /* ignore */ }
            this.liveVideo = null;
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
        this.log(`[${this.cam}] Registered label image: "${label}"`);
    }
    startMQTT() {
        this.log(`[${this.cam}] Starting MQTT client → ${this.settings.mqtt_url}`);
        this.mqtt = new MQTTClient_1.FrigateMQTTClient(this.settings.mqtt_url, this.settings.mqtt_username || undefined, this.settings.mqtt_password || undefined);
        this.mqtt.onRawMessage = (topic, payload) => {
            if (!this.isDebugMqtt())
                return;
            const str = payload.toString('utf8');
            const isPrintable = /^[\x09\x0A\x0D\x20-\x7E]*$/.test(str);
            const display = isPrintable ? str.slice(0, 500) : `[binary: ${payload.length} bytes]`;
            const line = `[${this.cam}] ← ${topic}: ${display}`;
            this.log(`[MQTT DEBUG] ${line}`);
            this.homey.app.addDebugLog(line);
        };
        this.mqtt.connect(() => this.onMQTTConnected(), () => this.onMQTTDisconnected());
    }
    stopMQTT() {
        var _a;
        this.log(`[${this.cam}] Stopping MQTT client`);
        (_a = this.mqtt) === null || _a === void 0 ? void 0 : _a.disconnect();
        this.mqtt = null;
        this.subscriptionsSetup = false;
    }
    // ---------- MQTT connection ----------
    async onMQTTConnected() {
        if (!this.subscriptionsSetup) {
            this.subscriptionsSetup = true;
            this.log(`[${this.cam}] MQTT connected — registering subscriptions`);
            this.mqtt.subscribe(`${this.p}/available`, (_t, payload) => {
                this.log(`[${this.cam}] Frigate server availability: ${payload}`);
                if (payload === 'online')
                    this.setAvailable().catch(() => { });
                else
                    this.setUnavailable(this.homey.__('device.unavailable')).catch(() => { });
            });
            this.mqtt.subscribe(`${this.p}/${this.cam}/motion`, (_t, payload) => {
                this.log(`[${this.cam}] Motion state: ${payload}`);
                this.setCapabilityValue('alarm_motion', payload === 'ON').catch(() => { });
            });
            this.mqtt.subscribe(`${this.p}/${this.cam}/review_status`, (_t, payload) => {
                this.log(`[${this.cam}] Review status: ${payload}`);
                if (payload === 'NONE') {
                    this.unreviewedCount = 0;
                    this.setCapabilityValue('unreviewed_alerts', 0).catch(() => { });
                }
            });
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
            this.mqtt.subscribe(`${this.p}/events`, (_t, payload) => {
                this.handleEvent(payload).catch((err) => this.error(`[${this.cam}] handleEvent error:`, err));
            });
            this.mqtt.subscribe(`${this.p}/reviews`, (_t, payload) => {
                this.handleReview(payload).catch((err) => this.error(`[${this.cam}] handleReview error:`, err));
            });
            this.mqtt.subscribeBinary(`${this.p}/${this.cam}/+/snapshot`, (topic, payload) => {
                var _a;
                (_a = this.cameraImage) === null || _a === void 0 ? void 0 : _a.update().catch(() => { });
                const parts = topic.split('/');
                const label = parts[parts.length - 2];
                if (label) {
                    this.labelBuffers.set(label, payload);
                    this.updateLabelImage(label).catch((err) => this.error(`[${this.cam}] Label image update failed:`, err));
                }
            });
            this.log(`[${this.cam}] Subscriptions active`);
        }
        else {
            this.log(`[${this.cam}] MQTT reconnected`);
        }
        await this.setAvailable();
    }
    async onMQTTDisconnected() {
        this.log(`[${this.cam}] MQTT disconnected — device marked unavailable`);
        await this.setUnavailable(this.homey.__('device.unavailable'));
        await this.setCapabilityValue('alarm_motion', false);
    }
    // ---------- MQTT message handlers ----------
    async handleEvent(payload) {
        var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k, _l, _m, _o;
        let msg;
        try {
            msg = JSON.parse(payload);
        }
        catch {
            return;
        }
        if (msg.type !== 'end')
            return;
        const ev = msg.after;
        if (ev.camera !== this.cam)
            return;
        const score = Math.round(((_b = (_a = ev.top_score) !== null && _a !== void 0 ? _a : ev.score) !== null && _b !== void 0 ? _b : 0) * 100);
        this.log(`[${this.cam}] Event ended: id=${ev.id} label="${ev.label}" score=${score}% sub_label="${(_c = ev.sub_label) !== null && _c !== void 0 ? _c : '-'}" zones=[${[...((_d = ev.current_zones) !== null && _d !== void 0 ? _d : []), ...((_e = ev.entered_zones) !== null && _e !== void 0 ? _e : [])].join(', ') || 'none'}] snapshot=${ev.has_snapshot} clip=${ev.has_clip}`);
        // Discard stale retained/replayed events (ended more than 60 s ago)
        if (ev.end_time !== null) {
            const ageSec = Date.now() / 1000 - ev.end_time;
            if (ageSec > 60) {
                this.log(`[${this.cam}] Event ${ev.id} discarded — stale (ended ${ageSec.toFixed(0)}s ago, likely a retained MQTT message)`);
                return;
            }
        }
        // Rotate daily counter at midnight
        const today = new Date().getDate();
        if (today !== this.dailyCountDate) {
            this.log(`[${this.cam}] Midnight rollover — resetting daily counters and dedup set`);
            this.dailyEventCount = 0;
            this.dailyCountDate = today;
            this.seenEventIds.clear();
            this.recentDetections.clear();
        }
        if (this.seenEventIds.has(ev.id)) {
            this.log(`[${this.cam}] Event ${ev.id} discarded — duplicate`);
            return;
        }
        this.seenEventIds.add(ev.id);
        this.dailyEventCount++;
        this.setCapabilityValue('event_count', this.dailyEventCount).catch(() => { });
        // Track label using detection start time for label-detected-recently condition
        const labelKey = ((_f = ev.label) !== null && _f !== void 0 ? _f : '').toLowerCase();
        if (!this.recentDetections.has(labelKey))
            this.recentDetections.set(labelKey, []);
        this.recentDetections.get(labelKey).push((_g = ev.start_time) !== null && _g !== void 0 ? _g : Date.now() / 1000);
        // Apply optional label filter
        if (this.labelFilter.length > 0 && !this.labelFilter.includes(labelKey)) {
            this.log(`[${this.cam}] Event ${ev.id} discarded — label "${labelKey}" not in filter [${this.labelFilter.join(', ')}]`);
            return;
        }
        const zones = [...new Set([...((_h = ev.current_zones) !== null && _h !== void 0 ? _h : []), ...((_j = ev.entered_zones) !== null && _j !== void 0 ? _j : [])])].join(', ');
        const cooldownMs = Math.max(0, (_k = this.settings.event_trigger_delay) !== null && _k !== void 0 ? _k : 5) * 1000;
        if (cooldownMs > 0 && Date.now() - this.lastTriggerFiredAt < cooldownMs) {
            const remainingSec = ((cooldownMs - (Date.now() - this.lastTriggerFiredAt)) / 1000).toFixed(1);
            this.log(`[${this.cam}] Event ${ev.id} discarded — cooldown active (${remainingSec}s remaining)`);
            return;
        }
        this.lastTriggerFiredAt = Date.now();
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        let eventSnapshot = this.cameraImage;
        const snapUrl = this.snapshotUrl(ev.id);
        if (snapUrl && ev.has_snapshot) {
            try {
                const img = await this.homey.images.createImage();
                img.setStream(async (imageStream) => {
                    try {
                        const buffer = await this.fetchBuffer(snapUrl);
                        imageStream.end(buffer);
                        this.log(`[${this.cam}] Event snapshot fetch succeeded (${snapUrl})`);
                    }
                    catch (err) {
                        this.error(`[${this.cam}] Event snapshot fetch failed (${snapUrl}):`, err);
                        imageStream.end();
                    }
                });
                eventSnapshot = img;
            }
            catch (err) {
                this.error(`[${this.cam}] Failed to create event image:`, err);
            }
        }
        const triggerTokens = {
            label: (_l = ev.label) !== null && _l !== void 0 ? _l : '',
            sub_label: (_m = ev.sub_label) !== null && _m !== void 0 ? _m : '',
            zones,
            score,
            event_id: ev.id,
            snapshot_url: snapUrl,
            clip_url: this.clipUrl(ev.id),
            snapshot: eventSnapshot,
            device_name: this.getName(),
        };
        this.log(`[${this.cam}] TRIGGER object-detected — id=${ev.id} label="${ev.label}" sub_label="${(_o = ev.sub_label) !== null && _o !== void 0 ? _o : '-'}" score=${score}% zones=[${zones || 'none'}]`);
        this.homey.flow.getDeviceTriggerCard('object-detected')
            .trigger(this, triggerTokens)
            .catch((err) => this.error(`[${this.cam}] object-detected trigger error:`, err));
        this.homey.flow.getTriggerCard('object-detected-any')
            .trigger(triggerTokens)
            .catch((err) => this.error(`[${this.cam}] object-detected-any trigger error:`, err));
        if (this.seenEventIds.size > 500) {
            this.seenEventIds = new Set([...this.seenEventIds].slice(-250));
        }
    }
    async handleReview(payload) {
        var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k, _l, _m, _o, _p, _q;
        let msg;
        try {
            msg = JSON.parse(payload);
        }
        catch {
            return;
        }
        if (msg.type !== 'new' && msg.type !== 'end')
            return;
        const rev = msg.after;
        if (rev.camera !== this.cam)
            return;
        if (msg.type === 'new') {
            if (this.seenReviewIds.has(rev.id)) {
                this.log(`[${this.cam}] Review ${rev.id} (new) discarded — duplicate`);
                return;
            }
            this.seenReviewIds.add(rev.id);
            if (!rev.has_been_reviewed) {
                this.unreviewedCount++;
                this.setCapabilityValue('unreviewed_alerts', this.unreviewedCount).catch(() => { });
                this.log(`[${this.cam}] Unreviewed alert count: ${this.unreviewedCount}`);
            }
            if (rev.severity === 'alert') {
                const objects = ((_b = (_a = rev.data) === null || _a === void 0 ? void 0 : _a.objects) !== null && _b !== void 0 ? _b : []).join(', ');
                const zones = ((_d = (_c = rev.data) === null || _c === void 0 ? void 0 : _c.zones) !== null && _d !== void 0 ? _d : []).join(', ');
                this.log(`[${this.cam}] TRIGGER review-alert — id=${rev.id} objects=[${objects || 'none'}] zones=[${zones || 'none'}]`);
                this.homey.flow.getDeviceTriggerCard('review-alert').trigger(this, {
                    review_id: rev.id,
                    severity: rev.severity,
                    objects,
                    zones,
                }).catch((err) => this.error(`[${this.cam}] review-alert trigger error:`, err));
            }
            if (this.seenReviewIds.size > 500) {
                this.seenReviewIds = new Set([...this.seenReviewIds].slice(-250));
            }
            return;
        }
        // type:"end" — review segment is finalized; snapshot with bboxes is now available
        if (rev.severity !== 'alert')
            return;
        if (this.seenEndReviewIds.has(rev.id)) {
            this.log(`[${this.cam}] Review ${rev.id} (end) discarded — duplicate`);
            return;
        }
        this.seenEndReviewIds.add(rev.id);
        const primaryEventId = (_g = ((_f = (_e = rev.data) === null || _e === void 0 ? void 0 : _e.detections) !== null && _f !== void 0 ? _f : [])[0]) !== null && _g !== void 0 ? _g : null;
        const label = (_k = ((_j = (_h = rev.data) === null || _h === void 0 ? void 0 : _h.objects) !== null && _j !== void 0 ? _j : [])[0]) !== null && _k !== void 0 ? _k : '';
        const subLabel = (_o = ((_m = (_l = rev.data) === null || _l === void 0 ? void 0 : _l.sub_labels) !== null && _m !== void 0 ? _m : [])[0]) !== null && _o !== void 0 ? _o : '';
        const zones = ((_q = (_p = rev.data) === null || _p === void 0 ? void 0 : _p.zones) !== null && _q !== void 0 ? _q : []).join(', ');
        const snapUrl = primaryEventId ? this.snapshotUrl(primaryEventId) : '';
        const clipUrl = primaryEventId ? this.clipUrl(primaryEventId) : '';
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        let alertSnapshot = this.cameraImage;
        if (snapUrl) {
            try {
                const img = await this.homey.images.createImage();
                img.setStream(async (imageStream) => {
                    try {
                        const buffer = await this.fetchBuffer(snapUrl);
                        imageStream.end(buffer);
                        this.log(`[${this.cam}] Alert snapshot fetch succeeded (${snapUrl})`);
                    }
                    catch (err) {
                        this.error(`[${this.cam}] Alert snapshot fetch failed (${snapUrl}):`, err);
                        imageStream.end();
                    }
                });
                alertSnapshot = img;
            }
            catch (err) {
                this.error(`[${this.cam}] Failed to create alert image:`, err);
            }
        }
        const alertTokens = {
            label,
            sub_label: subLabel,
            zones,
            score: 0,
            event_id: primaryEventId !== null && primaryEventId !== void 0 ? primaryEventId : '',
            snapshot_url: snapUrl,
            clip_url: clipUrl,
            snapshot: alertSnapshot,
            device_name: this.getName(),
        };
        this.log(`[${this.cam}] TRIGGER alert-object-detected — review=${rev.id} event=${primaryEventId !== null && primaryEventId !== void 0 ? primaryEventId : 'none'} label="${label}" sub_label="${subLabel || '-'}" zones=[${zones || 'none'}]`);
        this.homey.flow.getDeviceTriggerCard('alert-object-detected')
            .trigger(this, alertTokens)
            .catch((err) => this.error(`[${this.cam}] alert-object-detected trigger error:`, err));
        if (this.seenEndReviewIds.size > 500) {
            this.seenEndReviewIds = new Set([...this.seenEndReviewIds].slice(-250));
        }
    }
    // ---------- Flow condition handlers ----------
    wasLabelDetectedRecently(label, minutes) {
        var _a;
        const cutoff = Date.now() / 1000 - minutes * 60;
        const key = label.trim().toLowerCase();
        const timestamps = (_a = this.recentDetections.get(key)) !== null && _a !== void 0 ? _a : [];
        const fresh = timestamps.filter((t) => t >= cutoff);
        this.recentDetections.set(key, fresh);
        this.log(`[${this.cam}] Condition label-detected-recently: label="${key}" window=${minutes}min → ${fresh.length} detection(s) found`);
        return fresh.length > 0;
    }
    hasUnreviewedAlerts() {
        return this.unreviewedCount > 0;
    }
    // ---------- Flow action handlers (all via MQTT publish) ----------
    pub(topic, payload = '') {
        if (!this.mqtt)
            throw new Error('MQTT not connected');
        this.log(`[${this.cam}] MQTT publish → ${topic}${payload ? ` = ${payload}` : ''}`);
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
        this.log(`[${this.cam}] Device added to Homey`);
    }
    async onSettings({ changedKeys }) {
        var _a;
        this.log(`[${this.cam}] Settings changed (${changedKeys.join(', ')}) — reconnecting`);
        this.stopMQTT();
        await this.stopLiveVideo();
        this.loadSettings();
        this.seenEventIds.clear();
        this.seenReviewIds.clear();
        this.seenEndReviewIds.clear();
        this.recentDetections.clear();
        this.labelImages.clear();
        this.labelBuffers.clear();
        this.unreviewedCount = 0;
        this.lastTriggerFiredAt = 0;
        this.startMQTT();
        (_a = this.cameraImage) === null || _a === void 0 ? void 0 : _a.update().catch(() => { });
        await this.initLiveVideo();
    }
    async onRenamed(name) {
        this.log(`[${this.cam}] Renamed to "${name}"`);
    }
    async onDeleted() {
        this.log(`[${this.cam}] Device deleted`);
        this.stopMQTT();
        await this.stopLiveVideo();
    }
}
module.exports = CameraDevice;
