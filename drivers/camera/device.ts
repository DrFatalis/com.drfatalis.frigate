import Homey from 'homey';
import http from 'http';
import https from 'https';
import stream from 'stream';
import { FrigateMQTTClient } from '../../lib/MQTTClient';

interface DeviceSettings {
  mqtt_url: string;
  mqtt_username: string;
  mqtt_password: string;
  mqtt_topic_prefix: string;
  camera_name: string;
  frigate_url: string;
  frigate_local_url: string;
  label_filter: string;
  live_stream_interval: number;
}

interface FrigateEventPayload {
  type: 'new' | 'update' | 'end';
  before: Record<string, unknown> | null;
  after: {
    id: string;
    camera: string;
    label: string;
    sub_label: string | null;
    score: number;
    top_score: number;
    start_time: number;
    end_time: number | null;
    current_zones: string[];
    entered_zones: string[];
    has_snapshot: boolean;
    has_clip: boolean;
  };
}

interface FrigateReviewPayload {
  type: 'new' | 'update' | 'end';
  before: Record<string, unknown> | null;
  after: {
    id: string;
    camera: string;
    severity: 'alert' | 'detection';
    has_been_reviewed: boolean;
    data: { objects: string[]; zones: string[]; audio: string[] };
  };
}

class CameraDevice extends Homey.Device {

  private settings: DeviceSettings = {
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

  private mqtt: FrigateMQTTClient | null = null;
  private destroyed: boolean = false;

  // Latest JPEG from frigate/{camera}/+/snapshot (binary MQTT payload)
  private latestSnapshot: Buffer | null = null;
  // Homey image object — serves the buffered JPEG and shows on device card
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private cameraImage: any = null;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private liveStreamImage: any = null;
  private liveStreamTimer: ReturnType<typeof setInterval> | null = null;

  // In-memory rolling window: label → array of unix timestamps (seconds)
  // Used to answer the label-detected-recently condition without any HTTP call
  private recentDetections: Map<string, number[]> = new Map();

  // Per-label snapshot images for the device-card image picker
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private labelImages: Map<string, any> = new Map();
  private labelBuffers: Map<string, Buffer> = new Map();

  // Unreviewed alert counter (tracked from MQTT; reset when review_status=NONE)
  private unreviewedCount: number = 0;

  // Daily event counter
  private dailyEventCount: number = 0;
  private dailyCountDate: number = new Date().getDate();

  // Dedup guards
  private seenEventIds: Set<string> = new Set();
  private seenReviewIds: Set<string> = new Set();

  // Prevents re-registering MQTT handlers on every reconnect
  private subscriptionsSetup: boolean = false;

  // ---------- Helpers ----------

  private loadSettings(): void {
    this.settings = this.getSettings() as DeviceSettings;
    if (!this.settings.mqtt_topic_prefix?.trim()) this.settings.mqtt_topic_prefix = 'frigate';
    this.settings.frigate_url = this.settings.frigate_url?.replace(/\/+$/, '') ?? '';
    this.settings.frigate_local_url = this.settings.frigate_local_url?.replace(/\/+$/, '') ?? '';
  }

  private get p(): string { return this.settings.mqtt_topic_prefix; }
  private get cam(): string { return this.settings.camera_name; }

  private get labelFilter(): string[] {
    if (!this.settings.label_filter?.trim()) return [];
    return this.settings.label_filter.split(',').map((l) => l.trim().toLowerCase()).filter(Boolean);
  }

  private snapshotUrl(eventId: string): string {
    const base = this.settings.frigate_url;
    return base ? `${base}/api/events/${eventId}/snapshot.jpg?bbox=1` : '';
  }

  private clipUrl(eventId: string): string {
    const base = this.settings.frigate_url;
    return base ? `${base}/api/events/${eventId}/clip.mp4` : '';
  }

  private fetchBuffer(url: string): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      const get = url.startsWith('https://') ? https.get : http.get;
      get(url, (res) => {
        if (res.statusCode !== 200) {
          res.resume();
          reject(new Error(`HTTP ${res.statusCode}`));
          return;
        }
        const chunks: Buffer[] = [];
        res.on('data', (chunk: Buffer) => chunks.push(chunk));
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
      if (!this.hasCapability(cap)) await this.addCapability(cap);
    }
    await this.setCapabilityValue('alarm_motion', false);
    await this.setCapabilityValue('event_count', 0);
    await this.setCapabilityValue('unreviewed_alerts', 0);

    await this.initCameraImage();
    await this.initLiveStreamImage();
    this.startMQTT();
  }

  private async initCameraImage(): Promise<void> {
    try {
      this.cameraImage = await this.homey.images.createImage();
      this.cameraImage.setStream(async (imageStream: stream.Writable) => {
        const base = this.settings.frigate_local_url || this.settings.frigate_url;
        if (base) {
          const url = `${base}/api/${this.cam}/latest.jpg`;
          try {
            const buffer = await this.fetchBuffer(url);
            imageStream.end(buffer);
          } catch (err) {
            this.error('Camera image fetch failed:', err);
            imageStream.end();
          }
        } else if (this.latestSnapshot) {
          imageStream.end(this.latestSnapshot);
        } else {
          imageStream.end();
        }
      });
      // Attach image to the device card (visible in the Homey app device tile)
      await this.setCameraImage('snapshot', 'Latest Snapshot', this.cameraImage);
    } catch (err) {
      this.error('Failed to create camera image:', err);
    }
  }

  private async initLiveStreamImage(): Promise<void> {
    const base = this.settings.frigate_local_url;
    if (!base) return;
    try {
      if (!this.liveStreamImage) {
        this.liveStreamImage = await this.homey.images.createImage();
        await this.setCameraImage('live_stream', 'Live Stream', this.liveStreamImage);
      }
      if (base.startsWith('https://')) {
        // HTTPS: Homey app fetches the MJPEG URL directly — true live stream.
        this.stopLiveStreamTimer();
        this.liveStreamImage.setUrl(`${base}/api/${this.cam}/stream`);
      } else {
        // HTTP: setUrl is rejected by the SDK; poll latest.jpg on a timer instead.
        this.liveStreamImage.setStream(async (imageStream: stream.Writable) => {
          const url = `${base}/api/${this.cam}/latest.jpg`;
          try {
            const buffer = await this.fetchBuffer(url);
            imageStream.end(buffer);
          } catch (err) {
            this.error('Live stream fetch failed:', err);
            imageStream.end();
          }
        });
        this.startLiveStreamTimer();
      }
      this.liveStreamImage.update().catch(() => {});
    } catch (err) {
      this.error('Failed to create live stream image:', err);
    }
  }

  private startLiveStreamTimer(): void {
    this.stopLiveStreamTimer();
    const interval = Math.max(200, this.settings.live_stream_interval ?? 500);
    this.liveStreamTimer = setInterval(() => {
      this.liveStreamImage?.update().catch(() => {});
    }, interval);
  }

  private stopLiveStreamTimer(): void {
    if (this.liveStreamTimer !== null) {
      clearInterval(this.liveStreamTimer);
      this.liveStreamTimer = null;
    }
  }

  private async updateLabelImage(label: string): Promise<void> {
    if (this.labelImages.has(label)) {
      this.labelImages.get(label).update().catch(() => {});
      return;
    }
    const img = await this.homey.images.createImage();
    img.setStream(async (imageStream: stream.Writable) => {
      imageStream.end(this.labelBuffers.get(label) ?? Buffer.alloc(0));
    });
    this.labelImages.set(label, img);
    const title = `${label.charAt(0).toUpperCase()}${label.slice(1)}`;
    await this.setCameraImage(`label_${label}`, `Latest ${title}`, img);
  }

  private startMQTT(): void {
    this.mqtt = new FrigateMQTTClient(
      this.settings.mqtt_url,
      this.settings.mqtt_username || undefined,
      this.settings.mqtt_password || undefined,
    );
    this.mqtt.connect(
      () => this.onMQTTConnected(),
      () => this.onMQTTDisconnected(),
    );
  }

  private stopMQTT(): void {
    this.mqtt?.disconnect();
    this.mqtt = null;
    this.subscriptionsSetup = false;
  }

  // ---------- MQTT connection ----------

  private async onMQTTConnected(): Promise<void> {
    this.log(`"${this.cam}" MQTT connected`);
    await this.setAvailable();

    if (this.subscriptionsSetup) return;
    this.subscriptionsSetup = true;

    // Frigate server availability
    this.mqtt!.subscribe(`${this.p}/available`, (_t, payload) => {
      if (payload === 'online') this.setAvailable().catch(() => {});
      else this.setUnavailable(this.homey.__('device.unavailable')).catch(() => {});
    });

    // Camera motion state
    this.mqtt!.subscribe(`${this.p}/${this.cam}/motion`, (_t, payload) => {
      this.setCapabilityValue('alarm_motion', payload === 'ON').catch(() => {});
    });

    // Per-camera review status: NONE means all reviews viewed → reset counter
    this.mqtt!.subscribe(`${this.p}/${this.cam}/review_status`, (_t, payload) => {
      if (payload === 'NONE') {
        this.unreviewedCount = 0;
        this.setCapabilityValue('unreviewed_alerts', 0).catch(() => {});
      }
    });

    // Detection FPS from stats broadcast
    this.mqtt!.subscribe(`${this.p}/stats`, (_t, payload) => {
      try {
        const stats = JSON.parse(payload);
        const fps = stats?.cameras?.[this.cam]?.detection_fps;
        if (typeof fps === 'number') {
          this.setCapabilityValue('detection_fps', fps).catch(() => {});
        }
      } catch { /* ignore */ }
    });

    // New detection events
    this.mqtt!.subscribe(`${this.p}/events`, (_t, payload) => {
      this.handleEvent(payload).catch((err: unknown) => this.error('handleEvent error:', err));
    });

    // New review items
    this.mqtt!.subscribe(`${this.p}/reviews`, (_t, payload) => this.handleReview(payload));

    // Snapshot images: frigate/{camera}/{label}/snapshot publishes raw JPEG bytes.
    // Buffer the latest one for the live card image, and maintain a per-label
    // image so the device card offers a picker ("Latest Person", "Latest Car", …).
    this.mqtt!.subscribeBinary(`${this.p}/${this.cam}/+/snapshot`, (topic, payload) => {
      this.latestSnapshot = payload;
      this.cameraImage?.update().catch(() => {});
      // topic format: {prefix}/{cam}/{label}/snapshot
      const parts = topic.split('/');
      const label = parts[parts.length - 2];
      if (label) {
        this.labelBuffers.set(label, payload);
        this.updateLabelImage(label).catch((err: unknown) => this.error('Label image update failed:', err));
      }
    });
  }

  private async onMQTTDisconnected(): Promise<void> {
    this.log(`"${this.cam}" MQTT disconnected`);
    await this.setUnavailable(this.homey.__('device.unavailable'));
    await this.setCapabilityValue('alarm_motion', false);
  }

  // ---------- MQTT message handlers ----------

  private async handleEvent(payload: string): Promise<void> {
    let msg: FrigateEventPayload;
    try { msg = JSON.parse(payload); } catch { return; }

    if (msg.type !== 'new') return;
    const ev = msg.after;
    if (ev.camera !== this.cam) return;
    // Rotate daily counter at midnight before the dedup check so the clear
    // doesn't wipe the ID we're about to add (which would defeat the guard).
    const today = new Date().getDate();
    if (today !== this.dailyCountDate) {
      this.dailyEventCount = 0;
      this.dailyCountDate = today;
      this.seenEventIds.clear();
      this.recentDetections.clear();
    }

    if (this.seenEventIds.has(ev.id)) return;
    this.seenEventIds.add(ev.id);
    this.dailyEventCount++;
    this.setCapabilityValue('event_count', this.dailyEventCount).catch(() => {});

    // Track label in rolling window for label-detected-recently condition
    const labelKey = (ev.label ?? '').toLowerCase();
    const nowSec = Date.now() / 1000;
    if (!this.recentDetections.has(labelKey)) this.recentDetections.set(labelKey, []);
    this.recentDetections.get(labelKey)!.push(nowSec);

    // Apply optional label filter before firing flow trigger
    if (this.labelFilter.length > 0 && !this.labelFilter.includes(labelKey)) return;

    const zones = [...new Set([...(ev.current_zones ?? []), ...(ev.entered_zones ?? [])])].join(', ');

    // Build a per-event snapshot image. The stream is fetched lazily (when Homey
    // requests it, e.g. to attach to an email), by which time Frigate has the snapshot ready.
    // Falls back to the latest buffered MQTT snapshot if the HTTP fetch fails.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let eventSnapshot: any = this.cameraImage;
    const snapUrl = this.snapshotUrl(ev.id);
    if (snapUrl) {
      try {
        const img = await this.homey.images.createImage();
        img.setStream(async (imageStream: stream.Writable) => {
          try {
            const buffer = await this.fetchBuffer(snapUrl);
            imageStream.end(buffer);
          } catch (err) {
            this.error('Event snapshot fetch failed:', err);
            imageStream.end(this.latestSnapshot ?? Buffer.alloc(0));
          }
        });
        eventSnapshot = img;
      } catch (err) {
        this.error('Failed to create event image:', err);
      }
    }

    const triggerTokens = {
      label: ev.label ?? '',
      sub_label: ev.sub_label ?? '',
      zones,
      score: Math.round((ev.top_score ?? ev.score ?? 0) * 100),
      event_id: ev.id,
      snapshot_url: snapUrl,
      clip_url: this.clipUrl(ev.id),
      snapshot: eventSnapshot,
      device_name: this.getName(),
    };

    this.homey.flow.getDeviceTriggerCard('object-detected')
      .trigger(this, triggerTokens)
      .catch((err: unknown) => this.error('object-detected trigger error:', err));

    this.homey.flow.getTriggerCard('object-detected-any')
      .trigger(triggerTokens)
      .catch((err: unknown) => this.error('object-detected-any trigger error:', err));

    if (this.seenEventIds.size > 500) {
      this.seenEventIds = new Set([...this.seenEventIds].slice(-250));
    }
  }

  private handleReview(payload: string): void {
    let msg: FrigateReviewPayload;
    try { msg = JSON.parse(payload); } catch { return; }

    if (msg.type !== 'new') return;
    const rev = msg.after;
    if (rev.camera !== this.cam) return;
    if (this.seenReviewIds.has(rev.id)) return;
    this.seenReviewIds.add(rev.id);

    if (!rev.has_been_reviewed) {
      this.unreviewedCount++;
      this.setCapabilityValue('unreviewed_alerts', this.unreviewedCount).catch(() => {});
    }

    if (rev.severity === 'alert') {
      this.homey.flow.getDeviceTriggerCard('review-alert').trigger(this, {
        review_id: rev.id,
        severity: rev.severity,
        objects: (rev.data?.objects ?? []).join(', '),
        zones: (rev.data?.zones ?? []).join(', '),
      }).catch((err: unknown) => this.error('review-alert trigger error:', err));
    }

    if (this.seenReviewIds.size > 500) {
      this.seenReviewIds = new Set([...this.seenReviewIds].slice(-250));
    }
  }

  // ---------- Flow condition handlers ----------

  wasLabelDetectedRecently(label: string, minutes: number): boolean {
    const cutoff = Date.now() / 1000 - minutes * 60;
    const key = label.trim().toLowerCase();
    const timestamps = this.recentDetections.get(key) ?? [];
    const fresh = timestamps.filter((t) => t >= cutoff);
    // Prune stale entries while we're here
    this.recentDetections.set(key, fresh);
    return fresh.length > 0;
  }

  hasUnreviewedAlerts(): boolean {
    return this.unreviewedCount > 0;
  }

  // ---------- Flow action handlers (all via MQTT publish) ----------

  private pub(topic: string, payload: string = ''): void {
    if (!this.mqtt) throw new Error('MQTT not connected');
    this.mqtt.publish(topic, payload);
  }

  restart(): void          { this.pub(`${this.p}/restart`); }
  setDetection(on: boolean): void  { this.pub(`${this.p}/${this.cam}/detect/set`, on ? 'ON' : 'OFF'); }
  setRecording(on: boolean): void  { this.pub(`${this.p}/${this.cam}/recordings/set`, on ? 'ON' : 'OFF'); }
  setSnapshots(on: boolean): void  { this.pub(`${this.p}/${this.cam}/snapshots/set`, on ? 'ON' : 'OFF'); }
  suspendNotifications(minutes: number): void {
    this.pub(`${this.p}/${this.cam}/notifications/suspend`, String(Math.round(minutes)));
  }

  // ---------- Lifecycle ----------

  async onAdded() {
    this.log(`Camera "${this.cam}" added`);
  }

  async onSettings({}: {
    oldSettings: Record<string, string | number | boolean | null | undefined>;
    newSettings: Record<string, string | number | boolean | null | undefined>;
    changedKeys: string[];
  }): Promise<string | void> {
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
    this.cameraImage?.update().catch(() => {});
    await this.initLiveStreamImage();
  }

  async onRenamed(name: string) {
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
